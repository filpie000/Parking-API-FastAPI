import os
import datetime
import json
import logging
import threading # To run the MQTT client in the background

from fastapi import FastAPI, Depends, HTTPException, Header, Request
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import requests
from fastapi.middleware.cors import CORSMiddleware

# MQTT library
import paho.mqtt.client as mqtt 

# Set up logger for diagnostics
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===== MQTT CONFIGURATION (MUST MATCH ESP32 CODE!) =====
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
# CORRECTED TOPIC: Now subscribing to the topic used by gate_1_4g_mqtt_gateway.ino
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/status/update") 

# === Database Configuration ===
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === Table Definitions (No change) ===
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0) # 0=wolne, 1=zajęte, 2=nieznane (Free, Occupied, Unknown)
    ostatnia_aktualizacja = Column(DateTime, default=datetime.datetime.utcnow)

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime, index=True)
    sensor_id = Column(String, index=True)
    status = Column(Integer)

class OstatniStanBramki(Base):
    __tablename__ = "ostatni_stan_bramki"
    bramka_id = Column(String, primary_key=True, default="Bramka_A") 
    ostatni_kontakt = Column(DateTime)

class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    device_token = Column(String, primary_key=True, index=True) 
    sensor_id = Column(String, index=True)
    czas_dodania = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === API Key Definition (No change) ===
API_KEY = os.environ.get('API_KEY')
async def check_api_key(x_api_key: str = Header(None)):
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

# === FastAPI Startup ===
app = FastAPI(title="Parking API")

# CORS rules (No change)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models (No change)
class WymaganyFormat(BaseModel):
    sensor_id: str
    status: int 

class ObserwujRequest(BaseModel):
    sensor_id: str
    device_token: str

@app.get("/")
def read_root():
    return {"status": "Parking API działa! Słucham MQTT na: " + MQTT_TOPIC_SUBSCRIBE}

# PUSH Notification function (No change)
def send_push_notification(token: str, sensor_id: str):
    logger.info(f"Wysyłanie powiadomienia PUSH (przez Expo) do tokena: {token} dla miejsca: {sensor_id}")
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={
            "to": token,
            "sound": "default",
            "title": "❌ Miejsce parkingowe zajęte!",
            "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte.",
            "data": { "sensor_id": sensor_id, "action": "reroute" }
        })
        logger.info(f"Pomyślnie wysłano żądanie do Expo dla: {sensor_id}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Expo: {e}")

# Mobile App Endpoint (No change)
@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    token = request.device_token
    sensor_id = request.sensor_id
    teraz = datetime.datetime.utcnow()

    wpis = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == token).first()
    
    if wpis:
        wpis.sensor_id = sensor_id
        wpis.czas_dodania = teraz
    else:
        nowy_obserwator = ObserwowaneMiejsca(
            device_token=token,
            sensor_id=sensor_id,
            czas_dodania=teraz
        )
        db.add(nowy_obserwator)
    
    db.commit()
    return {"status": "obserwowanie rozpoczęte", "miejsce": sensor_id}


# === KEY FUNCTION PROCESSING DATABASE WRITE ===
# This function is used by both MQTT and (optionally) the HTTP endpoint
def process_parking_update(dane_z_bramki: WymaganyFormat, db: Session, teraz: datetime.datetime):
    
    sensor_id = dane_z_bramki.sensor_id
    nowy_status = dane_z_bramki.status

    # 1. Save to Historical Data
    nowy_rekord_historyczny = DaneHistoryczne(
        czas_pomiaru=teraz,
        sensor_id=sensor_id,
        status=nowy_status
    )
    db.add(nowy_rekord_historyczny)

    # 2. Update Current Status
    miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    poprzedni_status = -1
    
    if miejsce_db:
        poprzedni_status = miejsce_db.status
        miejsce_db.status = nowy_status
        miejsce_db.ostatnia_aktualizacja = teraz
    else:
        nowe_miejsce = AktualnyStan(
            sensor_id=sensor_id,
            status=nowy_status,
            ostatnia_aktualizacja=teraz
        )
        db.add(nowe_miejsce)
    
    # 3. Notification Logic (No change)
    if poprzedni_status != 1 and nowy_status == 1:
        logger.info(f"Wykryto zajęcie miejsca: {sensor_id}. Sprawdzanie obserwatorów...")
        
        limit_czasu_obserwacji = datetime.timedelta(minutes=30)
        obserwatorzy = db.query(ObserwowaneMiejsca).filter(
            ObserwowaneMiejsca.sensor_id == sensor_id,
            (teraz - ObserwowaneMiejsca.czas_dodania) < limit_czasu_obserwacji
        ).all()

        tokeny_do_usuniecia = []
        for obserwator in obserwatorzy:
            send_push_notification(obserwator.device_token, obserwator.sensor_id)
            tokeny_do_usuniecia.append(obserwator.device_token)

        if tokeny_do_usuniecia:
            db.query(ObserwowaneMiejsca).filter(
                ObserwowaneMiejsca.device_token.in_(tokeny_do_usuniecia)
            ).delete(synchronize_session=False)

    
    # 4. Update Gateway last contact time (No change)
    bramka_id_z_czujnika = sensor_id.split('_')[0] 
    bramka_db = db.query(OstatniStanBramki).filter(OstatniStanBramki.bramka_id == bramka_id_z_czujnika).first()
    if bramka_db:
        bramka_db.ostatni_kontakt = teraz
    else:
        nowa_bramka = OstatniStanBramki(bramka_id=bramka_id_z_czujnika, ostatni_kontakt=teraz)
        db.add(nowa_bramka)

    db.commit()
    return {"status": "zapisano"}

# Endpoint for LILYGO GATEWAY (kept as fallback)
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
async def aktualizuj_miejsce_http(request: Request, db: Session = Depends(get_db)):
    teraz = datetime.datetime.utcnow()
    
    body_bytes = await request.body()
    raw_json_str = body_bytes.decode('latin-1').strip().replace('\x00', '')
    logger.info(f"Odebrano surowy payload (HTTP): {repr(raw_json_str)}") 

    try:
        dane = json.loads(raw_json_str)
        dane_z_bramki = WymaganyFormat(**dane) 
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.error(f"BŁĄD PARSOWANIA JSON (HTTP): {e} | Surowe dane: {repr(raw_json_str)}")
        raise HTTPException(status_code=400, detail=f"Niepoprawny format danych JSON. Szczegóły: {e}")
    
    return process_parking_update(dane_z_bramki, db, teraz)


# Mobile App Endpoint (Public) (No change)
@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    
    teraz = datetime.datetime.utcnow()
    limit_czasu_bramki = datetime.timedelta(minutes=15)
    
    bramki_offline = db.query(OstatniStanBramki).filter(
        (OstatniStanBramki.ostatni_kontakt == None) |
        ((teraz - OstatniStanBramki.ostatni_kontakt) > limit_czasu_bramki)
    ).all()

    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2}) # 2 = Unknown
    
    db.commit()
    
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca

# =========================================================
# === MQTT CLIENT IMPLEMENTATION ===
# =========================================================

mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    """
    Called when the client connects successfully to the broker.
    """
    if rc == 0:
        logger.info(f"Klient MQTT podłączony do brokera {MQTT_BROKER}. Subskrybuję temat: {MQTT_TOPIC_SUBSCRIBE}")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"Nie udało się połączyć z MQTT, kod: {rc}")

def on_message(client, userdata, msg):
    """
    Called when a message is received on the subscribed topic.
    """
    teraz = datetime.datetime.utcnow()
    
    try:
        # Decode payload as JSON
        raw_json_str = msg.payload.decode('utf-8').strip().replace('\x00', '')
        logger.info(f"ODEBRANO MQTT na temacie {msg.topic}: {repr(raw_json_str)}")
        
        dane = json.loads(raw_json_str)
        dane_z_bramki = WymaganyFormat(**dane) 
        
        # Open DB session and process data
        db = next(get_db())
        process_parking_update(dane_z_bramki, db, teraz)
        db.close()
        
    except json.JSONDecodeError as e:
        logger.error(f"BŁĄD PARSOWANIA JSON (MQTT): {e} | Surowe dane: {repr(raw_json_str)}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY przetwarzania wiadomości MQTT: {e}")


def mqtt_listener_thread():
    """
    The main MQTT listening function, run in a separate thread.
    """
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    # Attempt to connect to the broker
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        # Loop forever in the background
        mqtt_client.loop_forever()
    except Exception as e:
        logger.error(f"BŁĄD połączenia z brokerem MQTT: {e}")

# === STARTUP AND SHUTDOWN HOOKS ===
@app.on_event("startup")
async def startup_event():
    """
    Starts the MQTT client in a separate thread when FastAPI starts.
    """
    logger.info("Uruchamiam nasłuch MQTT...")
    # Start the listening function in a separate thread
    thread = threading.Thread(target=mqtt_listener_thread)
    thread.daemon = True # Thread will close when the main program closes
    thread.start()

@app.on_event("shutdown")
def shutdown_event():
    """
    Disconnects the MQTT client when FastAPI shuts down.
    """
    logger.info("Zamykanie klienta MQTT...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
