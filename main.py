import os
import datetime
import json
import logging
import threading
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from datetime import date
from fastapi import FastAPI, Depends, HTTPException, Header, Request
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import requests
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Strefy czasowe ===
# Zawsze używaj świadomych obiektów datetime!
UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

# Funkcja pomocnicza do pobierania aktualnego czasu UTC
def now_utc() -> datetime.datetime:
    """Zwraca aktualny czas jako świadomy obiekt UTC."""
    return datetime.datetime.now(UTC)

# MQTT
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/tester/status")

# DATABASE
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL — używam SQLite.")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Stałe
GRUPY_SENSOROW = ["EURO", "BUD"]

MANUALNA_MAPA_SWIAT = {
    date(2024, 11, 1): "Wszystkich Świętych",
    date(2024, 11, 11): "Święto Niepodległości",
    date(2024, 12, 24): "Wigilia",
    date(2024, 12, 25): "Boże Narodzenie",
    date(2024, 12, 26): "Drugi Dzień Świąt",
    date(2024, 12, 31): "Sylwester",
    date(2025, 1, 1): "Nowy Rok",
}

# === Tabele ===
# WAŻNE: DateTime(timezone=True) wymaga, aby kolumna w bazie danych
# obsługiwała strefę czasową (np. 'timestamp with time zone' w PostgreSQL).

class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0)
    # Zapisuj domyślnie jako świadomy UTC
    ostatnia_aktualizacja = Column(DateTime(timezone=True), default=now_utc)

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Zapisuj jako świadomy UTC
    czas_pomiaru = Column(DateTime(timezone=True), index=True)
    sensor_id = Column(String, index=True)
    status = Column(Integer)

class DaneSwieta(Base):
    __tablename__ = "dane_swieta"
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Zapisuj jako świadomy UTC
    czas_pomiaru = Column(DateTime(timezone=True), index=True)
    sensor_id = Column(String, index=True)
    status = Column(Integer)
    nazwa_swieta = Column(String, index=True)

class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    device_token = Column(String, primary_key=True, index=True)
    sensor_id = Column(String, index=True)
    # Zapisuj jako świadomy UTC
    czas_dodania = Column(DateTime(timezone=True), default=now_utc)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

API_KEY = os.environ.get('API_KEY')
async def check_api_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

app = FastAPI(title="Parking API")

# === ENDPOINT GŁÓWNY (/) ===
@app.get("/")
def read_root():
    """Zwraca komunikat powitalny na głównym adresie URL."""
    return {"API działa poprawnie."}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === MODELE ===

class WymaganyFormat(BaseModel):
    sensor_id: str
    status: int

class ObserwujRequest(BaseModel):
    sensor_id: str
    device_token: str

class StatystykiZapytanie(BaseModel):
    sensor_id: Optional[str] = None
    selected_date: str
    selected_hour: int

# === PUSH ===

def send_push_notification(token: str, sensor_id: str):
    logger.info(f"Wysyłam PUSH do {token} (sensor: {sensor_id})")
    try:
        requests.post(
            "https://exp.host/--/api/v2/push/send",
            json={
                "to": token,
                "sound": "default",
                "title": "❌ Miejsce parkingowe zajęte!",
                "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało zajęte.",
                "data": {"sensor_id": sensor_id, "action": "reroute"}
            }
        )
    except Exception as e:
        logger.error(f"Błąd podczas wysyłania PUSH: {e}")

# === STATYSTYKI (Poprawiona obsługa stref czasowych) ===

def get_time_with_offset(base_hour, offset_minutes):
    base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
    offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
    return offset_dt.time()

def calculate_occupancy_stats(sensor_prefix: str, selected_date_obj: datetime.date, selected_hour: int, db: Session) -> dict:
    nazwa_swieta = MANUALNA_MAPA_SWIAT.get(selected_date_obj)
    query = None
    kategoria_str = ""
    dni_do_uwzglednienia = []

    if nazwa_swieta:
        kategoria_str = f"Dane historyczne dla: {nazwa_swieta}"
        query = db.query(DaneSwieta).filter(
            DaneSwieta.sensor_id.startswith(sensor_prefix),
            DaneSwieta.nazwa_swieta == nazwa_swieta
        )
    else:
        selected_weekday = selected_date_obj.weekday()
        if 0 <= selected_weekday <= 3:
            kategoria_str = "Dni robocze (Pn-Cz)"
            dni_do_uwzglednienia = [0, 1, 2, 3]
        elif selected_weekday == 4:
            kategoria_str = "Piątek"
            dni_do_uwzglednienia = [4]
        elif selected_weekday == 5:
            kategoria_str = "Sobota"
            dni_do_uwzglednienia = [5]
        elif selected_weekday == 6:
            kategoria_str = "Niedziela"
            dni_do_uwzglednienia = [6]
        query = db.query(DaneHistoryczne).filter(
            DaneHistoryczne.sensor_id.startswith(sensor_prefix)
        )

    OFFSET_MINUT = 60
    czas_poczatek = get_time_with_offset(selected_hour, -OFFSET_MINUT)
    czas_koniec = get_time_with_offset(selected_hour, OFFSET_MINUT)

    wszystkie = query.all()
    dane_pasujace = []

    for rekord in wszystkie:
        czas_rekordu_db = rekord.czas_pomiaru # Jest to świadomy UTC (nowe dane) lub naiwny PL (stare dane)

        if czas_rekordu_db.tzinfo is None:
            # STARE DANE: Traktujemy jako naiwny czas polski
            czas_pl = czas_rekordu_db.replace(tzinfo=PL_TZ)
        else:
            # NOWE DANE: Zapisane jako świadomy UTC, konwertujemy do PL
            czas_pl = czas_rekordu_db.astimezone(PL_TZ)

        if not nazwa_swieta:
            if czas_pl.weekday() not in dni_do_uwzglednienia:
                continue

        czas_rek = czas_pl.time()

        if czas_poczatek > czas_koniec:
            if not (czas_rek >= czas_poczatek or czas_rek < czas_koniec):
                continue
        else:
            if not (czas_poczatek <= czas_rek < czas_koniec):
                continue

        dane_pasujace.append(rekord.status)

    if not dane_pasujace:
        return {
            "kategoria": kategoria_str,
            "przedzial_czasu": f"{czas_poczatek} - {czas_koniec}",
            "procent_zajetosci": 0,
            "liczba_pomiarow": 0
        }

    zajete = dane_pasujace.count(1)
    suma = len(dane_pasujace)
    procent = (zajete / suma) * 100 if suma > 0 else 0

    return {
        "kategoria": kategoria_str,
        "przedzial_czasu": f"{czas_poczatek} - {czas_koniec}",
        "procent_zajetosci": round(procent, 1),
        "liczba_pomiarow": suma
    }

# === ENDPOINT: OBSERWACJA (Zapisuje świadomy UTC) ===

@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    token = request.device_token
    sensor_id = request.sensor_id
    teraz_utc = now_utc() # Używamy świadomego UTC

    miejsce = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    if not miejsce or miejsce.status != 0:
        raise HTTPException(status_code=409, detail="Miejsce zajęte lub offline.")

    wpis = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == token).first()
    if wpis:
        wpis.sensor_id = sensor_id
        wpis.czas_dodania = teraz_utc # Zapisz świadomy UTC
    else:
        db.add(ObserwowaneMiejsca(device_token=token, sensor_id=sensor_id, czas_dodania=teraz_utc)) # Zapisz świadomy UTC

    db.commit()
    return {"status": "ok"}

# === ENDPOINT: STATYSTYKI ===

@app.post("/api/v1/statystyki/zajetosc")
def pobierz_statystyki(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    try:
        selected_date = datetime.datetime.strptime(z.selected_date, "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Zły format daty")

    if not z.sensor_id:
        raise HTTPException(status_code=400, detail="Brak sensor_id")

    wynik = calculate_occupancy_stats(z.sensor_id, selected_date, z.selected_hour, db)
    return {"wynik": wynik}

# === ENDPOINT: PROGNOZA ===

@app.get("/api/v1/prognoza/wszystkie_miejsca")
def pobierz_prognoze(db: Session = Depends(get_db), target_date: Optional[str] = None, target_hour: Optional[int] = None):
    prognozy = {}
    
    teraz_pl = now_utc().astimezone(PL_TZ) # Pobierz aktualny czas w PL

    if target_date and target_hour is not None:
        try:
            dt = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
            hour = int(target_hour)
        except:
            dt = teraz_pl.date()
            hour = teraz_pl.hour
    else:
        dt = teraz_pl.date()
        hour = teraz_pl.hour

    for grupa in GRUPY_SENSOROW:
        try:
            wynik = calculate_occupancy_stats(grupa, dt, hour, db)
            prognozy[grupa] = wynik["procent_zajetosci"]
        except:
            prognozy[grupa] = 0.0

    return prognozy

# === PRZETWARZANIE DANYCH Z BRAMKI (Zapisuje świadomy UTC) ===

def process_parking_update(dane: dict, db: Session):
    """Przetwarza dane i zapisuje do bazy używając świadomego czasu UTC."""
    
    teraz_utc = now_utc() # Czas zapisu (zawsze UTC)
    teraz_pl = teraz_utc.astimezone(PL_TZ) # Czas dla logiki (np. sprawdzanie świąt)

    if "sensor_id" in dane:
        dane_cz = WymaganyFormat(**dane)
        sensor_id = dane_cz.sensor_id
        nowy_status = dane_cz.status

        # Sprawdzamy święto na podstawie daty w Polsce
        nazwa_swieta = MANUALNA_MAPA_SWIAT.get(teraz_pl.date())

        if nazwa_swieta:
            db.add(DaneSwieta(
                czas_pomiaru=teraz_utc, # Zapisz świadomy UTC
                sensor_id=sensor_id,
                status=nowy_status,
                nazwa_swieta=nazwa_swieta
            ))
        else:
            db.add(DaneHistoryczne(
                czas_pomiaru=teraz_utc, # Zapisz świadomy UTC
                sensor_id=sensor_id,
                status=nowy_status
            ))

        miejsce = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
        poprzedni = -1

        if miejsce:
            poprzedni = miejsce.status
            miejsce.status = nowy_status
            miejsce.ostatnia_aktualizacja = teraz_utc # Zapisz świadomy UTC
        else:
            db.add(AktualnyStan(
                sensor_id=sensor_id,
                status=nowy_status,
                ostatnia_aktualizacja=teraz_utc # Zapisz świadomy UTC
            ))

        if poprzedni != 1 and nowy_status == 1:
            limit = datetime.timedelta(minutes=30)
            
            # Porównujemy świadomy czas UTC z bazy ze świadomym czasem UTC
            obserwatorzy = db.query(ObserwowaneMiejsca).filter(
                ObserwowaneMiejsca.sensor_id == sensor_id,
                (teraz_utc - ObserwowaneMiejsca.czas_dodania) < limit
            ).all()

            for o in obserwatorzy:
                send_push_notification(o.device_token, o.sensor_id)

            if obserwatorzy:
                db.query(ObserwowaneMiejsca).filter(
                    ObserwowaneMiejsca.device_token.in_([o.device_token for o in obserwatorzy])
                ).delete(synchronize_session=False)

        db.commit()
        return {"status": "ok"}

    elif "gateway_id" in dane:
        return {"status": "heartbeat"}

    return {"status": "unknown"}

# === HTTP Fallback ===

@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
async def aktualizuj_miejsce_http(request: Request, db: Session = Depends(get_db)):
    raw = (await request.body()).decode("latin-1").replace("\x00", "")
    
    try:
        dane = json.loads(raw)
    except:
        raise HTTPException(status_code=400, detail="Zły JSON")
    
    # Przekazujemy do zrefaktoryzowanej funkcji, która sama pobierze czas
    return process_parking_update(dane, db)

# === POPRAWIONY ENDPOINT: AKTUALNY STAN (Logika 3 minut) ===

@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):

    # Zawsze używaj świadomego czasu UTC do porównań
    teraz_utc = now_utc()
    czas_odciecia = teraz_utc - datetime.timedelta(minutes=3)

    sensory = db.query(AktualnyStan).all()
    zmiany = []

    for sensor in sensory:
        ost = sensor.ostatnia_aktualizacja # Powinien być świadomy UTC lub None
        
        jest_offline = False

        if ost is None:
            # Nigdy nie zaktualizowany
            jest_offline = True
        else:
            ost_utc = ost
            if ost.tzinfo is None:
                # STARE DANE: Były zapisane jako naiwny czas PL. Konwertujemy.
                logger.warning(f"Sensor {sensor.sensor_id} ma stary, naiwny znacznik czasu. Konwertowanie.")
                ost_utc = ost.replace(tzinfo=PL_TZ).astimezone(UTC)

            # Teraz ost_utc jest na pewno świadomym czasem UTC
            if ost_utc < czas_odciecia:
                jest_offline = True

        if jest_offline and sensor.status != 2:
            sensor.status = 2
            # Aktualizujemy czas, aby nie spamować bazy danych przy każdym odświeżeniu
            sensor.ostatnia_aktualizacja = teraz_utc 
            zmiany.append(sensor.sensor_id)

    if zmiany:
        logger.info(f"Oznaczono {len(zmiany)} sensorów jako offline (status 2).")
        db.commit()

    # Zwróć wszystkie sensory (ze zaktualizowanymi statusami)
    return db.query(AktualnyStan).all()



# === MQTT ===

mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("MQTT połączone")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"Błąd połączenia MQTT, kod: {rc}")

def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8").replace("\x00", "")
    
    try:
        dane = json.loads(raw)
    except Exception as e:
        logger.error(f"Zły JSON MQTT: {e} | Payload: {raw}")
        return

    db = None
    try:
        db = next(get_db())
        # Przekazujemy do zrefaktoryzowanej funkcji, która sama pobierze czas
        process_parking_update(dane, db)
    except Exception as e:
        logger.error(f"Błąd podczas przetwarzania wiadomości MQTT: {e}")
    finally:
        if db:
            db.close()

def mqtt_listener_thread():
    try:
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        logger.error(f"Nie można uruchomić wątku MQTT: {e}")

@app.on_event("startup")
async def startup_event():
    thread = threading.Thread(target=mqtt_listener_thread)
    thread.daemon = True
    thread.start()

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Zatrzymywanie klienta MQTT...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

