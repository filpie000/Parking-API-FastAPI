import os
import datetime
import json
import logging
import threading
from typing import Optional, List, Dict
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

# Konfiguracja MQTT
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/tester/status")

# Konfiguracja bazy danych
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL, używam domyślnej bazy SQLite.")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === STAŁE APLIKACJI ===
GRUPY_SENSOROW = ["EURO", "BUD"]

# Definicje tabel
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0)
    ostatnia_aktualizacja = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))

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
    czas_dodania = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc+1))

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

API_KEY = os.environ.get('API_KEY')
async def check_api_key(x_api_key: str = Header(None)):
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

app = FastAPI(title="Parking API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Modele Pydantic ===
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

# === Funkcje Pomocnicze ===
def send_push_notification(token: str, sensor_id: str):
    logger.info(f"Wysyłanie powiadomienia PUSH do: {token} dla: {sensor_id}")
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={
            "to": token, "sound": "default",
            "title": "❌ Miejsce parkingowe zajęte!",
            "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte. Kliknij, aby znaleźć nowe.",
            "data": { "sensor_id": sensor_id, "action": "reroute" } 
        })
        logger.info(f"Pomyślnie wysłano żądanie do Expo dla: {sensor_id}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Expo: {e}")

def get_time_with_offset(base_hour, offset_minutes):
    base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
    offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
    return offset_dt.time()

# === WEWNĘTRZNA FUNKCJA DO OBLICZANIA STATYSTYK ===
def calculate_occupancy_stats(sensor_prefix: str, selected_date_obj: datetime.date, selected_hour: int, db: Session) -> dict:
    selected_weekday = selected_date_obj.weekday()
    
    nazwy_dni = ["Poniedziałek", "Wtorek", "Środa", "Czwartek", "Piątek", "Sobota", "Niedziela"]
    kategoria_str = f"Dane historyczne dla: {nazwy_dni[selected_weekday]}"
    dni_do_uwzglednienia = [selected_weekday]

    OFFSET_MINUT = 60
    czas_poczatek = get_time_with_offset(selected_hour, -OFFSET_MINUT)
    czas_koniec = get_time_with_offset(selected_hour, OFFSET_MINUT)

    query = db.query(DaneHistoryczne).filter(
        DaneHistoryczne.sensor_id.startswith(sensor_prefix)
    )
    
    wszystkie_dane_dla_sensora = query.all()
    
    dane_pasujace = []
    
    for rekord in wszystkie_dane_dla_sensora:
        if not rekord.czas_pomiaru:
            continue
            
        dzien_rekordu = rekord.czas_pomiaru.weekday()
        czas_rekordu = rekord.czas_pomiaru.time()
        
        if dzien_rekordu not in dni_do_uwzglednienia:
            continue
            
        if czas_poczatek > czas_koniec:
            if not (czas_rekordu >= czas_poczatek or czas_rekordu < czas_koniec):
                continue
        else:
            if not (czas_poczatek <= czas_rekordu < czas_koniec):
                continue
                
        dane_pasujace.append(rekord.status)

    if not dane_pasujace:
        procent_zajetosci = 0
        liczba_pomiarow = 0
    else:
        zajete = dane_pasujace.count(1)
        wolne = dane_pasujace.count(0)
        suma_pomiarow = zajete + wolne
        procent_zajetosci = (zajete / suma_pomiarow) * 100 if suma_pomiarow > 0 else 0
        liczba_pomiarow = len(dane_pasujace)

    return {
        "kategoria": kategoria_str,
        "przedzial_czasu": f"{czas_poczatek.strftime('%H:%M')} - {czas_koniec.strftime('%H:%M')}",
        "procent_zajetosci": round(procent_zajetosci, 1),
        "liczba_pomiarow": liczba_pomiarow
    }


# === Endpointy ===
@app.get("/")
def read_root():
    return {"status": "Parking API działa! Słucham MQTT na: " + MQTT_TOPIC_SUBSCRIBE}

@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    token = request.device_token
    sensor_id = request.sensor_id
    teraz = datetime.datetime.now(datetime.timezone.utc)
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


@app.post("/api/v1/statystyki/zajetosc")
def pobierz_statystyki_zajetosci(zapytanie: StatystykiZapytanie, db: Session = Depends(get_db)):
    
    try:
        selected_date_obj = datetime.datetime.strptime(zapytanie.selected_date, "%Y-%m-%d").date()
        selected_hour = zapytanie.selected_hour
    except ValueError:
        raise HTTPException(status_code=400, detail="Niepoprawny format daty. Oczekiwano YYYY-MM-DD.")
        
    if not zapytanie.sensor_id:
         raise HTTPException(status_code=400, detail="Wymagany jest sensor_id (np. 'EURO' lub 'BUD').")
         
    try:
        wynik = calculate_occupancy_stats(zapytanie.sensor_id, selected_date_obj, selected_hour, db)
    except Exception as e:
        logger.error(f"Błąd przy /statystyki/zajetosc dla {zapytanie.sensor_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Wewnętrzny błąd serwera podczas obliczeń: {e}")
    
    return {
        "wynik_dynamiczny": wynik
    }

# ENDPOINT PROGNOZ (ODPORNY NA BŁĘDY)
@app.get("/api/v1/prognoza/wszystkie_miejsca")
def pobierz_prognoze_dla_wszystkich(
    db: Session = Depends(get_db), 
    target_date: Optional[str] = None, 
    target_hour: Optional[int] = None
):
    """
    Zwraca aktualną prognozę (procent zajętości) dla wszystkich zdefiniowanych grup sensorów.
    """
    prognozy: Dict[str, float] = {}
    selected_date_obj = None
    selected_hour = None
    use_fallback = True # Domyślnie użyj czasu "teraz"

    # Krok 1: Spróbuj przetworzyć parametry wejściowe
    if target_date and target_hour is not None:
        try:
            # Użyj int(target_hour), aby sprawdzić, czy to poprawna liczba
            selected_hour_int = int(target_hour) 
            selected_date_obj_parsed = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
            
            # Jeśli wszystko się udało, ustaw parametry i wyłącz fallback
            selected_date_obj = selected_date_obj_parsed
            selected_hour = selected_hour_int
            use_fallback = False 
            logger.info(f"Używam dostarczonego czasu prognozy: {selected_date_obj} @ {selected_hour}:00")
            
        except (ValueError, TypeError):
            # Jeśli parsowanie się nie uda (np. target_hour="" lub zła data),
            # logujemy błąd i use_fallback pozostaje True.
            logger.warning(f"Niepoprawny format target_date ('{target_date}') lub target_hour ('{target_hour}'). Używam czasu 'teraz'.")
            use_fallback = True

    # Krok 2: Jeśli parametry nie zostały podane LUB były błędne, użyj czasu "teraz"
    if use_fallback:
        teraz_utc = datetime.datetime.now(datetime.timezone.utc)
        selected_date_obj = teraz_utc.date()
        selected_hour = teraz_utc.hour
        logger.info(f"Generowanie domyślnej prognozy (teraz) dla: {selected_date_obj} @ {selected_hour}:00 UTC")

    # Krok 3: Obliczenia (teraz 'selected_hour' na pewno nie jest None)
    for grupa in GRUPY_SENSOROW:
        try:
            wynik = calculate_occupancy_stats(grupa, selected_date_obj, selected_hour, db)
            prognozy[grupa] = wynik['procent_zajetosci']
        except Exception as e:
            logger.error(f"Błąd podczas obliczania prognozy dla grupy {grupa}: {e}")
            prognozy[grupa] = 0.0

    return prognozy


# === FUNKCJA GŁÓWNA PRZETWARZANIA DANYCH ===
def process_parking_update(dane_z_bramki: WymaganyFormat, db: Session, teraz: datetime.datetime):
    sensor_id = dane_z_bramki.sensor_id
    nowy_status = dane_z_bramki.status
    nowy_rekord_historyczny = DaneHistoryczne(
        czas_pomiaru=teraz, sensor_id=sensor_id, status=nowy_status
    )
    db.add(nowy_rekord_historyczny)
    miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    poprzedni_status = -1
    if miejsce_db:
        poprzedni_status = miejsce_db.status
        miejsce_db.status = nowy_status
        miejsce_db.ostatnia_aktualizacja = teraz
    else:
        nowe_miejsce = AktualnyStan(
            sensor_id=sensor_id, status=nowy_status, ostatnia_aktualizacja=teraz
        )
        db.add(nowe_miejsce)
        
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
            logger.info(f"Wysłano powiadomienie do tokena {obserwator.device_token[:5]}... dla miejsca {sensor_id}")
        if tokeny_do_usuniecia:
            db.query(ObserwowaneMiejsca).filter(
                ObserwowaneMiejsca.device_token.in_(tokeny_do_usuniecia)
            ).delete(synchronize_session=False)
            
    bramka_id_z_czujnika = sensor_id.split('_')[0] 
    if bramka_id_z_czujnika in GRUPY_SENSOROW:
        bramka_db = db.query(OstatniStanBramki).filter(OstatniStanBramki.bramka_id == bramka_id_z_czujnika).first()
        if bramka_db:
            bramka_db.ostatni_kontakt = teraz
        else:
            nowa_bramka = OstatniStanBramki(bramka_id=bramka_id_z_czujnika, ostatni_kontakt=teraz)
            db.add(nowa_bramka)
    
    db.commit()
    return {"status": "zapisano"}

# Endpoint dla BRAMKI (HTTP Fallback)
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
async def aktualizuj_miejsce_http(request: Request, db: Session = Depends(get_db)):
    teraz = datetime.datetime.now(datetime.timezone.utc)
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


# Endpoint dla APLIKACJI (Publiczny)
@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    teraz = datetime.datetime.now(datetime.timezone.utc)
    
    # Inicjalizuj brakujące bramki w OstatniStanBramki, jeśli jeszcze nie istnieją
    for grupa in GRUPY_SENSOROW:
        istnieje = db.query(OstatniStanBramki).filter(OstatniStanBramki.bramka_id == grupa).first()
        if not istnieje:
            logger.info(f"Inicjalizuję status bramki dla: {grupa}")
            db.add(OstatniStanBramki(bramka_id=grupa, ostatni_kontakt=None))
            db.commit()
            
    limit_czasu_bramki = datetime.timedelta(minutes=15)
    bramki_offline = db.query(OstatniStanBramki).filter(
        (OstatniStanBramki.ostatni_kontakt == None) |
        ((teraz - OstatniStanBramki.ostatni_kontakt) > limit_czasu_bramki)
    ).all()
    
    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2}) # Ustaw status 2 (Nieznany)
    
    db.commit()
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca

# =========================================================
# === IMPLEMENTACJA KLIENTA MQTT ===
# =========================================================
mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Klient MQTT podłączony do brokera {MQTT_BROKER}. Subskrybuję temat: {MQTT_TOPIC_SUBSCRIBE}")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"Nie udało się połączyć z MQTT, kod: {rc}")

def on_message(client, userdata, msg):
    teraz = datetime.datetime.now(datetime.timezone.utc)
    try:
        raw_json_str = msg.payload.decode('utf-8').strip().replace('\x00', '')
        logger.info(f"ODEBRANO MQTT na temacie {msg.topic}: {repr(raw_json_str)}")
        dane = json.loads(raw_json_str)
        dane_z_bramki = WymaganyFormat(**dane) 
        db = next(get_db())
        process_parking_update(dane_z_bramki, db, teraz)
        db.close()
    except json.JSONDecodeError as e:
        logger.error(f"BŁĄD PARSOWANIA JSON (MQTT): {e} | Surowe dane: {repr(raw_json_str)}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY przetwarzania wiadomości MQTT: {e}")

def mqtt_listener_thread():
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        logger.error(f"BŁĄD połączenia z brokerem MQTT: {e}")

# === HOOKI STARTOWE I ZATRZYMUJĄCE ===
@app.on_event("startup")
async def startup_event():
    logger.info("Uruchamiam nasłuch MQTT...")
    thread = threading.Thread(target=mqtt_listener_thread)
    thread.daemon = True 
    thread.start()

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Zamykanie klienta MQTT...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

