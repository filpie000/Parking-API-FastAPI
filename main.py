import os
import datetime
import json
import logging
import threading
import time # Importujemy 'time' do spania wątku
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
UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
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
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0)
    ostatnia_aktualizacja = Column(DateTime(timezone=True), default=now_utc)

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime(timezone=True), index=True)
    sensor_id = Column(String, index=True)
    status = Column(Integer)

class DaneSwieta(Base):
    __tablename__ = "dane_swieta"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime(timezone=True), index=True)
    sensor_id = Column(String, index=True)
    status = Column(Integer)
    nazwa_swieta = Column(String, index=True)

class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    device_token = Column(String, primary_key=True, index=True)
    sensor_id = Column(String, index=True)
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

# === NOWA LOGIKA TŁA (SPRAWDZANIE STANU) ===

# Flaga do bezpiecznego zatrzymywania wątków
shutdown_event_flag = threading.Event()

def check_stale_sensors():
    """
    Funkcja, która sprawdza "nieświeże" sensory i ustawia im status 2.
    Ta funkcja jest wywoływana w oddzielnym wątku i tworzy własną sesję DB.
    """
    db = None
    try:
        # KRYTYCZNE: Wątek musi stworzyć własną, nową sesję DB.
        db = SessionLocal()
        
        teraz_utc = now_utc()
        czas_odciecia = teraz_utc - datetime.timedelta(minutes=3)
        
        logger.info(f"TŁO: Sprawdzam sensory, które nie były aktualizowane od {czas_odciecia}...")

        # Znajdź sensory, które:
        # 1. NIE są już oznaczone jako offline (status != 2)
        # 2. Ich 'ostatnia_aktualizacja' jest starsza niż 'czas_odciecia'
        sensory_do_aktualizacji = db.query(AktualnyStan).filter(
            AktualnyStan.status != 2,
            (AktualnyStan.ostatnia_aktualizacja < czas_odciecia) | (AktualnyStan.ostatnia_aktualizacja == None)
        ).all()

        if sensory_do_aktualizacji:
            logger.info(f"TŁO: Znaleziono {len(sensory_do_aktualizacji)} przestarzałych sensorów. Ustawiam status 2.")
            for sensor in sensory_do_aktualizacji:
                sensor.status = 2
                sensor.ostatnia_aktualizacja = teraz_utc # Zapisujemy czas sprawdzenia
            db.commit()
        else:
             logger.info("TŁO: Wszystkie sensory są aktualne.")

    except Exception as e:
        logger.error(f"TŁO: Błąd podczas sprawdzania sensorów: {e}")
        if db:
            db.rollback()
    finally:
        # KRYTYCZNE: Zawsze zamykaj sesję w wątku.
        if db:
            db.close()

def sensor_checker_thread():
    """
    Główna pętla wątku sprawdzającego.
    Uruchamia `check_stale_sensors()` co 30 sekund.
    """
    while not shutdown_event_flag.is_set():
        check_stale_sensors()
        # Czekaj 30 sekund, ale sprawdzaj flagę co sekundę,
        # aby umożliwić szybkie zamknięcie aplikacji
        shutdown_event_flag.wait(30)
    logger.info("TŁO: Wątek sprawdzający zakończył działanie.")


# === ENDPOINT GŁÓWNY (/) ===
@app.get("/")
def read_root():
    """Zwraca komunikat powitalny na głównym adresie URL."""
    return {"message": "API działa poprawnie."} # Zwracamy poprawny JSON

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

# === STATYSTYKI (bez zmian) ===
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
        czas_rekordu_db = rekord.czas_pomiaru
        if czas_rekordu_db.tzinfo is None:
            czas_pl = czas_rekordu_db.replace(tzinfo=PL_TZ)
        else:
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

# === ENDPOINTY (bez zmian) ===

@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    token = request.device_token
    sensor_id = request.sensor_id
    teraz_utc = now_utc()
    miejsce = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    if not miejsce or miejsce.status != 0:
        raise HTTPException(status_code=409, detail="Miejsce zajęte lub offline.")
    wpis = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == token).first()
    if wpis:
        wpis.sensor_id = sensor_id
        wpis.czas_dodania = teraz_utc
    else:
        db.add(ObserwowaneMiejsca(device_token=token, sensor_id=sensor_id, czas_dodania=teraz_utc))
    db.commit()
    return {"status": "ok"}

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

@app.get("/api/v1/prognoza/wszystkie_miejsca")
def pobierz_prognoze(db: Session = Depends(get_db), target_date: Optional[str] = None, target_hour: Optional[int] = None):
    prognozy = {}
    teraz_pl = now_utc().astimezone(PL_TZ)
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

# === PRZETWARZANIE DANYCH (bez zmian) ===

def process_parking_update(dane: dict, db: Session):
    teraz_utc = now_utc()
    teraz_pl = teraz_utc.astimezone(PL_TZ)
    if "sensor_id" in dane:
        dane_cz = WymaganyFormat(**dane)
        sensor_id = dane_cz.sensor_id
        nowy_status = dane_cz.status
        nazwa_swieta = MANUALNA_MAPA_SWIAT.get(teraz_pl.date())
        if nazwa_swieta:
            db.add(DaneSwieta(
                czas_pomiaru=teraz_utc,
                sensor_id=sensor_id,
                status=nowy_status,
                nazwa_swieta=nazwa_swieta
            ))
        else:
            db.add(DaneHistoryczne(
                czas_pomiaru=teraz_utc,
                sensor_id=sensor_id,
                status=nowy_status
            ))
        miejsce = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
        poprzedni = -1
        if miejsce:
            poprzedni = miejsce.status
            miejsce.status = nowy_status
            miejsce.ostatnia_aktualizacja = teraz_utc
        else:
            db.add(AktualnyStan(
                sensor_id=sensor_id,
                status=nowy_status,
                ostatnia_aktualizacja=teraz_utc
            ))
        if poprzedni != 1 and nowy_status == 1:
            limit = datetime.timedelta(minutes=30)
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

@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
async def aktualizuj_miejsce_http(request: Request, db: Session = Depends(get_db)):
    raw = (await request.body()).decode("latin-1").replace("\x00", "")
    try:
        dane = json.loads(raw)
    except:
        raise HTTPException(status_code=400, detail="Zły JSON")
    return process_parking_update(dane, db)

# === UPROSZCZONY ENDPOINT: AKTUALNY STAN ===
# Usunęliśmy całą logikę sprawdzania.
# Teraz ten endpoint tylko odczytuje dane, które są 
# aktualizowane przez wątek w tle.

@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    logger.info("API: Pobieram aktualny stan z bazy danych.")
    # Po prostu zwróć to, co jest w bazie. Wątek w tle dba o statusy.
    return db.query(AktualnyStan).all()

# === MQTT ===

mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("MQTT: Połączone")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"MQTT: Błąd połączenia, kod: {rc}")

def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8").replace("\x00", "")
    try:
        dane = json.loads(raw)
    except Exception as e:
        logger.error(f"MQTT: Zły JSON: {e} | Payload: {raw}")
        return
    db = None
    try:
        db = next(get_db())
        process_parking_update(dane, db)
    except Exception as e:
        logger.error(f"MQTT: Błąd podczas przetwarzania wiadomości: {e}")
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
        logger.error(f"MQTT: Nie można uruchomić wątku: {e}")
    
    # Ta pętla utrzymuje wątek przy życiu i pozwala mu reagować na flagę
    while not shutdown_event_flag.is_set():
        time.sleep(1) # Śpij krótko, aby nie obciążać CPU
    
    logger.info("MQTT: Wątek słuchacza zakończył działanie.")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()


# === ZARZĄDZANIE STARTUP/SHUTDOWN ===
# Zmieniamy, aby uruchomić OBA wątki

@app.on_event("startup")
async def startup_event():
    # Uruchom wątek MQTT
    thread_mqtt = threading.Thread(target=mqtt_listener_thread)
    thread_mqtt.daemon = True # Wątek zamknie się, jeśli główny program padnie
    thread_mqtt.start()
    
    # Uruchom nasz nowy wątek do sprawdzania sensorów
    thread_checker = threading.Thread(target=sensor_checker_thread)
    thread_checker.daemon = True
    thread_checker.start()


@app.on_event("shutdown")
def shutdown_event():
    logger.info("Zatrzymywanie aplikacji...")
    # Ustaw flagę, która da znać *wszystkim* wątkom, że mają się zamknąć
    shutdown_event_flag.set()
    logger.info("Wysłano sygnał zamknięcia do wątków.")
    # Dajmy wątkom chwilę na zamknięcie się
    time.sleep(1.5)
