import os
import datetime
import json
import logging
import threading
import time
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from datetime import date
from fastapi import FastAPI, Depends, HTTPException, Header, Request, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import requests
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt
import redis
import asyncio

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

# === [OPTYMALIZACJA] Konfiguracja Redis ===
REDIS_URL = os.environ.get('REDIS_URL')
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Połączono z Redis (Cache).")
    except Exception as e:
        logger.error(f"Nie można połączyć z Redis: {e}. Cache będzie wyłączony.")
        redis_client = None
else:
    logger.warning("Brak REDIS_URL. Cache będzie wyłączony.")

# === Słownik Mapowania Sensorów (MQTT Binarnie -> API) ===
SENSOR_MAP = {
    1: "EURO_1",
    2: "EURO_2",
    3: "EURO_3",
    4: "EURO_4",
    5: "BUD_1",
    6: "BUD_2",
    7: "BUD_3",
    8: "BUD_4",
    500: "TEST_PARKING_500",
}

# === Stałe ===
GRUPY_SENSOROW = ["EURO", "BUD"]

# === Rozszerzona Mapa Świąt 2023-2025 ===
MANUALNA_MAPA_SWIAT = {
    # --- 2023 ---
    date(2023, 11, 1): "Wszystkich Świętych",
    date(2023, 11, 11): "Święto Niepodległości",
    date(2023, 12, 24): "Wigilia",
    date(2023, 12, 25): "Boże Narodzenie",
    date(2023, 12, 26): "Drugi Dzień Świąt",
    date(2023, 12, 31): "Sylwester",
    
    # --- 2024 ---
    date(2024, 1, 1): "Nowy Rok",
    date(2024, 11, 1): "Wszystkich Świętych",
    date(2024, 11, 11): "Święto Niepodległości",
    date(2024, 12, 24): "Wigilia",
    date(2024, 12, 25): "Boże Narodzenie",
    date(2024, 12, 26): "Drugi Dzień Świąt",
    date(2024, 12, 31): "Sylwester",

    # --- 2025 ---
    date(2025, 1, 1): "Nowy Rok",
    date(2025, 4, 18): "Wielki Piątek",
    date(2025, 4, 20): "Wielkanoc",
    date(2025, 4, 21): "Poniedziałek Wielkanocny",
    date(2025, 11, 1): "Wszystkich Świętych",
    date(2025, 11, 11): "Święto Niepodległości",
    date(2025, 12, 24): "Wigilia",
    date(2025, 12, 25): "Boże Narodzenie",
    date(2025, 12, 26): "Drugi Dzień Świąt",
    date(2025, 12, 31): "Sylwester",
}

# === Tabele ===
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0)
    ostatnia_aktualizacja = Column(DateTime(timezone=True), default=now_utc)
    
    def to_dict(self):
        return {
            "sensor_id": self.sensor_id,
            "status": self.status,
            "ostatnia_aktualizacja": self.ostatnia_aktualizacja.isoformat() if self.ostatnia_aktualizacja else None
        }

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

# === INICJALIZACJA APLIKACJI ===
app = FastAPI(title="Parking API")

# === Menedżer Połączeń WebSocket ===
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket: Nowy klient połączony. Łącznie: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket: Klient rozłączony. Pozostało: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        message_json = json.dumps(message, default=str)
        
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message_json)
            except Exception as e:
                logger.warning(f"WebSocket: Błąd wysyłania, usuwam klienta: {e}")
                self.disconnect(connection)

manager = ConnectionManager()

# Flaga do bezpiecznego zatrzymywania wątków
shutdown_event_flag = threading.Event()

# === Wątek sprawdzający (Stale Sensors) ===
def check_stale_sensors():
    db = None
    try:
        db = SessionLocal()
        teraz_utc = now_utc()
        czas_odciecia = teraz_utc - datetime.timedelta(minutes=3)
        
        logger.info(f"TŁO: Sprawdzam sensory, które nie były aktualizowane od {czas_odciecia}...")

        sensory_do_aktualizacji = db.query(AktualnyStan).filter(
            AktualnyStan.status != 2,
            (AktualnyStan.ostatnia_aktualizacja < czas_odciecia) | (AktualnyStan.ostatnia_aktualizacja == None)
        ).all()

        if sensory_do_aktualizacji:
            logger.info(f"TŁO: Znaleziono {len(sensory_do_aktualizacji)} przestarzałych sensorów. Ustawiam status 2.")
            
            zmiany_do_broadcastu = []
            
            for sensor in sensory_do_aktualizacji:
                sensor.status = 2
                sensor.ostatnia_aktualizacja = teraz_utc
                zmiany_do_broadcastu.append(sensor.to_dict())

            db.commit()
            
            if manager.active_connections:
                logger.info("TŁO: Rozgłaszam zmiany (status 2) przez WebSocket...")
                asyncio.run_coroutine_threadsafe(manager.broadcast(zmiany_do_broadcastu), asyncio.get_event_loop())
                
        else:
            logger.info("TŁO: Wszystkie sensory są aktualne.")

    except Exception as e:
        logger.error(f"TŁO: Błąd podczas sprawdzania sensorów: {e}")
        if db:
            db.rollback()
    finally:
        if db:
            db.close()

def sensor_checker_thread():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    logger.info("TŁO: Wątek sprawdzający uruchomiony.")
    while not shutdown_event_flag.is_set():
        check_stale_sensors()
        shutdown_event_flag.wait(30)
    logger.info("TŁO: Wątek sprawdzający zakończył działanie.")

# === Konfiguracja CORS ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === MODELE (dla API) ===
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
def send_push_notification(token: str, title: str, body: str, data: dict):
    logger.info(f"Wysyłam PUSH do {token}: {title}")
    try:
        requests.post(
            "https://exp.host/--/api/v2/push/send",
            json={
                "to": token,
                "sound": "default",
                "title": title,
                "body": body,
                "data": data
            }
        )
    except Exception as e:
        logger.error(f"Błąd podczas wysyłania PUSH: {e}")

# === PRZETWARZANIE DANYCH (MQTT i Debug) ===
async def process_parking_update(dane: dict, db: Session):
    teraz_utc = now_utc()
    teraz_pl = teraz_utc.astimezone(PL_TZ)
    zmiana_stanu = None
    
    if "sensor_id" in dane:
        dane_cz = WymaganyFormat(**dane)
        sensor_id = dane_cz.sensor_id
        nowy_status = dane_cz.status
        nazwa_swieta = MANUALNA_MAPA_SWIAT.get(teraz_pl.date())
        
        # Zapis historii / świąt
        if nazwa_swieta:
            db.add(DaneSwieta(
                czas_pomiaru=teraz_utc, sensor_id=sensor_id,
                status=nowy_status, nazwa_swieta=nazwa_swieta
            ))
        else:
            db.add(DaneHistoryczne(
                czas_pomiaru=teraz_utc, sensor_id=sensor_id, status=nowy_status
            ))
            
        miejsce = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
        poprzedni = -1
        
        if miejsce:
            poprzedni = miejsce.status
            miejsce.status = nowy_status
            miejsce.ostatnia_aktualizacja = teraz_utc
        else:
            db.add(AktualnyStan(
                sensor_id=sensor_id, status=nowy_status,
                ostatnia_aktualizacja=teraz_utc
            ))
        
        # Wykryto zmianę
        if poprzedni != nowy_status:
            logger.info(f"WYKRYTO ZMIANĘ STANU dla {sensor_id}: {poprzedni} -> {nowy_status}")
            zmiana_stanu = {"sensor_id": sensor_id, "status": nowy_status, "ostatnia_aktualizacja": teraz_utc.isoformat()}
            
            # === LOGIKA POWIADOMIEŃ (INTELIGENTNA Z NEW_TARGET) ===
            if poprzedni != 1 and nowy_status == 1:
                # Limit 30 minut na stare obserwacje
                limit = datetime.timedelta(minutes=30)
                
                obserwatorzy = db.query(ObserwowaneMiejsca).filter(
                    ObserwowaneMiejsca.sensor_id == sensor_id,
                    (teraz_utc - ObserwowaneMiejsca.czas_dodania) < limit
                ).all()
                
                if obserwatorzy:
                    grupa_prefix = sensor_id.split('_')[0] # np. "EURO" z "EURO_1"
                    
                    wolni_sasiedzi = db.query(AktualnyStan).filter(
                        AktualnyStan.sensor_id.startswith(grupa_prefix), # Ten sam parking
                        AktualnyStan.sensor_id != sensor_id,             # Inne miejsce niż to zajęte
                        AktualnyStan.status == 0                         # Status Wolny
                    ).all()
                    
                    # Domyślnie: Panika (wszystko zajęte)
                    tytul_push = "❌ Miejsce zajęte!"
                    czynnosc = "reroute"
                    tresc_push = f"Miejsce {sensor_id} zostało zajęte. Szukam alternatywy..."
                    nowy_cel = None
                    
                    # Jeśli znaleziono wolnego sąsiada: Info + new_target
                    if wolni_sasiedzi:
                        najlepszy_sasiad = wolni_sasiedzi[0].sensor_id
                        tytul_push = "⚠️ Zmiana miejsca"
                        czynnosc = "info"
                        tresc_push = f"Miejsce {sensor_id} zajęte. Przekierowuję na {najlepszy_sasiad}!"
                        nowy_cel = najlepszy_sasiad

                    # Wysyłamy powiadomienia
                    for o in obserwatorzy:
                        send_push_notification(
                            o.device_token, 
                            tytul_push, 
                            tresc_push, 
                            {
                                "sensor_id": sensor_id, 
                                "action": czynnosc,
                                "new_target": nowy_cel # Wysyłamy ID nowego miejsca do apki
                            }
                        )
                    
                    # ZAWSZE czyścimy obserwację
                    db.query(ObserwowaneMiejsca).filter(
                        ObserwowaneMiejsca.device_token.in_([o.device_token for o in obserwatorzy])
                    ).delete(synchronize_session=False)
                    
        db.commit()
        
        if zmiana_stanu and manager.active_connections:
            await manager.broadcast([zmiana_stanu])
            
        return {"status": "ok"}
        
    return {"status": "unknown format"}

# === STATYSTYKI (Logika biznesowa) ===
def get_time_with_offset(base_hour, offset_minutes):
    base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
    offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
    return offset_dt.time()

def calculate_occupancy_stats(sensor_prefix: str, selected_date_obj: datetime.date, selected_hour: int, db: Session) -> dict:
    nazwa_swieta = MANUALNA_MAPA_SWIAT.get(selected_date_obj)
    query = None
    kategoria_str = ""
    dni_do_uwzglednienia = []

    # Określenie kategorii
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

    # Ustalenie przedziału czasowego +/- 60 minut
    OFFSET_MINUT = 60
    try:
        czas_poczatek = get_time_with_offset(selected_hour, -OFFSET_MINUT)
        czas_koniec = get_time_with_offset(selected_hour, OFFSET_MINUT)
    except Exception as e:
        logger.error(f"Błąd obliczania czasu (offset): {e}")
        return {"error": "Błąd obliczania czasu", "procent_zajetosci": 0, "liczba_pomiarow": 0}

    wszystkie = query.all()
    dane_pasujace = []

    for rekord in wszystkie:
        try:
            czas_rekordu_db = rekord.czas_pomiaru

            # 1. Zabezpieczenie przed NULL
            if not czas_rekordu_db:
                continue

            # 2. Zabezpieczenie przed String (fix dla SQLite)
            if isinstance(czas_rekordu_db, str):
                try:
                    # Próba parsowania stringa do datetime
                    czas_rekordu_db = datetime.datetime.fromisoformat(czas_rekordu_db)
                except ValueError:
                    try:
                        czas_rekordu_db = datetime.datetime.strptime(czas_rekordu_db, "%Y-%m-%d %H:%M:%S.%f")
                    except:
                        continue

            # 3. Konwersja strefy czasowej
            if czas_rekordu_db.tzinfo is None:
                czas_pl = czas_rekordu_db.replace(tzinfo=PL_TZ)
            else:
                czas_pl = czas_rekordu_db.astimezone(PL_TZ)

            # Filtrowanie po dniu tygodnia (jeśli to nie święto)
            if not nazwa_swieta:
                if czas_pl.weekday() not in dni_do_uwzglednienia:
                    continue

            czas_rek = czas_pl.time()
            
            pasuje_godzinowo = False
            if czas_poczatek > czas_koniec:
                if czas_rek >= czas_poczatek or czas_rek < czas_koniec:
                    pasuje_godzinowo = True
            else:
                if czas_poczatek <= czas_rek < czas_koniec:
                    pasuje_godzinowo = True

            if pasuje_godzinowo:
                dane_pasujace.append(rekord.status)

        except Exception as e:
            continue

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

# === ENDPOINTY ===

@app.get("/")
def read_root():
    return {"message": "API działa poprawnie."}

# === ENDPOINT DEBUGERSKI (Przesunięty tutaj, bo musi być po app i WymaganyFormat) ===
@app.post("/api/v1/debug/symulacja_sensora")
async def debug_sensor_update(dane: WymaganyFormat, db: Session = Depends(get_db)):
    logger.info(f"DEBUG: Otrzymano symulację z Postmana: {dane}")
    payload = dane.dict()
    
    try:
        # Wywołujemy funkcję logiki, która jest zdefiniowana wyżej
        wynik = await process_parking_update(payload, db)
        return wynik
    except Exception as e:
        logger.error(f"DEBUG ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
    if not z.sensor_id:
        raise HTTPException(status_code=400, detail="Brak sensor_id")

    # === NOWA LOGIKA GRUPOWANIA ===
    # Zamiast liczyć dla "EURO_1", bierzemy przedrostek "EURO"
    try:
        grupa_sensorow = z.sensor_id.split('_')[0]
    except IndexError:
        grupa_sensorow = z.sensor_id

    # 1. Sprawdź Cache (Redis) - klucz teraz zależy od GRUPY
    cache_key = f"stats:GROUP:{grupa_sensorow}:{z.selected_date}:{z.selected_hour}"
    
    if redis_client:
        try:
            cached_result = redis_client.get(cache_key)
            if cached_result:
                logger.info(f"CACHE: Zwracam wynik grupowy z Redis dla: {cache_key}")
                return {"wynik_dynamiczny": json.loads(cached_result)}
        except Exception as e:
            logger.error(f"CACHE: Błąd odczytu z Redis: {e}")
            
    logger.info(f"CACHE: Brak w cache, obliczam statystyki dla grupy: {grupa_sensorow}")
    
    try:
        try:
            selected_date = datetime.datetime.strptime(z.selected_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Zły format daty. Oczekiwano YYYY-MM-DD")
        
        # Wywołanie funkcji obliczającej dla CAŁEJ GRUPY
        wynik = calculate_occupancy_stats(grupa_sensorow, selected_date, z.selected_hour, db)
        
        if redis_client:
            try:
                redis_client.set(cache_key, json.dumps(wynik), ex=3600)
            except Exception as e:
                logger.error(f"CACHE: Błąd zapisu do Redis: {e}")
                
        return {"wynik_dynamiczny": wynik}

    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.exception("KRYTYCZNY BŁĄD w pobierz_statystyki:")
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")

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

@app.get("/api/v1/aktualny_stan")
async def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    logger.info("API: Pobieram aktualny stan (początkowy) z bazy danych.")
    wyniki = db.query(AktualnyStan).all()
    return [miejsce.to_dict() for miejsce in wyniki]

# === Endpoint WebSocket ===
@app.websocket("/ws/stan")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data != "ping":
                logger.debug(f"WebSocket: Otrzymano wiadomość od klienta: {data}")
            await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket: Błąd połączenia: {e}")
        manager.disconnect(websocket)

# === MQTT ===
mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("MQTT: Połączone")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"MQTT: Błąd połączenia, kod: {rc}")

def on_message(client, userdata, msg):
    raw_payload = msg.payload
    dane = {}

    try:
        if len(raw_payload) != 3:
            raise ValueError(f"Oczekiwano 3 bajtów [ID_H, ID_L, STATUS], otrzymano: {len(raw_payload)}")

        sensor_id_num = (raw_payload[0] << 8) | raw_payload[1]
        status_num = int(raw_payload[2])
        sensor_id_str = SENSOR_MAP.get(sensor_id_num)
        
        if not sensor_id_str:
            raise ValueError(f"Nieznane ID sensora w SENSOR_MAP: {sensor_id_num}")

        dane = {
            "sensor_id": sensor_id_str,
            "status": status_num
        }
        
    except Exception as e:
        logger.error(f"MQTT: Błąd parsowania wiadomości binarnej: {e} | Payload (raw): {raw_payload}")
        return
        
    db = None
    try:
        db = next(get_db())
        loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(process_parking_update(dane, db), loop)
    except Exception as e:
        logger.error(f"MQTT: Błąd podczas przetwarzania wiadomości (process_parking_update): {e}")
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
    
    while not shutdown_event_flag.is_set():
        time.sleep(1)
    
    logger.info("MQTT: Wątek słuchacza zakończył działanie.")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

# === ZARZĄDZANIE STARTUP/SHUTDOWN ===
@app.on_event("startup")
async def startup_event():
    thread_mqtt = threading.Thread(target=mqtt_listener_thread)
    thread_mqtt.daemon = True
    thread_mqtt.start()
    
    thread_checker = threading.Thread(target=sensor_checker_thread)
    thread_checker.daemon = True
    thread_checker.start()

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Zatrzymywanie aplikacji...")
    shutdown_event_flag.set()
    logger.info("Wysłano sygnał zamknięcia do wątków.")
    time.sleep(1.5)
