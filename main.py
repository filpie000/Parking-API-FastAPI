import os
import datetime
import json
import logging
import threading
import time
import asyncio
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from datetime import date

# FastAPI & Async Utils
from fastapi import FastAPI, Depends, HTTPException, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# SQLAlchemy Async
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

# Rate Limiting (Ochrona przed spamem)
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Networking
import requests
import paho.mqtt.client as mqtt
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Strefy czasowe ===
UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

# === Rate Limiter (Ochrona) ===
limiter = Limiter(key_func=get_remote_address)

# === DATABASE (Async Configuration) ===
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    # Fallback dla testów lokalnych (SQLite też może być async, ale tu zakładamy Postgres na produkcji)
    # Do testów lokalnych bez Postgresa potrzebujesz sterownika aiosqlite
    DATABASE_URL = "sqlite+aiosqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL — używam SQLite (Async).")
elif DATABASE_URL.startswith("postgres://"):
    # Fix dla Rendera/Heroku (wymagane dla asyncpg)
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# Tworzenie silnika asynchronicznego
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# Dependency Injection (Async)
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

# === Inicjalizacja Aplikacji ===
app = FastAPI(title="Parking API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === MQTT Config ===
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/tester/status")

# === Redis (Cache) ===
REDIS_URL = os.environ.get('REDIS_URL')
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Połączono z Redis (Cache).")
    except Exception as e:
        logger.error(f"Redis Error: {e}")

# === Mapy i Stałe ===
SENSOR_MAP = {
    1: "EURO_1", 2: "EURO_2", 3: "EURO_3", 4: "EURO_4",
    5: "BUD_1", 6: "BUD_2", 7: "BUD_3", 8: "BUD_4",
    500: "TEST_PARKING_500",
}
GRUPY_SENSOROW = ["EURO", "BUD"]

MANUALNA_MAPA_SWIAT = {
    date(2023, 11, 1): "Wszystkich Świętych", date(2023, 11, 11): "Święto Niepodległości",
    date(2023, 12, 24): "Wigilia", date(2023, 12, 25): "Boże Narodzenie",
    date(2024, 1, 1): "Nowy Rok", date(2024, 11, 1): "Wszystkich Świętych",
    date(2025, 1, 1): "Nowy Rok", date(2025, 4, 20): "Wielkanoc",
}

# === Modele Bazy Danych ===
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

# Tworzenie tabel (Sync - wymagane przy starcie dla SQLAlchemy)
# W środowisku produkcyjnym lepiej używać Alembic do migracji
async def init_models():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# === Modele Pydantic ===
class WymaganyFormat(BaseModel):
    sensor_id: str
    status: int
class ObserwujRequest(BaseModel):
    sensor_id: str
    device_token: str
class StatystykiZapytanie(BaseModel):
    sensor_id: str
    selected_date: str
    selected_hour: int
class RaportRequest(BaseModel):
    start_date: str
    end_date: str
    groups: List[str]
    include_workdays: bool
    include_weekends: bool
    include_holidays: bool

# === WebSocket Manager ===
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        message_json = json.dumps(message, default=str)
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message_json)
            except:
                self.disconnect(connection)

manager = ConnectionManager()
shutdown_event_flag = threading.Event()

# === PUSH Helper ===
def send_push_notification(token: str, title: str, body: str, data: dict):
    # To jest zapytanie HTTP, może być blokujące, w idealnym świecie powinno być asynchroniczne (aiohttp)
    # Ale przy małej skali requests w wątku jest akceptowalne.
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={"to": token, "title": title, "body": body, "data": data}, timeout=2)
    except Exception as e:
        logger.error(f"Push Error: {e}")

# === CORE LOGIC (Async) ===
async def process_parking_update(dane: dict, db: AsyncSession):
    teraz_utc = now_utc()
    teraz_pl = teraz_utc.astimezone(PL_TZ)
    zmiana_stanu = None
    
    if "sensor_id" in dane:
        sensor_id = dane["sensor_id"]
        nowy_status = dane["status"]
        nazwa_swieta = MANUALNA_MAPA_SWIAT.get(teraz_pl.date())
        
        # Zapis Historii
        if nazwa_swieta:
            db.add(DaneSwieta(czas_pomiaru=teraz_utc, sensor_id=sensor_id, status=nowy_status, nazwa_swieta=nazwa_swieta))
        else:
            db.add(DaneHistoryczne(czas_pomiaru=teraz_utc, sensor_id=sensor_id, status=nowy_status))
            
        # Aktualizacja Stanu
        result = await db.execute(select(AktualnyStan).where(AktualnyStan.sensor_id == sensor_id))
        miejsce = result.scalars().first()
        
        poprzedni = -1
        if miejsce:
            poprzedni = miejsce.status
            miejsce.status = nowy_status
            miejsce.ostatnia_aktualizacja = teraz_utc
        else:
            db.add(AktualnyStan(sensor_id=sensor_id, status=nowy_status, ostatnia_aktualizacja=teraz_utc))
        
        # Logika Powiadomień (Sąsiad)
        if poprzedni != nowy_status:
            zmiana_stanu = {"sensor_id": sensor_id, "status": nowy_status, "ostatnia_aktualizacja": teraz_utc.isoformat()}
            
            if poprzedni != 1 and nowy_status == 1:
                limit = datetime.timedelta(minutes=30)
                # Pobierz obserwatorów
                obs_res = await db.execute(select(ObserwowaneMiejsca).where(
                    ObserwowaneMiejsca.sensor_id == sensor_id,
                    (teraz_utc - ObserwowaneMiejsca.czas_dodania) < limit
                ))
                obserwatorzy = obs_res.scalars().all()
                
                if obserwatorzy:
                    grupa = sensor_id.split('_')[0]
                    # Szukaj wolnego sąsiada
                    sasiad_res = await db.execute(select(AktualnyStan).where(
                        AktualnyStan.sensor_id.startswith(grupa),
                        AktualnyStan.sensor_id != sensor_id,
                        AktualnyStan.status == 0
                    ))
                    wolni = sasiad_res.scalars().all()
                    
                    tytul, akcja, tresc, cel = "❌ Miejsce zajęte!", "reroute", f"Miejsce {sensor_id} zajęte.", None
                    if wolni:
                        tytul, akcja, tresc, cel = "⚠️ Zmiana", "info", f"{sensor_id} zajęte. Jedź na {wolni[0].sensor_id}!", wolni[0].sensor_id
                    
                    for o in obserwatorzy:
                        send_push_notification(o.device_token, tytul, tresc, {"sensor_id": sensor_id, "action": akcja, "new_target": cel})
                    
                    # Usuń obserwacje (delete w SQLAlchemy 1.4/2.0 async jest inne, tu prościej usunąć po ID w pętli lub osobnym query delete)
                    # Dla uproszczenia usuwamy w pętli
                    for o in obserwatorzy:
                        await db.delete(o)
                        
        await db.commit()
        
        if zmiana_stanu and manager.active_connections:
            await manager.broadcast([zmiana_stanu])
        
        return {"status": "ok"}
    return {"status": "error"}

# === ENDPOINTY ===

@app.get("/")
def read_root():
    return {"message": "API (Async) działa poprawnie."}

# Dashboard
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    try:
        with open("dashboard.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "Błąd: Brak pliku dashboard.html"

# --- Pobieranie Stanu (Z Rate Limitem) ---
@app.get("/api/v1/aktualny_stan")
@limiter.limit("60/minute") # Max 60 zapytań na minutę z jednego IP
async def pobierz_aktualny_stan(request: Request, limit: int = 100, db: AsyncSession = Depends(get_db)):
    # Używamy select() dla async
    result = await db.execute(select(AktualnyStan).limit(limit))
    wyniki = result.scalars().all()
    return [miejsce.to_dict() for miejsce in wyniki]

# --- Debug / Symulacja ---
@app.post("/api/v1/debug/symulacja_sensora")
async def debug_sensor(dane: WymaganyFormat, db: AsyncSession = Depends(get_db)):
    return await process_parking_update(dane.dict(), db)

# --- Obserwacja ---
@app.post("/api/v1/obserwuj_miejsce")
async def obserwuj_miejsce(request: ObserwujRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AktualnyStan).where(AktualnyStan.sensor_id == request.sensor_id))
    miejsce = result.scalars().first()
    
    if not miejsce or miejsce.status != 0:
        raise HTTPException(status_code=409, detail="Miejsce zajęte/offline")
        
    # Sprawdź czy już obserwuje
    res_obs = await db.execute(select(ObserwowaneMiejsca).where(ObserwowaneMiejsca.device_token == request.device_token))
    wpis = res_obs.scalars().first()
    
    if wpis:
        wpis.sensor_id = request.sensor_id
        wpis.czas_dodania = now_utc()
    else:
        db.add(ObserwowaneMiejsca(device_token=request.device_token, sensor_id=request.sensor_id, czas_dodania=now_utc()))
    
    await db.commit()
    return {"status": "ok"}

# --- Raport Zbiorczy (Dashboard) ---
@app.post("/api/v1/dashboard/raport")
async def generuj_raport(req: RaportRequest, db: AsyncSession = Depends(get_db)):
    try:
        s_date = datetime.datetime.strptime(req.start_date, "%Y-%m-%d").date()
        e_date = datetime.datetime.strptime(req.end_date, "%Y-%m-%d").date()
    except: raise HTTPException(400, "Data error")

    # Pobieranie danych (Non-blocking)
    rows = []
    if req.include_workdays or req.include_weekends:
        res = await db.execute(select(DaneHistoryczne).filter(
            DaneHistoryczne.czas_pomiaru >= s_date, 
            DaneHistoryczne.czas_pomiaru <= e_date + datetime.timedelta(days=1)
        ))
        rows.extend(res.scalars().all())

    if req.include_holidays:
        res = await db.execute(select(DaneSwieta).filter(
            DaneSwieta.czas_pomiaru >= s_date, 
            DaneSwieta.czas_pomiaru <= e_date + datetime.timedelta(days=1)
        ))
        rows.extend(res.scalars().all())

    # Agregacja w Pythonie (szybka dla <100k rekordów, dla milionów przenieść do SQL)
    agregacja = {g: {h: [] for h in range(24)} for g in req.groups}

    for row in rows:
        local_time = row.czas_pomiaru.astimezone(PL_TZ) if row.czas_pomiaru.tzinfo else row.czas_pomiaru.replace(tzinfo=PL_TZ)
        hour = local_time.hour
        weekday = local_time.date().weekday()
        is_hol = isinstance(row, DaneSwieta)
        
        # Filtrowanie
        if is_hol and not req.include_holidays: continue
        if not is_hol:
            if (weekday >= 5) and not req.include_weekends: continue
            if (weekday < 5) and not req.include_workdays: continue
            
        try:
            grp = row.sensor_id.split('_')[0]
            if grp in agregacja: agregacja[grp][hour].append(row.status)
        except: pass

    wynik = {}
    for g in req.groups:
        wynik[g] = []
        for h in range(24):
            vals = agregacja[g][h]
            wynik[g].append(round((sum(vals)/len(vals)*100), 1) if vals else 0)
            
    return wynik

# --- Tło (Sync wrapper for async DB in Thread) ---
# Uwaga: Używanie wątków z Async DB jest trudne. 
# Dla uproszczenia w check_stale_sensors stworzymy nową pętlę zdarzeń lub użyjemy synchronicznego zapytania?
# Najbezpieczniej przy asyncpg jest używać asyncio.create_task w startup event zamiast threadingu.
async def check_stale_task():
    while True:
        try:
            async with AsyncSessionLocal() as db:
                cutoff = now_utc() - datetime.timedelta(minutes=3)
                res = await db.execute(select(AktualnyStan).where(
                    AktualnyStan.status != 2,
                    (AktualnyStan.ostatnia_aktualizacja < cutoff) | (AktualnyStan.ostatnia_aktualizacja == None)
                ))
                sensors = res.scalars().all()
                
                if sensors:
                    changes = []
                    for s in sensors:
                        s.status = 2
                        s.ostatnia_aktualizacja = now_utc()
                        changes.append(s.to_dict())
                    await db.commit()
                    if manager.active_connections:
                        await manager.broadcast(changes)
        except Exception as e:
            logger.error(f"Stale Check Error: {e}")
        
        await asyncio.sleep(30)

# --- MQTT Run ---
mqtt_client = mqtt.Client()

def run_mqtt():
    # MQTT działa w osobnym wątku, ale musi wywoływać async DB
    # Używamy run_coroutine_threadsafe
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    def on_msg(c, u, m):
        try:
            sid = SENSOR_MAP.get((m.payload[0]<<8)|m.payload[1])
            if sid:
                # Wywołanie async funkcji z wątku sync
                future = asyncio.run_coroutine_threadsafe(
                    process_async_wrapper(sid, int(m.payload[2])), loop_main
                )
        except: pass

    mqtt_client.on_connect = lambda c, u, f, rc: c.subscribe(MQTT_TOPIC_SUBSCRIBE)
    mqtt_client.on_message = on_msg
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except: pass

# Helper do mostkowania wątków
async def process_async_wrapper(sensor_id, status):
    async with AsyncSessionLocal() as db:
        await process_parking_update({"sensor_id": sensor_id, "status": status}, db)

# --- Startup ---
loop_main = None

@app.on_event("startup")
async def startup():
    global loop_main
    loop_main = asyncio.get_running_loop()
    
    await init_models() # Utwórz tabele
    
    # Uruchom zadanie w tle (zamiast wątku) - to jest "Async way"
    asyncio.create_task(check_stale_task())
    
    # MQTT musi być w wątku bo biblioteka paho jest synchroniczna
    t = threading.Thread(target=run_mqtt, daemon=True)
    t.start()

@app.websocket("/ws/stan")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data != "ping": await websocket.send_text("pong")
    except: manager.disconnect(websocket)
