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

# Rate Limiting
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

# === Rate Limiter ===
limiter = Limiter(key_func=get_remote_address)

# === DATABASE (Async) ===
DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    # Fallback dla testów lokalnych (SQLite Async)
    DATABASE_URL = "sqlite+aiosqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL — używam SQLite (Async).")
else:
    # Fix dla Rendera (wymuszenie asyncpg)
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
    elif DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

try:
    engine = create_async_engine(DATABASE_URL, echo=False)
except Exception as e:
    logger.error(f"Błąd silnika DB: {e}")
    raise e

AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

# === Inicjalizacja ===
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

# === Redis ===
REDIS_URL = os.environ.get('REDIS_URL')
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Redis OK.")
    except: redis_client = None

# === Mapy ===
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

# === Tabele ===
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0)
    ostatnia_aktualizacja = Column(DateTime(timezone=True), default=now_utc)
    def to_dict(self):
        return {"sensor_id": self.sensor_id, "status": self.status, "ostatnia_aktualizacja": self.ostatnia_aktualizacja.isoformat() if self.ostatnia_aktualizacja else None}

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

async def init_models():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# === Modele ===
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

# === WebSocket ===
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
            try: await connection.send_text(message_json)
            except: self.disconnect(connection)

manager = ConnectionManager()

# === Logika Biznesowa (Async) ===

def get_time_with_offset(base_hour, offset_minutes):
    base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
    offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
    return offset_dt.time()

async def calculate_occupancy_stats_async(sensor_prefix: str, selected_date_obj: date, selected_hour: int, db: AsyncSession) -> dict:
    nazwa_swieta = MANUALNA_MAPA_SWIAT.get(selected_date_obj)
    kategoria_str = ""
    rows = []
    
    try:
        czas_poczatek = get_time_with_offset(selected_hour, -60)
        czas_koniec = get_time_with_offset(selected_hour, 60)
    except:
        return {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": "Błąd czasu"}

    if nazwa_swieta:
        kategoria_str = f"Święto: {nazwa_swieta}"
        res = await db.execute(select(DaneSwieta).filter(DaneSwieta.sensor_id.startswith(sensor_prefix)))
        rows = res.scalars().all()
    else:
        selected_weekday = selected_date_obj.weekday()
        dni_do_uwzglednienia = []
        if 0 <= selected_weekday <= 3:
            kategoria_str = "Dni robocze (Pn-Cz)"
            dni_do_uwzglednienia = [0, 1, 2, 3]
        elif selected_weekday == 4:
            kategoria_str = "Piątek"
            dni_do_uwzglednienia = [4]
        elif selected_weekday >= 5:
            kategoria_str = "Weekend"
            dni_do_uwzglednienia = [5, 6]

        res = await db.execute(select(DaneHistoryczne).filter(DaneHistoryczne.sensor_id.startswith(sensor_prefix)))
        rows = res.scalars().all()

        filtered_rows = []
        for row in rows:
            if not row.czas_pomiaru: continue
            local = row.czas_pomiaru.astimezone(PL_TZ) if row.czas_pomiaru.tzinfo else row.czas_pomiaru.replace(tzinfo=PL_TZ)
            if local.weekday() in dni_do_uwzglednienia:
                filtered_rows.append(row)
        rows = filtered_rows

    dane_pasujace = []
    for row in rows:
        if not row.czas_pomiaru: continue
        local = row.czas_pomiaru.astimezone(PL_TZ) if row.czas_pomiaru.tzinfo else row.czas_pomiaru.replace(tzinfo=PL_TZ)
        t = local.time()
        
        match = False
        if czas_poczatek > czas_koniec:
            if t >= czas_poczatek or t < czas_koniec: match = True
        else:
            if czas_poczatek <= t < czas_koniec: match = True
            
        if match: dane_pasujace.append(row.status)

    if not dane_pasujace:
        return {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": kategoria_str}

    zajete = dane_pasujace.count(1)
    total = len(dane_pasujace)
    procent = (zajete / total) * 100 if total > 0 else 0

    return {
        "procent_zajetosci": round(procent, 1),
        "liczba_pomiarow": total,
        "kategoria": kategoria_str,
        "przedzial_czasu": f"{czas_poczatek} - {czas_koniec}"
    }

# === Processing (POPRAWIONY URL v2) ===
def send_push_notification(token: str, title: str, body: str, data: dict):
    # ZMIANA: API v2
    url = "https://exp.host/--/api/v2/push/send"
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    payload = {
        "to": token,
        "title": title,
        "body": body,
        "data": data,
        "sound": "default",
        "priority": "high",
        "channelId": "parking-alerts"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        # Logowanie
        logger.info(f"--- PUSH NOTIFICATION ---")
        logger.info(f"Target: {token[:15]}...")
        logger.info(f"Status Code: {response.status_code}")
        
        try:
            resp_json = response.json()
            data_status = resp_json.get('data', {}).get('status')
            
            if response.status_code != 200:
                 logger.error(f"HTTP Error: {response.text}")
            elif data_status == "error":
                 logger.error(f"Expo API Logic Error: {resp_json}")
            else:
                 logger.info(f"Expo Success: {resp_json}")
                 
        except json.JSONDecodeError:
            logger.error(f"Non-JSON Response: {response.text}")
            
        logger.info(f"-------------------------")
        
    except Exception as e:
        logger.error(f"!!! CRITICAL PUSH ERROR (Network/Timeout) !!!: {e}")


async def process_parking_update(dane: dict, db: AsyncSession):
    teraz_utc = now_utc()
    teraz_pl = teraz_utc.astimezone(PL_TZ)
    zmiana_stanu = None
    
    if "sensor_id" in dane:
        sid = dane["sensor_id"]
        status = dane["status"]
        swieto = MANUALNA_MAPA_SWIAT.get(teraz_pl.date())
        
        if swieto: db.add(DaneSwieta(czas_pomiaru=teraz_utc, sensor_id=sid, status=status, nazwa_swieta=swieto))
        else: db.add(DaneHistoryczne(czas_pomiaru=teraz_utc, sensor_id=sid, status=status))
            
        res = await db.execute(select(AktualnyStan).where(AktualnyStan.sensor_id == sid))
        m = res.scalars().first()
        prev = -1
        if m:
            prev = m.status
            m.status = status
            m.ostatnia_aktualizacja = teraz_utc
        else:
            db.add(AktualnyStan(sensor_id=sid, status=status, ostatnia_aktualizacja=teraz_utc))
        
        if prev != status:
            zmiana_stanu = {"sensor_id": sid, "status": status, "ostatnia_aktualizacja": teraz_utc.isoformat()}
            
            if prev != 1 and status == 1:
                limit = datetime.timedelta(minutes=30)
                obs_res = await db.execute(select(ObserwowaneMiejsca).where(ObserwowaneMiejsca.sensor_id == sid, (teraz_utc - ObserwowaneMiejsca.czas_dodania) < limit))
                obs = obs_res.scalars().all()
                
                if obs:
                    logger.info(f"Znaleziono {len(obs)} obserwatorów dla {sid}")
                    grp = sid.split('_')[0]
                    free_res = await db.execute(select(AktualnyStan).where(AktualnyStan.sensor_id.startswith(grp), AktualnyStan.sensor_id != sid, AktualnyStan.status == 0))
                    free = free_res.scalars().all()
                    
                    tit, act, body, target = "❌ Zajęte!", "reroute", f"Miejsce {sid} zostało zajęte.", None
                    if free:
                        tit, act, body, target = "⚠️ Zmiana", "info", f"{sid} zajęte. Jedź na {free[0].sensor_id}!", free[0].sensor_id
                    
                    for o in obs:
                        send_push_notification(o.device_token, tit, body, {"sensor_id": sid, "action": act, "new_target": target})
                        await db.delete(o)
                        
        await db.commit()
        if zmiana_stanu and manager.active_connections: await manager.broadcast([zmiana_stanu])
        return {"status": "ok"}
    return {"status": "err"}

# === ENDPOINTY ===

@app.get("/")
def root(): return {"msg": "API OK"}

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    try:
        with open("dashboard.html", "r", encoding="utf-8") as f: return f.read()
    except: return "Błąd: Brak dashboard.html"

@app.get("/api/v1/aktualny_stan")
@limiter.limit("60/minute")
async def get_status(request: Request, limit: int = 100, db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(AktualnyStan).limit(limit))
    return [m.to_dict() for m in res.scalars().all()]

# --- NAPRAWIONY ENDPOINT PROGNOZY ---
@app.get("/api/v1/prognoza/wszystkie_miejsca")
async def get_forecast(target_date: Optional[str] = None, target_hour: Optional[int] = None, db: AsyncSession = Depends(get_db)):
    prognozy = {}
    teraz = now_utc().astimezone(PL_TZ)
    try:
        dt = datetime.datetime.strptime(target_date, "%Y-%m-%d").date() if target_date else teraz.date()
        hr = int(target_hour) if target_hour is not None else teraz.hour
    except:
        dt = teraz.date(); hr = teraz.hour
        
    for grp in GRUPY_SENSOROW:
        # Obliczamy statystyki ASYNCHRONICZNIE
        wynik = await calculate_occupancy_stats_async(grp, dt, hr, db)
        prognozy[grp] = wynik["procent_zajetosci"]
        
    return prognozy

# --- NAPRAWIONY ENDPOINT STATYSTYK ---
@app.post("/api/v1/statystyki/zajetosc")
async def get_stats(z: StatystykiZapytanie, db: AsyncSession = Depends(get_db)):
    try:
        grp = z.sensor_id.split('_')[0]
    except: grp = z.sensor_id
    
    key = f"stats:{grp}:{z.selected_date}:{z.selected_hour}"
    if redis_client:
        try:
            cached = redis_client.get(key)
            if cached: return {"wynik_dynamiczny": json.loads(cached)}
        except: pass
        
    try:
        date_obj = datetime.datetime.strptime(z.selected_date, "%Y-%m-%d").date()
    except: raise HTTPException(400, "Bad Date")
    
    wynik = await calculate_occupancy_stats_async(grp, date_obj, z.selected_hour, db)
    
    if redis_client:
        try: redis_client.set(key, json.dumps(wynik), ex=3600)
        except: pass
        
    return {"wynik_dynamiczny": wynik}

@app.post("/api/v1/obserwuj_miejsce")
async def observe(r: ObserwujRequest, db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(AktualnyStan).where(AktualnyStan.sensor_id == r.sensor_id))
    m = res.scalars().first()
    if not m or m.status != 0: raise HTTPException(409, "Zajęte")
    
    obs_check = await db.execute(select(ObserwowaneMiejsca).where(ObserwowaneMiejsca.device_token == r.device_token))
    wpis = obs_check.scalars().first()
    if wpis:
        wpis.sensor_id = r.sensor_id
        wpis.czas_dodania = now_utc()
    else:
        db.add(ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id))
    await db.commit()
    return {"status": "ok"}

@app.post("/api/v1/debug/symulacja_sensora")
async def debug(d: WymaganyFormat, db: AsyncSession = Depends(get_db)):
    return await process_parking_update(d.dict(), db)

@app.post("/api/v1/dashboard/raport")
async def report(req: RaportRequest, db: AsyncSession = Depends(get_db)):
    try:
        s_d = datetime.datetime.strptime(req.start_date, "%Y-%m-%d").date()
        e_d = datetime.datetime.strptime(req.end_date, "%Y-%m-%d").date()
    except: raise HTTPException(400, "Date error")
    
    rows = []
    if req.include_workdays or req.include_weekends:
        res = await db.execute(select(DaneHistoryczne).filter(DaneHistoryczne.czas_pomiaru >= s_d, DaneHistoryczne.czas_pomiaru <= e_d + datetime.timedelta(days=1)))
        rows.extend(res.scalars().all())
    if req.include_holidays:
        res = await db.execute(select(DaneSwieta).filter(DaneSwieta.czas_pomiaru >= s_d, DaneSwieta.czas_pomiaru <= e_d + datetime.timedelta(days=1)))
        rows.extend(res.scalars().all())
        
    agg = {g: {h: [] for h in range(24)} for g in req.groups}
    for r in rows:
        local = r.czas_pomiaru.astimezone(PL_TZ) if r.czas_pomiaru.tzinfo else r.czas_pomiaru.replace(tzinfo=PL_TZ)
        h = local.hour
        wd = local.date().weekday()
        is_hol = isinstance(r, DaneSwieta)
        
        if is_hol and not req.include_holidays: continue
        if not is_hol:
            if wd >= 5 and not req.include_weekends: continue
            if wd < 5 and not req.include_workdays: continue
        
        try:
            g = r.sensor_id.split('_')[0]
            if g in agg: agg[g][h].append(r.status)
        except: pass
        
    fin = {}
    for g in req.groups:
        fin[g] = []
        for h in range(24):
            v = agg[g][h]
            fin[g].append(round(sum(v)/len(v)*100, 1) if v else 0)
    return fin

# --- Background & MQTT ---
loop_main = None

async def check_stale():
    while True:
        try:
            async with AsyncSessionLocal() as db:
                cut = now_utc() - datetime.timedelta(minutes=3)
                res = await db.execute(select(AktualnyStan).where(AktualnyStan.status != 2, (AktualnyStan.ostatnia_aktualizacja < cut)|(AktualnyStan.ostatnia_aktualizacja == None)))
                sensors = res.scalars().all()
                if sensors:
                    chg = []
                    for s in sensors:
                        s.status = 2
                        s.ostatnia_aktualizacja = now_utc()
                        chg.append(s.to_dict())
                    await db.commit()
                    if manager.active_connections: await manager.broadcast(chg)
        except: pass
        await asyncio.sleep(30)

mqtt_c = mqtt.Client()
def run_mqtt():
    asyncio.set_event_loop(asyncio.new_event_loop())
    mqtt_c.on_connect = lambda c,u,f,rc: c.subscribe(MQTT_TOPIC_SUBSCRIBE)
    mqtt_c.on_message = lambda c,u,m: asyncio.run_coroutine_threadsafe(process_async_wrapper(SENSOR_MAP.get((m.payload[0]<<8)|m.payload[1]), int(m.payload[2])), loop_main) if SENSOR_MAP.get((m.payload[0]<<8)|m.payload[1]) else None
    try:
        mqtt_c.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_c.loop_forever()
    except: pass

async def process_async_wrapper(sid, stat):
    async with AsyncSessionLocal() as db: await process_parking_update({"sensor_id": sid, "status": stat}, db)

@app.on_event("startup")
async def start():
    global loop_main
    loop_main = asyncio.get_running_loop()
    await init_models()
    asyncio.create_task(check_stale())
    threading.Thread(target=run_mqtt, daemon=True).start()

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            d = await ws.receive_text()
            if d!="ping": await ws.send_text("pong")
    except: manager.disconnect(ws)
