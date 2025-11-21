import os
import datetime
import json
import logging
import threading
import time
import asyncio
import secrets
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from datetime import date

# FastAPI & Async Utils
from fastapi import FastAPI, Depends, HTTPException, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# SQLAlchemy Sync & Async
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

# Security (Hashing)
from passlib.context import CryptContext

# Networking & Optymalizacja
import requests
import paho.mqtt.client as mqtt
import redis
import msgpack  # <--- BINARNE DANE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Strefy czasowe ===
UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

# === Haszowanie Haseł ===
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password[:72])

# === Konfiguracja Zmiennych Środowiskowych ===
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/tester/status")

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL — używam SQLite.")
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# === Baza Danych (Synchronous SQLAlchemy) ===
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === Redis (Cache) ===
REDIS_URL = os.environ.get('REDIS_URL')
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Redis: Połączono.")
    except: 
        logger.warning("Redis: Brak połączenia.")
        redis_client = None

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

# === MODELE BAZY DANYCH ===

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

# --- NOWE TABELE UŻYTKOWNIKA ---
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    token = Column(String, index=True, nullable=True)
    is_disabled = Column(Boolean, default=False)
    vehicles = relationship("Vehicle", back_populates="owner")

class Vehicle(Base):
    __tablename__ = "vehicles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    license_plate = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="vehicles")

# Utworzenie tabel
Base.metadata.create_all(bind=engine)

# === MODELE PYDANTIC (API REQUESTS) ===
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

class UserAuth(BaseModel):
    email: str
    password: str

class StatusUpdate(BaseModel):
    token: str
    is_disabled: bool

class VehicleAdd(BaseModel):
    token: str
    name: str
    license_plate: str

class RaportRequest(BaseModel):
    start_date: str
    end_date: str
    groups: List[str]
    include_workdays: bool
    include_weekends: bool
    include_holidays: bool

# === APLIKACJA FASTAPI ===
app = FastAPI(title="Parking API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === WEBSOCKET MANAGER (MessagePack) ===
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
        # OPTYMALIZACJA: Wysyłamy skrócony format MessagePack (binarny)
        # message to lista zmian np. [{'sensor_id': 'EURO_1', 'status': 1, ...}]
        # Dla oszczędności transferu, można tu mapować klucze na krótsze (id, s, t)
        
        try:
            message_binary = msgpack.packb(message)
            for connection in list(self.active_connections):
                try:
                    await connection.send_bytes(message_binary)
                except:
                    self.disconnect(connection)
        except Exception as e:
            logger.error(f"WS Broadcast Error: {e}")

manager = ConnectionManager()

# === Logika Biznesowa ===

def send_push_notification(token: str, title: str, body: str, data: dict):
    try:
        requests.post(
            "https://exp.host/--/api/v2/push/send",
            json={"to": token, "title": title, "body": body, "data": data},
            timeout=5
        )
    except: pass

async def process_parking_update(dane: dict, db: Session):
    teraz_utc = now_utc()
    teraz_pl = teraz_utc.astimezone(PL_TZ)
    zmiana_stanu = None
    
    if "sensor_id" in dane:
        sid = dane["sensor_id"]
        status = dane["status"]
        
        swieto = MANUALNA_MAPA_SWIAT.get(teraz_pl.date())
        if swieto: db.add(DaneSwieta(czas_pomiaru=teraz_utc, sensor_id=sid, status=status, nazwa_swieta=swieto))
        else: db.add(DaneHistoryczne(czas_pomiaru=teraz_utc, sensor_id=sid, status=status))
            
        m = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sid).first()
        prev = -1
        if m:
            prev = m.status
            m.status = status
            m.ostatnia_aktualizacja = teraz_utc
        else:
            db.add(AktualnyStan(sensor_id=sid, status=status, ostatnia_aktualizacja=teraz_utc))
        
        if prev != status:
            zmiana_stanu = {"sensor_id": sid, "status": status, "ostatnia_aktualizacja": teraz_utc.isoformat()}
            
            # Obsługa PUSH (Wolni sąsiedzi)
            if prev != 1 and status == 1:
                limit = datetime.timedelta(minutes=30)
                obs = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.sensor_id == sid, (teraz_utc - ObserwowaneMiejsca.czas_dodania) < limit).all()
                if obs:
                    grp = sid.split('_')[0]
                    free = db.query(AktualnyStan).filter(AktualnyStan.sensor_id.startswith(grp), AktualnyStan.sensor_id != sid, AktualnyStan.status == 0).first()
                    
                    tit, act, body, target = "❌ Zajęte!", "reroute", f"{sid} zajęte.", None
                    if free:
                        tit, act, body, target = "⚠️ Zmiana", "info", f"{sid} zajęte. Jedź na {free.sensor_id}!", free.sensor_id
                    
                    for o in obs:
                        send_push_notification(o.device_token, tit, body, {"sensor_id": sid, "action": act, "new_target": target})
                        db.delete(o)
                        
        db.commit()
        if zmiana_stanu and manager.active_connections: await manager.broadcast([zmiana_stanu])
        return {"status": "ok"}
    return {"status": "err"}

# === ENDPOINTY ===

@app.get("/")
def root(): return {"msg": "Parking API v2 (Auth + MsgPack)"}

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Błąd dashboard.html"

# --- AUTH ---
@app.post("/api/v1/auth/register")
def register(u: UserAuth, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == u.email).first():
        raise HTTPException(400, "Email zajęty")
    hashed = get_password_hash(u.password)
    db.add(User(email=u.email, hashed_password=hashed))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/auth/login")
def login(u: UserAuth, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == u.email).first()
    if not user or not verify_password(u.password, user.hashed_password):
        raise HTTPException(401, "Błędne dane")
    
    token = secrets.token_hex(16)
    user.token = token
    db.commit()
    return {"token": token, "email": user.email, "is_disabled": user.is_disabled}

# --- USER PROFILE ---
@app.get("/api/v1/user/me")
def get_me(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401, "Token expired")
    
    vehs = [{"name": v.name, "plate": v.license_plate} for v in user.vehicles]
    return {"email": user.email, "is_disabled": user.is_disabled, "vehicles": vehs}

@app.post("/api/v1/user/status")
def update_status(s: StatusUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user: raise HTTPException(401, "Auth error")
    user.is_disabled = s.is_disabled
    db.commit()
    return {"status": "updated"}

@app.post("/api/v1/user/vehicle")
def add_vehicle(v: VehicleAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first()
    if not user: raise HTTPException(401, "Auth error")
    db.add(Vehicle(name=v.name, license_plate=v.license_plate, user_id=user.id))
    db.commit()
    return {"status": "added"}

# --- CORE ---
@app.get("/api/v1/aktualny_stan")
def get_status(limit: int = 100, db: Session = Depends(get_db)):
    res = db.query(AktualnyStan).limit(limit).all()
    return [m.to_dict() for m in res]

@app.get("/api/v1/prognoza/wszystkie_miejsca")
def get_forecast(target_date: Optional[str] = None, target_hour: Optional[int] = None, db: Session = Depends(get_db)):
    # Uproszczona logika prognozy (placeholder)
    return {g: 0 for g in GRUPY_SENSOROW}

@app.post("/api/v1/statystyki/zajetosc")
def get_stats(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    # Redis Cache Logic
    key = f"stats:{z.sensor_id}:{z.selected_date}:{z.selected_hour}"
    if redis_client:
        cached = redis_client.get(key)
        if cached: return {"wynik_dynamiczny": json.loads(cached)}
    
    # (Tutaj normalnie Twoja funkcja calculate_occupancy_stats)
    # Dla uproszczenia zwracamy placeholder, jeśli nie wdrożyłeś pełnej logiki kalkulacji w tym pliku
    # Jeśli masz funkcję calculate_occupancy_stats z poprzednich wersji, wklej ją wyżej i użyj tutaj.
    wynik = {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": "Brak danych"}
    
    if redis_client: redis_client.set(key, json.dumps(wynik), ex=3600)
    return {"wynik_dynamiczny": wynik}

@app.post("/api/v1/obserwuj_miejsce")
def observe(r: ObserwujRequest, db: Session = Depends(get_db)):
    db.add(ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/dashboard/raport")
def report(req: RaportRequest, db: Session = Depends(get_db)):
    # Logika raportu dla dashboardu (uproszczona)
    return {g: [0]*24 for g in req.groups}

@app.post("/api/v1/debug/symulacja_sensora")
async def debug(d: WymaganyFormat, db: Session = Depends(get_db)):
    return await process_parking_update(d.dict(), db)

# --- MQTT & Background ---
mqtt_client = mqtt.Client()
shutdown_event = threading.Event()

def check_stale():
    while not shutdown_event.is_set():
        try:
            db = SessionLocal()
            cut = now_utc() - datetime.timedelta(minutes=3)
            old = db.query(AktualnyStan).filter(AktualnyStan.status != 2, (AktualnyStan.ostatnia_aktualizacja < cut)|(AktualnyStan.ostatnia_aktualizacja == None)).all()
            if old:
                chg = []
                for s in old:
                    s.status = 2
                    s.ostatnia_aktualizacja = now_utc()
                    chg.append(s.to_dict())
                db.commit()
                if chg and manager.active_connections:
                    asyncio.run_coroutine_threadsafe(manager.broadcast(chg), asyncio.get_event_loop())
            db.close()
        except: pass
        time.sleep(30)

def mqtt_thread():
    mqtt_client.on_connect = lambda c,u,f,rc: c.subscribe(MQTT_TOPIC_SUBSCRIBE)
    def on_msg(c,u,m):
        try:
            # Dekodowanie binarne: 2 bajty ID, 1 bajt STATUS
            sid_num = (m.payload[0]<<8)|m.payload[1]
            stat = int(m.payload[2])
            sid_str = SENSOR_MAP.get(sid_num)
            if sid_str:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                with SessionLocal() as db:
                    loop.run_until_complete(process_parking_update({"sensor_id": sid_str, "status": stat}, db))
        except: pass
    mqtt_client.on_message = on_msg
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except: pass

@app.on_event("startup")
async def start():
    threading.Thread(target=check_stale, daemon=True).start()
    threading.Thread(target=mqtt_thread, daemon=True).start()

@app.on_event("shutdown")
def stop():
    shutdown_event.set()
    mqtt_client.disconnect()

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            d = await ws.receive_text() # Ping
            if d!="ping": pass
            await ws.send_text("pong")
    except: manager.disconnect(ws)

