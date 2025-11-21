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

# FastAPI
from fastapi import FastAPI, Depends, HTTPException, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

# Security (BCRYPT bezpośrednio)
import bcrypt 

# Networking
import requests
import paho.mqtt.client as mqtt
import redis
import msgpack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === CONFIG ===
UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/tester/status")

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./parking_data.db"
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

# === REDIS ===
REDIS_URL = os.environ.get('REDIS_URL')
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    except: pass

# === SECURITY (BCRYPT FIX) ===
def get_password_hash(password: str) -> str:
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pwd_bytes, salt)
    return hashed.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        pwd_bytes = plain_password.encode('utf-8')
        hash_bytes = hashed_password.encode('utf-8')
        return bcrypt.checkpw(pwd_bytes, hash_bytes)
    except:
        return False

# === STAŁE ===
SENSOR_MAP = { 1: "EURO_1", 2: "EURO_2", 3: "EURO_3", 4: "EURO_4", 5: "BUD_1", 6: "BUD_2", 7: "BUD_3", 8: "BUD_4" }
GRUPY_SENSOROW = ["EURO", "BUD"]
MANUALNA_MAPA_SWIAT = { date(2025, 1, 1): "Nowy Rok", date(2025, 4, 20): "Wielkanoc" }

# === MODELE DB ===
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0)
    ostatnia_aktualizacja = Column(DateTime(timezone=True), default=now_utc)
    def to_dict(self): return {"sensor_id": self.sensor_id, "status": self.status, "ostatnia_aktualizacja": self.ostatnia_aktualizacja.isoformat() if self.ostatnia_aktualizacja else None}

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
    nazwa_swieta = Column(String)

class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    device_token = Column(String, primary_key=True, index=True)
    sensor_id = Column(String, index=True)
    czas_dodania = Column(DateTime(timezone=True), default=now_utc)

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

Base.metadata.create_all(bind=engine)

# === PYDANTIC ===
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

# === APP ===
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class ConnectionManager:
    def __init__(self): self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections: self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        try:
            message_binary = msgpack.packb(message)
            for connection in list(self.active_connections):
                try: await connection.send_bytes(message_binary)
                except: self.disconnect(connection)
        except: pass
manager = ConnectionManager()

# === LOGIC ===
def send_push_notification(token, title, body, data):
    try: requests.post("https://exp.host/--/api/v2/push/send", json={"to": token, "title": title, "body": body, "data": data}, timeout=5)
    except: pass

async def process_parking_update(dane: dict, db: Session):
    if "sensor_id" not in dane: return
    sid = dane["sensor_id"]; status = dane["status"]; teraz = now_utc()
    
    db.add(DaneHistoryczne(czas_pomiaru=teraz, sensor_id=sid, status=status))
    
    m = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sid).first()
    prev = -1
    if m:
        prev = m.status; m.status = status; m.ostatnia_aktualizacja = teraz
    else:
        db.add(AktualnyStan(sensor_id=sid, status=status, ostatnia_aktualizacja=teraz))
    
    if prev != status:
        chg = {"sensor_id": sid, "status": status}
        # PUSH LOGIC here (skrócona dla czytelności)
        if prev != 1 and status == 1:
             limit = datetime.timedelta(minutes=30)
             obs = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.sensor_id == sid, (teraz - ObserwowaneMiejsca.czas_dodania) < limit).all()
             if obs:
                 for o in obs:
                     send_push_notification(o.device_token, "Zajęte!", f"{sid} zajęte.", {"action": "reroute"})
                     db.delete(o)
        
        db.commit()
        if manager.active_connections: await manager.broadcast([chg])

# === ENDPOINTS ===
@app.get("/")
def root(): return {"msg": "API OK"}

@app.get("/dashboard", response_class=HTMLResponse)
def dash():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Błąd dashboard.html"

@app.post("/api/v1/auth/register")
def register(u: UserAuth, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == u.email).first():
        raise HTTPException(400, "Email zajęty")
    db.add(User(email=u.email, hashed_password=get_password_hash(u.password)))
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

@app.get("/api/v1/user/me")
def me(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401, "Auth error")
    return {"email": user.email, "is_disabled": user.is_disabled, "vehicles": [{"name": v.name, "plate": v.license_plate} for v in user.vehicles]}

@app.post("/api/v1/user/status")
def status(s: StatusUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user: raise HTTPException(401, "Auth error")
    user.is_disabled = s.is_disabled
    db.commit()
    return {"status": "updated"}

@app.post("/api/v1/user/vehicle")
def add_veh(v: VehicleAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first()
    if not user: raise HTTPException(401, "Auth error")
    db.add(Vehicle(name=v.name, license_plate=v.license_plate, user_id=user.id))
    db.commit()
    return {"status": "added"}

@app.get("/api/v1/aktualny_stan")
def get_stat(limit: int = 100, db: Session = Depends(get_db)):
    return [m.to_dict() for m in db.query(AktualnyStan).limit(limit).all()]

@app.get("/api/v1/prognoza/wszystkie_miejsca")
def forecast(): return {g: 0 for g in GRUPY_SENSOROW}

@app.post("/api/v1/statystyki/zajetosc")
def stats(z: StatystykiZapytanie):
    key = f"stats:{z.sensor_id}:{z.selected_date}:{z.selected_hour}"
    if redis_client:
        c = redis_client.get(key)
        if c: return {"wynik_dynamiczny": json.loads(c)}
    # Placeholder logic
    res = {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": "Brak danych"}
    if redis_client: redis_client.set(key, json.dumps(res), ex=3600)
    return {"wynik_dynamiczny": res}

@app.post("/api/v1/obserwuj_miejsce")
def obs(r: ObserwujRequest, db: Session = Depends(get_db)):
    db.add(ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/dashboard/raport")
def rep(r: RaportRequest): return {g: [0]*24 for g in r.groups}

# MQTT & WS
mqtt_c = mqtt.Client()
def mqtt_loop():
    mqtt_c.on_connect = lambda c,u,f,r: c.subscribe(MQTT_TOPIC_SUBSCRIBE)
    def on_m(c,u,m):
        try:
            sid_str = SENSOR_MAP.get((m.payload[0]<<8)|m.payload[1])
            if sid_str:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                with SessionLocal() as db:
                    loop.run_until_complete(process_parking_update({"sensor_id": sid_str, "status": int(m.payload[2])}, db))
        except: pass
    mqtt_c.on_message = on_m
    try: mqtt_c.connect(MQTT_BROKER, MQTT_PORT, 60); mqtt_c.loop_forever()
    except: pass

@app.on_event("startup")
async def start(): threading.Thread(target=mqtt_loop, daemon=True).start()

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True: await ws.receive_text()
    except: manager.disconnect(ws)
