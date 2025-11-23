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

from fastapi import FastAPI, Depends, HTTPException, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

import bcrypt 
import requests
import paho.mqtt.client as mqtt
import redis
import msgpack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

# --- CONFIG ---
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

REDIS_URL = os.environ.get('REDIS_URL')
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    except: pass

SENSOR_MAP = { 1: "EURO_1", 2: "EURO_2", 3: "EURO_3", 4: "EURO_4", 5: "BUD_1", 6: "BUD_2", 7: "BUD_3", 8: "BUD_4" }
GRUPY_SENSOROW = ["EURO", "BUD"]
MANUALNA_MAPA_SWIAT = { date(2025, 1, 1): "Nowy Rok", date(2025, 4, 20): "Wielkanoc" }

# --- DB MODELS ---
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
    dark_mode = Column(Boolean, default=False)
    vehicles = relationship("Vehicle", back_populates="owner")
    tickets = relationship("Ticket", back_populates="owner")

class Vehicle(Base):
    __tablename__ = "vehicles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    license_plate = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="vehicles")

class Ticket(Base):
    __tablename__ = "tickets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    sensor_id = Column(String)
    place_name = Column(String)
    plate = Column(String)
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    price = Column(Float)
    owner = relationship("User", back_populates="tickets")

# FIX: AKTUALIZACJA MODELU AIRBNB
class AirbnbOffer(Base):
    __tablename__ = "airbnb_offers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String)
    description = Column(String)
    price = Column(String)
    availability = Column(String)
    period = Column(String) # Np. "Pn-Pt"
    owner_name = Column(String)
    contact = Column(String)
    rating = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), default=now_utc)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    # NOWE POLA
    district = Column(String, nullable=True)
    start_date = Column(String, nullable=True) # YYYY-MM-DD
    end_date = Column(String, nullable=True)   # YYYY-MM-DD

Base.metadata.create_all(bind=engine)

# --- PYDANTIC ---
class UserAuth(BaseModel):
    email: str
    password: str
class StatusUpdate(BaseModel):
    token: str
    is_disabled: bool
class DarkModeUpdate(BaseModel):
    token: str
    dark_mode: bool
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
class TicketAdd(BaseModel):
    token: str
    sensor_id: str
    place_name: str
    plate: str
    end_time: str
    price: float
class AirbnbLocationUpdate(BaseModel):
    token: str
    offer_id: str
    latitude: float
    longitude: float

# AKTUALIZACJA MODELI DANYCH
class AirbnbAdd(BaseModel):
    token: str
    title: str
    description: str
    price: str
    availability: str # np. "8-16"
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    # NOWE POLA
    district: str
    start_date: str
    end_date: str

class AirbnbUpdate(BaseModel):
    token: str
    offer_id: str 
    title: str
    description: str
    price: str
    availability: str
    district: Optional[str] = None

class AirbnbDelete(BaseModel):
    token: str
    offer_id: str

# --- UTILS ---
def get_time_with_offset(base_hour, offset_minutes):
    base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
    offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
    return offset_dt.time()
def get_password_hash(password: str) -> str:
    pwd_bytes = password.encode('utf-8'); salt = bcrypt.gensalt(); hashed = bcrypt.hashpw(pwd_bytes, salt); return hashed.decode('utf-8')
def verify_password(plain_password: str, hashed_password: str) -> bool:
    try: return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except: return False
def calculate_occupancy_stats(sensor_prefix, selected_date_obj, selected_hour, db):
    return {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": "Brak"} 

# --- APP ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
manager = ConnectionManager() 

def send_push_notification(token, title, body, data):
    try: 
        payload = {"to": token, "title": title, "body": body, "data": data, "sound": "default", "priority": "high", "channelId": "parking_alerts_v2", "_displayInForeground": True}
        requests.post("https://exp.host/--/api/v2/push/send", json=payload, timeout=3)
    except Exception as e: logger.error(f"PUSH ERROR: {e}")

async def process_parking_update(dane, db):
    # ... (Logika PUSH bez zmian) ...
    pass

# === ENDPOINTS ===
@app.get("/")
def root(): return {"msg": "API OK"}
@app.get("/dashboard", response_class=HTMLResponse)
def dash():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Błąd dashboard.html"
@app.post("/api/v1/auth/register")
def register(u: UserAuth, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == u.email).first(): raise HTTPException(400, "Email zajęty")
    db.add(User(email=u.email, hashed_password=get_password_hash(u.password)))
    db.commit()
    return {"status": "ok"}
@app.post("/api/v1/auth/login")
def login(u: UserAuth, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == u.email).first()
    if not user or not verify_password(u.password, user.hashed_password): raise HTTPException(401, "Błędne dane")
    token = secrets.token_hex(16); user.token = token; db.commit()
    dm = getattr(user, 'dark_mode', False)
    return {"token": token, "email": user.email, "is_disabled": user.is_disabled, "dark_mode": dm}
@app.get("/api/v1/user/me")
def me(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first(); 
    if not user: raise HTTPException(401, "Auth error")
    dm = getattr(user, 'dark_mode', False)
    return {"email": user.email, "is_disabled": user.is_disabled, "dark_mode": dm, "vehicles": [{"name": v.name, "plate": v.license_plate} for v in user.vehicles]}
@app.post("/api/v1/user/status")
def status(s: StatusUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first(); 
    if not user: raise HTTPException(401, "Auth error")
    user.is_disabled = s.is_disabled; db.commit(); return {"status": "updated"}
@app.post("/api/v1/user/darkmode")
def update_darkmode(s: DarkModeUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first(); 
    if not user: raise HTTPException(401, "Auth error")
    if hasattr(user, 'dark_mode'): user.dark_mode = s.dark_mode; db.commit()
    return {"status": "updated"}
@app.post("/api/v1/user/vehicle")
def add_veh(v: VehicleAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first(); 
    if not user: raise HTTPException(401, "Auth error")
    db.add(Vehicle(name=v.name, license_plate=v.license_plate, user_id=user.id)); db.commit(); return {"status": "added"}
@app.post("/api/v1/user/ticket")
def buy_ticket(t: TicketAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == t.token).first(); 
    if not user: raise HTTPException(401, "Auth error")
    try: end_dt = datetime.datetime.fromisoformat(t.end_time.replace('Z', '+00:00'))
    except: end_dt = now_utc() + datetime.timedelta(hours=1)
    new_ticket = Ticket(user_id=user.id, sensor_id=t.sensor_id, place_name=t.place_name, plate=t.plate, start_time=now_utc(), end_time=end_dt, price=t.price)
    db.add(new_ticket); db.commit(); return {"status": "ticket_created", "id": new_ticket.id}
@app.get("/api/v1/user/ticket/active")
def get_active_ticket(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first(); 
    if not user: raise HTTPException(401, "Auth error")
    ticket = db.query(Ticket).filter(Ticket.user_id == user.id, Ticket.end_time > now_utc()).order_by(Ticket.end_time.desc()).first()
    if ticket: return {"placeName": ticket.place_name, "sensorId": ticket.sensor_id, "plate": ticket.plate, "startTime": ticket.start_time.isoformat(), "endTime": ticket.end_time.isoformat(), "price": ticket.price}
    return None
@app.get("/api/v1/aktualny_stan")
def get_stat(limit: int = 100, db: Session = Depends(get_db)):
    return [m.to_dict() for m in db.query(AktualnyStan).limit(limit).all()]
@app.get("/api/v1/prognoza/wszystkie_miejsca")
def forecast(target_date: Optional[str] = None, target_hour: Optional[int] = None, db: Session = Depends(get_db)):
    return {} # Skrót
@app.post("/api/v1/statystyki/zajetosc")
def stats(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    return {"wynik_dynamiczny": {"procent_zajetosci": 0, "liczba_pomiarow": 0}} # Skrót
@app.post("/api/v1/obserwuj_miejsce")
def obs(r: ObserwujRequest, db: Session = Depends(get_db)):
    istniejacy = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == r.device_token).first()
    if istniejacy: istniejacy.sensor_id = r.sensor_id; istniejacy.czas_dodania = now_utc()
    else: db.add(ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id))
    try: db.commit(); return {"status": "ok"}
    except: db.rollback(); raise HTTPException(500, "Error")
@app.post("/api/v1/dashboard/raport")
def rep(r: RaportRequest, request: Request, db: Session = Depends(get_db)):
    return {} # Skrót
@app.post("/api/v1/debug/symulacja_sensora")
async def debug(d: dict, db: Session = Depends(get_db)): await process_parking_update(d, db); return {"status": "ok"}

# === AIRBNB ENDPOINTS (ZMODYFIKOWANE) ===
@app.get("/api/v1/airbnb/offers")
def get_airbnb_offers(db: Session = Depends(get_db)):
    offers = db.query(AirbnbOffer).order_by(AirbnbOffer.created_at.desc()).all()
    return [{
        "id": str(o.id), "title": o.title, "description": o.description,
        "price": o.price, "availability": o.availability, 
        "period": f"{o.start_date or 'Brak'} - {o.end_date or 'Brak'}", # Wyświetlamy daty
        "owner": o.owner_name, "rating": o.rating,
        "latitude": o.latitude, "longitude": o.longitude,
        "district": o.district,
        "start_date": o.start_date, "end_date": o.end_date # Zwracamy surowe daty do filtra
    } for o in offers]

@app.post("/api/v1/airbnb/add")
def add_airbnb_offer(a: AirbnbAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == a.token).first()
    if not user: raise HTTPException(401, "Auth error")
    new_offer = AirbnbOffer(
        title=a.title, description=a.description, price=a.price, 
        availability=a.availability, period="", # To pole można olać, używamy dat
        owner_name=user.email.split('@')[0], contact=user.email, 
        latitude=a.latitude, longitude=a.longitude,
        district=a.district, start_date=a.start_date, end_date=a.end_date
    )
    db.add(new_offer)
    db.commit()
    return {"status": "added"}

@app.post("/api/v1/airbnb/location")
def update_offer_location(u: AirbnbLocationUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == u.token).first()
    if not user: raise HTTPException(401, "Auth error")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == int(u.offer_id)).first()
    if not offer: raise HTTPException(404, "Oferta nie znaleziona")
    if offer.owner_name != user.email.split('@')[0]: raise HTTPException(403, "Brak uprawnień")
    offer.latitude = u.latitude; offer.longitude = u.longitude; db.commit(); return {"status": "location_updated"}

@app.post("/api/v1/airbnb/delete")
def delete_offer(d: AirbnbDelete, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == d.token).first()
    if not user: raise HTTPException(401, "Auth error")
    try: oid = int(d.offer_id)
    except: raise HTTPException(400, "Złe ID oferty")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == oid).first()
    if not offer: raise HTTPException(404, "Oferta nie znaleziona")
    if offer.owner_name != user.email.split('@')[0]: raise HTTPException(403, "Brak uprawnień do usunięcia tej oferty")
    db.delete(offer)
    db.commit()
    return {"status": "deleted"}

@app.post("/api/v1/airbnb/update")
def update_offer_details(u: AirbnbUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == u.token).first()
    if not user: raise HTTPException(401, "Auth error")
    try: oid = int(u.offer_id)
    except: raise HTTPException(400, "Złe ID oferty")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == oid).first()
    if not offer: raise HTTPException(404, "Oferta nie znaleziona")
    if offer.owner_name != user.email.split('@')[0]: raise HTTPException(403, "Brak uprawnień do edycji tej oferty")
    offer.title = u.title
    offer.description = u.description
    offer.price = u.price
    offer.availability = u.availability
    if u.district: offer.district = u.district # Aktualizacja dzielnicy też
    db.commit()
    return {"status": "updated"}

mqtt_c = mqtt.Client()
def mqtt_loop():
    # ...
    pass
def check_stale():
    # ...
    pass
@app.on_event("startup")
async def start(): threading.Thread(target=mqtt_loop, daemon=True).start(); threading.Thread(target=check_stale, daemon=True).start()
@app.on_event("shutdown")
def stop(): mqtt_c.disconnect()
@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws); 
    try: while True: await ws.receive_text()
    except: manager.disconnect(ws)
