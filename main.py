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

class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    role = Column(String) # 'ALL', 'EURO', 'BUD'
    badge_name = Column(String)

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
    
    # NOWE UPRAWNIENIA (RBAC)
    perm_euro = Column(Boolean, default=False)     # Dostęp do strefy EURO
    perm_ev = Column(Boolean, default=False)       # Dostęp do ładowarek
    perm_disabled = Column(Boolean, default=False) # Dostęp do miejsc niepełnosprawnych

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

class AirbnbOffer(Base):
    __tablename__ = "airbnb_offers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String)
    description = Column(String)
    price = Column(String)
    availability = Column(String)
    period = Column(String)
    owner_name = Column(String)
    contact = Column(String)
    rating = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), default=now_utc)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    district = Column(String, nullable=True)
    start_date = Column(String, nullable=True)
    end_date = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# --- PYDANTIC ---
class UserAuth(BaseModel):
    email: str
    password: str
class AdminLogin(BaseModel):
    username: str
    password: str
class StatusUpdate(BaseModel):
    token: str
    is_disabled: bool
class AdminStatusUpdate(BaseModel):
    target_email: str
    is_disabled: bool
# NOWY MODEL AKTUALIZACJI UPRAWNIEŃ
class UserPermissionsUpdate(BaseModel):
    target_email: str
    perm_euro: bool
    perm_ev: bool
    perm_disabled: bool

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
class AirbnbAdd(BaseModel):
    token: str
    title: str
    description: str
    price: str
    availability: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    district: str
    start_date: str
    end_date: str
class AirbnbLocationUpdate(BaseModel):
    token: str
    offer_id: str
    latitude: float
    longitude: float
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

# ZAKTUALIZOWANY MODEL LISTY USERÓW
class UserList(BaseModel):
    email: str
    is_disabled: bool
    vehicle_count: int
    perm_euro: bool
    perm_ev: bool
    perm_disabled: bool

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
    pass

# === ENDPOINTS ===
@app.get("/")
def root(): return {"msg": "API OK"}

@app.get("/dashboard", response_class=HTMLResponse)
def dash():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Błąd dashboard.html"

@app.post("/api/v1/admin/auth")
def admin_login(data: AdminLogin, db: Session = Depends(get_db)):
    admin = db.query(Admin).filter(Admin.username == data.username).first()
    if not admin or not verify_password(data.password, admin.password_hash):
        raise HTTPException(401, "Błędny login lub hasło administratora")
    return {"status": "ok", "username": admin.username, "role": admin.role, "badge": admin.badge_name}

@app.post("/api/v1/auth/register")
def register(u: UserAuth, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == u.email).first(): raise HTTPException(400, "Email zajęty")
    # Domyślne uprawnienia: tylko EURO, bez EV, bez Disabled
    db.add(User(email=u.email, hashed_password=get_password_hash(u.password), perm_euro=True, perm_ev=False, perm_disabled=False))
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
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401, "Auth error")
    dm = getattr(user, 'dark_mode', False)
    
    # Zwracamy uprawnienia do frontendu (możesz to wykorzystać w App.js do ukrywania sekcji)
    # Na razie frontend używa tylko is_disabled (jako perm_disabled), ale backend jest gotowy.
    return {
        "email": user.email, 
        "is_disabled": user.is_disabled, # Legacy (dla kompatybilności)
        "perm_disabled": user.perm_disabled, # New
        "perm_ev": user.perm_ev,
        "perm_euro": user.perm_euro,
        "dark_mode": dm, 
        "vehicles": [{"name": v.name, "plate": v.license_plate} for v in user.vehicles]
    }

@app.post("/api/v1/user/status")
def status(s: StatusUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user: raise HTTPException(401, "Auth error")
    user.is_disabled = s.is_disabled # To zmienia tylko status w profilu (self-toggle)
    user.perm_disabled = s.is_disabled # Synchronizacja
    db.commit()
    return {"status": "updated"}

@app.post("/api/v1/user/darkmode")
def update_darkmode(s: DarkModeUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user: raise HTTPException(401, "Auth error")
    if hasattr(user, 'dark_mode'): user.dark_mode = s.dark_mode; db.commit()
    return {"status": "updated"}

@app.post("/api/v1/user/vehicle")
def add_veh(v: VehicleAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first()
    if not user: raise HTTPException(401, "Auth error")
    db.add(Vehicle(name=v.name, license_plate=v.license_plate, user_id=user.id)); db.commit(); return {"status": "added"}

@app.post("/api/v1/user/ticket")
def buy_ticket(t: TicketAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == t.token).first()
    if not user: raise HTTPException(401, "Auth error")
    try: end_dt = datetime.datetime.fromisoformat(t.end_time.replace('Z', '+00:00'))
    except: end_dt = now_utc() + datetime.timedelta(hours=1)
    new_ticket = Ticket(user_id=user.id, sensor_id=t.sensor_id, place_name=t.place_name, plate=t.plate, start_time=now_utc(), end_time=end_dt, price=t.price)
    db.add(new_ticket); db.commit(); return {"status": "ticket_created", "id": new_ticket.id}

@app.get("/api/v1/user/ticket/active")
def get_active_ticket(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401, "Auth error")
    ticket = db.query(Ticket).filter(Ticket.user_id == user.id, Ticket.end_time > now_utc()).order_by(Ticket.end_time.desc()).first()
    if ticket: return {"placeName": ticket.place_name, "sensorId": ticket.sensor_id, "plate": ticket.plate, "startTime": ticket.start_time.isoformat(), "endTime": ticket.end_time.isoformat(), "price": ticket.price}
    return None

@app.get("/api/v1/aktualny_stan")
def get_stat(limit: int = 100, db: Session = Depends(get_db)):
    return [m.to_dict() for m in db.query(AktualnyStan).limit(limit).all()]

@app.get("/api/v1/prognoza/wszystkie_miejsca")
def forecast(target_date: Optional[str] = None, target_hour: Optional[int] = None, db: Session = Depends(get_db)):
    return {} 

@app.post("/api/v1/statystyki/zajetosc")
def stats(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    return {"wynik_dynamiczny": {"procent_zajetosci": 0, "liczba_pomiarow": 0}} 

@app.post("/api/v1/obserwuj_miejsce")
def obs(r: ObserwujRequest, db: Session = Depends(get_db)):
    istniejacy = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == r.device_token).first()
    if istniejacy: istniejacy.sensor_id = r.sensor_id; istniejacy.czas_dodania = now_utc()
    else: db.add(ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id))
    try: db.commit(); return {"status": "ok"}
    except: db.rollback(); raise HTTPException(500, "Error")

@app.post("/api/v1/dashboard/raport")
def rep(r: RaportRequest, request: Request, db: Session = Depends(get_db)):
    return {} 

@app.post("/api/v1/debug/symulacja_sensora")
async def debug(d: dict, db: Session = Depends(get_db)): await process_parking_update(d, db); return {"status": "ok"}

# === AIRBNB ENDPOINTS ===
@app.get("/api/v1/airbnb/offers")
def get_airbnb_offers(db: Session = Depends(get_db)):
    offers = db.query(AirbnbOffer).order_by(AirbnbOffer.created_at.desc()).all()
    return [{
        "id": str(o.id), "title": o.title, "description": o.description,
        "price": o.price, "availability": o.availability, 
        "period": f"{o.start_date or 'Brak'} - {o.end_date or 'Brak'}",
        "owner": o.owner_name, "rating": o.rating,
        "latitude": o.latitude, "longitude": o.longitude,
        "district": o.district, "start_date": o.start_date, "end_date": o.end_date
    } for o in offers]

@app.post("/api/v1/airbnb/add")
def add_airbnb_offer(a: AirbnbAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == a.token).first()
    if not user: raise HTTPException(401, "Auth error")
    new_offer = AirbnbOffer(title=a.title, description=a.description, price=a.price, availability=a.availability, period="", owner_name=user.email.split('@')[0], contact=user.email, latitude=a.latitude, longitude=a.longitude, district=a.district, start_date=a.start_date, end_date=a.end_date)
    db.add(new_offer); db.commit(); return {"status": "added"}

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
    db.delete(offer); db.commit(); return {"status": "deleted"}

@app.post("/api/v1/airbnb/update")
def update_offer_details(u: AirbnbUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == u.token).first()
    if not user: raise HTTPException(401, "Auth error")
    try: oid = int(u.offer_id)
    except: raise HTTPException(400, "Złe ID oferty")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == oid).first()
    if not offer: raise HTTPException(404, "Oferta nie znaleziona")
    if offer.owner_name != user.email.split('@')[0]: raise HTTPException(403, "Brak uprawnień do edycji tej oferty")
    offer.title = u.title; offer.description = u.description; offer.price = u.price; offer.availability = u.availability; 
    if u.district: offer.district = u.district
    db.commit()
    return {"status": "updated"}

# === ADMIN USER MANAGEMENT ===
@app.get("/api/v1/admin/users")
def get_all_users(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [
        UserList(
            email=u.email,
            is_disabled=u.is_disabled,
            vehicle_count=len(u.vehicles),
            perm_euro=getattr(u, 'perm_euro', False),
            perm_ev=getattr(u, 'perm_ev', False),
            perm_disabled=getattr(u, 'perm_disabled', False)
        ) for u in users
    ]

@app.post("/api/v1/admin/toggle_user")
def toggle_user_status(u: AdminStatusUpdate, db: Session = Depends(get_db)):
    target = db.query(User).filter(User.email == u.target_email).first()
    if not target: raise HTTPException(404, "User not found")
    target.is_disabled = u.is_disabled
    db.commit()
    return {"status": "updated"}

# NOWY ENDPOINT: AKTUALIZACJA UPRAWNIEŃ
@app.post("/api/v1/admin/update_permissions")
def update_user_permissions(u: UserPermissionsUpdate, db: Session = Depends(get_db)):
    target = db.query(User).filter(User.email == u.target_email).first()
    if not target: raise HTTPException(404, "User not found")
    target.perm_euro = u.perm_euro
    target.perm_ev = u.perm_ev
    target.perm_disabled = u.perm_disabled
    
    # Synchronizacja is_disabled z perm_disabled dla starej logiki
    target.is_disabled = u.perm_disabled
    
    db.commit()
    return {"status": "updated"}

# === SYSTEM STARTUP - CREATE DEFAULT ADMINS ===
def create_default_admins(db: Session):
    if db.query(Admin).first(): return
    logger.info("Creating default admins...")
    admins = [
        {"user": "admin", "pass": "admin123", "role": "ALL", "badge": "Super Admin"},
        {"user": "euro_admin", "pass": "euro123", "role": "EURO", "badge": "Admin EURO"},
        {"user": "bud_admin", "pass": "bud123", "role": "BUD", "badge": "Admin BUD"},
    ]
    for a in admins:
        db.add(Admin(username=a["user"], password_hash=get_password_hash(a["pass"]), role=a["role"], badge_name=a["badge"]))
    db.commit()

mqtt_c = mqtt.Client()
def mqtt_loop():
    mqtt_c.on_connect = lambda c,u,f,r: c.subscribe(MQTT_TOPIC_SUBSCRIBE)
    def on_m(c,u,m):
        try:
            sid_str = SENSOR_MAP.get((m.payload[0]<<8)|m.payload[1])
            if sid_str:
                loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop);
                with SessionLocal() as db: loop.run_until_complete(process_parking_update({"sensor_id": sid_str, "status": int(m.payload[2])}, db))
        except: pass
    mqtt_c.on_message = on_m
    try: mqtt_c.connect(MQTT_BROKER, MQTT_PORT, 60); mqtt_c.loop_forever()
    except: pass
def check_stale():
    while not threading.main_thread().is_alive() is False:
        try:
            with SessionLocal() as db:
                cut = now_utc() - datetime.timedelta(minutes=5)
                old = db.query(AktualnyStan).filter(AktualnyStan.status != 2, (AktualnyStan.ostatnia_aktualizacja < cut)|(AktualnyStan.ostatnia_aktualizacja == None)).all()
                if old:
                    chg = [];
                    for s in old: s.status = 2; s.ostatnia_aktualizacja = now_utc(); chg.append(s.to_dict())
                    db.commit()
                    if chg and manager.active_connections: asyncio.run_coroutine_threadsafe(manager.broadcast(chg), asyncio.get_event_loop())
        except: pass
        time.sleep(120)
@app.on_event("startup")
async def start(): 
    threading.Thread(target=mqtt_loop, daemon=True).start(); threading.Thread(target=check_stale, daemon=True).start()
    with SessionLocal() as db: create_default_admins(db)
@app.on_event("shutdown")
def stop(): mqtt_c.disconnect()
@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws); 
    try: while True: await ws.receive_text()
    except: manager.disconnect(ws)
