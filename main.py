import os
import datetime
import logging
import secrets
import json
import threading
import time
import traceback
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, Request, WebSocket, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float, Text, Table, func, Date, DECIMAL, extract, and_, or_, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship, joinedload
from sqlalchemy.exc import IntegrityError, ProgrammingError

import bcrypt
import msgpack
import paho.mqtt.client as mqtt

# --- KONFIGURACJA LOGOWANIA ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

# --- KONFIGURACJA MQTT ---
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC = "parking/+/status" 

# --- BAZA DANYCH ---
DATABASE_URL = os.environ.get('DATABASE_URL', "postgresql://postgres:postgres@localhost:5432/postgres")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    pool_recycle=3600,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

# ==========================================
#      MODELE BAZY DANYCH
# ==========================================

class User(Base):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    phone_number = Column(String(20))
    token = Column(String(255))
    is_disabled = Column(Boolean, default=False)
    is_blocked = Column(Boolean, default=False)
    payment_token = Column(String(255))
    ticket_id = Column(Integer, ForeignKey("tickets.id"), nullable=True, default=None)
    created_at = Column(DateTime, default=now_utc)
    vehicles = relationship("Vehicle", back_populates="owner", cascade="all, delete-orphan")
    active_ticket = relationship("Ticket", foreign_keys=[ticket_id])

class Ticket(Base):
    __tablename__ = "tickets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    place_name = Column(String(100), nullable=False)
    plate_number = Column(String(20), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"))

class Vehicle(Base):
    __tablename__ = "vehicles"
    id_veh = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    plate_number = Column(String(20), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    owner = relationship("User", back_populates="vehicles")

class Admin(Base):
    __tablename__ = "admins"
    admin_id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False)
    password_hashed = Column(String(255), nullable=False)
    badge_name = Column(String(100))
    permissions = relationship("AdminPermissions", back_populates="admin", uselist=False)

class AdminPermissions(Base):
    __tablename__ = "admin_permissions"
    permission_id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(Integer, ForeignKey("admins.admin_id", ondelete="CASCADE"), nullable=False)
    city = Column(String(100))
    view_disabled = Column(Boolean, default=False)
    view_ev = Column(Boolean, default=False)
    allowed_state = Column(Text)
    admin = relationship("Admin", back_populates="permissions")

class HistoricalData(Base):
    __tablename__ = "historical_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=now_utc)
    sensor_id = Column(String(50), nullable=False)
    status = Column(Integer, nullable=False)

class HolidayData(Base):
    __tablename__ = "holiday_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=now_utc)
    sensor_id = Column(String(50), nullable=False)
    status = Column(Integer, nullable=False)
    holiday_name = Column(String(100))

class DeviceSubscription(Base):
    __tablename__ = "device_subscriptions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_token = Column(String(255), nullable=False)
    sensor_name = Column(String(100), nullable=False)
    subscribed_at = Column(DateTime, default=now_utc)

class District(Base):
    __tablename__ = "districts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    district = Column(String(100), nullable=False) 
    city = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    price_info = Column(String(255), nullable=True)
    capacity = Column(Integer, default=0)

class State(Base):
    __tablename__ = "states"
    state_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    city = Column(String(100), default='Inowrocław')

class AirbnbOffer(Base):
    __tablename__ = "airbnb_offers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(150), nullable=False)
    description = Column(Text)
    price = Column(DECIMAL(10, 2), nullable=False)
    h_availability = Column(String(100))
    owner_name = Column(String(100))
    owner_user_id = Column(Integer, ForeignKey("users.user_id"))
    contact = Column(String(150))
    rating = Column(DECIMAL(3, 2), default=0)
    latitude = Column(DECIMAL(9, 6))
    longitude = Column(DECIMAL(9, 6))
    
    state_id = Column(Integer, ForeignKey("states.state_id"), nullable=True)
    district_id = Column(Integer, ForeignKey("districts.id"), nullable=True)
    
    start_date = Column(Date)
    end_date = Column(Date)
    created_at = Column(DateTime, default=now_utc)
    
    state_rel = relationship("State") 

class ParkingSpot(Base):
    __tablename__ = "parking_spots"
    name = Column(String, primary_key=True) 
    current_status = Column(Integer, default=0) 
    last_seen = Column(DateTime(timezone=True), default=now_utc)
    city = Column(String, nullable=True)
    state_id = Column(Integer, ForeignKey("states.state_id"), nullable=True) 
    district_id = Column(Integer, ForeignKey("districts.id"), nullable=True)
    coordinates = Column(String, nullable=True)
    is_disabled_friendly = Column(Boolean, default=False)
    is_ev = Column(Boolean, default=False)
    is_paid = Column(Boolean, default=True)
    
    district_rel = relationship("District")
    state_rel = relationship("State") 

Base.metadata.create_all(bind=engine)

# ==========================================
#            SCHEMATY PYDANTIC
# ==========================================
class UserRegister(BaseModel): email: str; password: str; phone_number: Optional[str] = None
class UserLogin(BaseModel): email: str; password: str
class SubscriptionRequest(BaseModel): device_token: str; sensor_name: str
class TicketPurchase(BaseModel): token: str; place_name: str; plate_number: str; duration_hours: int
class VehicleAdd(BaseModel): token: str; name: str; plate_number: str
class StatystykiZapytanie(BaseModel): sensor_id: str; selected_date: str; selected_hour: int

class AirbnbAdd(BaseModel):
    token: str
    title: str
    description: Optional[str] = None
    price: float
    h_availability: Optional[str] = None
    contact: str
    district_id: Optional[int] = None 
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

# MODEL ODPOWIEDZI - DODANO BRAKUJĄCE POLA
class AirbnbResponse(BaseModel):
    id: int
    title: Optional[str] = "Brak tytułu"
    description: Optional[str] = None
    price: Optional[float] = 0.0
    h_availability: Optional[str] = None # DODANO
    contact: Optional[str] = None
    owner_name: Optional[str] = None # DODANO
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    state_name: Optional[str] = "Inny"
    
    class Config:
        orm_mode = True

class DistrictPayload(BaseModel):
    id: Optional[int] = None
    district: Optional[str] = None
    city: Optional[str] = None
    description: Optional[str] = None
    price_info: Optional[str] = None
    capacity: Optional[int] = None

class StatePayload(BaseModel):
    state_id: Optional[int] = None
    name: Optional[str] = None
    city: Optional[str] = None

class SpotPayload(BaseModel):
    name: str
    city: Optional[str] = None
    state_id: Optional[int] = None
    district_id: Optional[int] = None
    coordinates: Optional[str] = None
    is_disabled_friendly: Optional[bool] = None
    is_ev: Optional[bool] = None
    is_paid: Optional[bool] = None

# ==========================================
#              POMOCNIKI
# ==========================================
def get_password_hash(p: str) -> str: return bcrypt.hashpw(p.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
def verify_password(plain: str, hashed: str) -> bool:
    if not hashed: return False
    try: return bcrypt.checkpw(plain.encode('utf-8'), hashed.encode('utf-8'))
    except: return False

def send_push(token, title, body, data=None):
    try:
        import requests
        payload = {"to": token, "title": title, "body": body}
        if data: payload["data"] = data 
        requests.post("https://exp.host/--/api/v2/push/send", json=payload, timeout=2)
        logger.info(f"PUSH SENT: {token}")
    except Exception as e: logger.error(f"PUSH ERROR: {e}")

def get_easter_date(year):
    a = year % 19; b = year // 100; c = year % 100; d = b // 4; e = b % 4; f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30; i = c // 4; k = c % 4; l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451; month = (h + l - 7 * m + 114) // 31; day = ((h + l - 7 * m + 114) % 31) + 1
    return datetime.date(year, month, day)

def get_holidays_for_year(year):
    easter = get_easter_date(year)
    easter_monday = easter + datetime.timedelta(days=1)
    corpus_christi = easter + datetime.timedelta(days=60)
    holidays = {
        datetime.date(year, 1, 1): "Nowy Rok", datetime.date(year, 1, 6): "Trzech Króli", easter: "Wielkanoc", easter_monday: "Poniedziałek Wielkanocny",
        datetime.date(year, 5, 1): "Święto Pracy", datetime.date(year, 5, 3): "Święto Konstytucji 3 Maja", corpus_christi: "Boże Ciało",
        datetime.date(year, 8, 15): "Wniebowzięcie NMP", datetime.date(year, 11, 1): "Wszystkich Świętych", datetime.date(year, 11, 11): "Święto Niepodległości",
        datetime.date(year, 12, 24): "Wigilia", datetime.date(year, 12, 25): "Boże Narodzenie (1)", datetime.date(year, 12, 26): "Boże Narodzenie (2)"
    }
    return holidays

def check_if_holiday(target_date):
    holidays = get_holidays_for_year(target_date.year)
    return holidays.get(target_date)

# ==========================================
#              LOGIKA MQTT
# ==========================================
class ConnectionManager:
    def __init__(self): self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket): await websocket.accept(); self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket): 
        if websocket in self.active_connections: self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        try:
            msg_bin = msgpack.packb(message)
            for conn in list(self.active_connections):
                try: await conn.send_bytes(msg_bin)
                except: self.disconnect(conn)
        except: pass

manager = ConnectionManager()

def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        sensor_name = payload.get("name") or payload.get("sensor_id")
        status = int(payload.get("status", 0))
        if not sensor_name: return
        with SessionLocal() as db:
            spot = db.query(ParkingSpot).filter(ParkingSpot.name == sensor_name).first()
            if spot:
                prev = spot.current_status; spot.current_status = status; spot.last_seen = now_utc()
                if prev != status:
                    today_holiday = check_if_holiday(datetime.date.today())
                    if today_holiday:
                        db.add(HolidayData(sensor_id=sensor_name, status=status, holiday_name=today_holiday))
                    else:
                        db.add(HistoricalData(sensor_id=sensor_name, status=status))
                    if status == 1:
                        subs = db.query(DeviceSubscription).filter(DeviceSubscription.sensor_name == sensor_name).all()
                        for sub in subs:
                            send_push(sub.device_token, "⚠️ Ktoś zajął Twoje miejsce!", f"Miejsce {sensor_name} zajęte. Kliknij, aby znaleźć alternatywę.", data={"action": "find_alt"})
                            db.delete(sub)
                db.commit()
    except Exception as e: logger.error(f"MQTT Error: {e}")

def start_mqtt():
    try: client = mqtt.Client(); client.on_message = on_mqtt_message; client.connect(MQTT_BROKER, MQTT_PORT, 60); client.subscribe(MQTT_TOPIC); client.loop_start(); logger.info("MQTT Client Started")
    except Exception as e: logger.warning(f"MQTT Failed to start: {e}")

# ==========================================
#                APLIKACJA
# ==========================================
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event(): start_mqtt()

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try: 
        while True: await ws.receive_text()
    except: manager.disconnect(ws)

# --- ENDPOINTY ---
@app.post("/api/v1/auth/register")
def register(u: UserRegister, db: Session = Depends(get_db)):
    try:
        if db.query(User).filter(User.email == u.email).first(): raise HTTPException(400, "Email zajęty")
        new_user = User(email=u.email, password_hash=get_password_hash(u.password), phone_number=u.phone_number)
        db.add(new_user); db.commit(); return {"status": "registered", "user_id": new_user.user_id}
    except Exception as e: db.rollback(); logger.error(f"Register: {e}"); raise HTTPException(500, str(e))

@app.post("/api/v1/auth/login")
def login(u: UserLogin, db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.email == u.email).first()
        if not user: raise HTTPException(401, "Błędne dane")
        if not verify_password(u.password, user.password_hash): raise HTTPException(401, "Błędne dane")
        token = secrets.token_hex(16); user.token = token; db.commit()
        return {"token": token, "email": user.email, "user_id": user.user_id, "is_disabled": user.is_disabled}
    except Exception as e: db.rollback(); logger.error(f"Login: {e}"); raise HTTPException(500, str(e))

@app.get("/api/v1/aktualny_stan")
def get_spots(db: Session = Depends(get_db)):
    spots = db.query(ParkingSpot).all()
    res = []
    for s in spots:
        dist_obj = s.district_rel
        res.append({
            "sensor_id": s.name, "status": s.current_status, "city": s.city, 
            "state": s.state_rel.name if s.state_rel else "Brak danych", 
            "place_name": dist_obj.district if dist_obj else "Parking Ogólny", 
            "place_description": dist_obj.description if dist_obj else "",
            "place_price": dist_obj.price_info if dist_obj else "",
            "place_capacity": dist_obj.capacity if dist_obj else 0,
            "district_id": s.district_id, "state_id": s.state_id,
            "wspolrzedne": {"latitude": float(s.coordinates.split(',')[0]), "longitude": float(s.coordinates.split(',')[1])} if s.coordinates else None, 
            "is_disabled_friendly": s.is_disabled_friendly, "is_ev": s.is_ev, "is_paid": s.is_paid
        })
    return res

@app.get("/api/v1/states")
def get_all_states(db: Session = Depends(get_db)): return db.query(State).all()

@app.post("/api/v1/subscribe_spot")
async def subscribe_device(request: SubscriptionRequest, db: Session = Depends(get_db)):
    db.query(DeviceSubscription).filter(DeviceSubscription.device_token == request.device_token, DeviceSubscription.sensor_name == request.sensor_name).delete()
    db.add(DeviceSubscription(device_token=request.device_token, sensor_name=request.sensor_name)); db.commit(); return {"status": "success"}

@app.get("/api/v1/user/where_is_my_car")
def where_is_my_car(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401)
    if not user.ticket_id: return {"status": "no_ticket"}
    t = db.query(Ticket).filter(Ticket.id == user.ticket_id).first()
    return {"status": "found", "place_name": t.place_name, "end_time": t.end_time, "plate_number": t.plate_number} if t else {"status": "error"}

@app.post("/api/v1/user/buy_ticket")
def buy_ticket(req: TicketPurchase, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == req.token).first()
    if not user: raise HTTPException(401)
    start = now_utc(); end = start + datetime.timedelta(hours=req.duration_hours)
    nt = Ticket(place_name=req.place_name, plate_number=req.plate_number, start_time=start, end_time=end, price=5.0*req.duration_hours, user_id=user.user_id)
    db.add(nt); db.flush(); user.ticket_id = nt.id
    spot = db.query(ParkingSpot).filter(ParkingSpot.name == req.place_name).first()
    if spot: spot.current_status = 1; db.add(HistoricalData(sensor_id=spot.name, status=1))
    db.commit(); return {"status": "ok", "ticket_id": nt.id}

@app.post("/api/v1/airbnb/add")
def add_airbnb(a: AirbnbAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == a.token).first()
    if not u: raise HTTPException(401, "Nieautoryzowany")
    try:
        db.add(AirbnbOffer(title=a.title, description=a.description, price=a.price, h_availability=a.h_availability, owner_name=u.email, owner_user_id=u.user_id, contact=a.contact, state_id=a.district_id, start_date=datetime.datetime.strptime(a.start_date, "%Y-%m-%d").date() if a.start_date else None, end_date=datetime.datetime.strptime(a.end_date, "%Y-%m-%d").date() if a.end_date else None, latitude=a.latitude, longitude=a.longitude))
        db.commit(); return {"status": "ok"}
    except Exception as e: db.rollback(); logger.error(f"Airbnb Add: {e}"); raise HTTPException(500, f"DB Error: {str(e)}")

# LISTA OFERT Z PEŁNYMI DANYMI
@app.get("/api/v1/airbnb/offers", response_model=List[AirbnbResponse])
def get_airbnb(district_id: Optional[int] = None, db: Session = Depends(get_db)):
    try:
        q = db.query(AirbnbOffer)
        if district_id: q = q.filter(AirbnbOffer.state_id == district_id)
        offers = q.all()
        
        res = []
        for o in offers:
            try:
                s_name = "Inny"
                if o.state_rel: s_name = o.state_rel.name
                
                res.append({
                    "id": o.id, "title": o.title or "Brak tytułu", "description": o.description,
                    "price": float(o.price) if o.price is not None else 0.0,
                    # MAPUJEMY NOWE POLA DO MODELU
                    "h_availability": o.h_availability or "Brak danych",
                    "owner_name": o.owner_name or "Nieznany",
                    "contact": o.contact,
                    "latitude": float(o.latitude) if o.latitude is not None else 0.0,
                    "longitude": float(o.longitude) if o.longitude is not None else 0.0,
                    "state_name": s_name
                })
            except Exception as e:
                continue
        return res
    except Exception as e:
        logger.error(f"Global Airbnb Error: {e}")
        return []

@app.get("/api/v1/debug/airbnb")
def debug_airbnb(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("SELECT * FROM airbnb_offers LIMIT 20")).fetchall()
        return {"count": len(result), "rows": [dict(row._mapping) for row in result]}
    except Exception as e: return {"error": str(e)}

@app.post("/api/v1/statystyki/zajetosc")
def stats_mobile(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    try: target_date = datetime.datetime.strptime(z.selected_date, "%Y-%m-%d").date()
    except: raise HTTPException(400)
    start_h = max(0, z.selected_hour - 1); end_h = min(23, z.selected_hour + 1)
    target_sensors = [z.sensor_id]; target_spot = db.query(ParkingSpot).filter(ParkingSpot.name == z.sensor_id).first()
    if target_spot:
        if target_spot.is_ev or target_spot.is_disabled_friendly: target_sensors = [z.sensor_id]
        elif target_spot.district_id:
            district_peers = db.query(ParkingSpot.name).filter(ParkingSpot.district_id == target_spot.district_id, ParkingSpot.is_ev == False, ParkingSpot.is_disabled_friendly == False).all()
            if district_peers: target_sensors = [p.name for p in district_peers]
    holiday_name = check_if_holiday(target_date)
    if holiday_name:
        query = db.query(HolidayData).filter(HolidayData.sensor_id.in_(target_sensors), HolidayData.holiday_name == holiday_name, extract('hour', HolidayData.timestamp) >= start_h, extract('hour', HolidayData.timestamp) <= end_h)
        if query.count() == 0: query = db.query(HistoricalData).filter(HistoricalData.sensor_id.in_(target_sensors), extract('month', HistoricalData.timestamp) == target_date.month, extract('day', HistoricalData.timestamp) == target_date.day, extract('hour', HistoricalData.timestamp) >= start_h, extract('hour', HistoricalData.timestamp) <= end_h)
    else:
        query = db.query(HistoricalData).filter(HistoricalData.sensor_id.in_(target_sensors), extract('hour', HistoricalData.timestamp) >= start_h, extract('hour', HistoricalData.timestamp) <= end_h)
        weekday = target_date.weekday()
        if 0 <= weekday <= 3: query = query.filter(extract('dow', HistoricalData.timestamp).in_([1, 2, 3, 4])) 
        elif weekday == 4: query = query.filter(extract('dow', HistoricalData.timestamp) == 5)
        elif weekday == 5: query = query.filter(extract('dow', HistoricalData.timestamp) == 6)
        elif weekday == 6: query = query.filter(extract('dow', HistoricalData.timestamp) == 0)
    raw_data = query.all()
    valid = raw_data if holiday_name else [r for r in raw_data if not check_if_holiday(r.timestamp.date())]
    return {"wynik": {"procent_zajetosci": int((sum(1 for x in valid if x.status==1)/len(valid))*100) if valid else 0, "liczba_pomiarow": len(valid), "typ_dnia": holiday_name or "Standardowy"}}

@app.post("/api/v1/admin/manage/district")
async def manage_district(d: DistrictPayload, db: Session = Depends(get_db)):
    try:
        if d.id:
            ex = db.query(District).filter(District.id == d.id).first()
            if ex: 
                if d.district: ex.district = d.district
                if d.city: ex.city = d.city
                if d.description: ex.description = d.description
                if d.price_info: ex.price_info = d.price_info
                if d.capacity is not None: ex.capacity = d.capacity
                db.commit(); return {"status": "updated"}
        else:
            if not d.district: raise HTTPException(400)
            db.add(District(district=d.district, city=d.city, description=d.description, price_info=d.price_info, capacity=d.capacity or 0)); db.commit(); return {"status": "created"}
    except Exception as e: db.rollback(); raise HTTPException(500, str(e))

@app.post("/api/v1/admin/manage/state")
async def manage_state(s: StatePayload, db: Session = Depends(get_db)):
    try:
        if s.state_id:
            ex = db.query(State).filter(State.state_id == s.state_id).first()
            if ex: 
                if s.name: ex.name = s.name
                if s.city: ex.city = s.city
                db.commit(); return {"status": "updated"}
        else:
            if not s.name: raise HTTPException(400)
            db.add(State(name=s.name, city=s.city)); db.commit(); return {"status": "created"}
    except Exception as e: db.rollback(); raise HTTPException(500, str(e))

@app.post("/api/v1/admin/manage/spot")
async def manage_spot(s: SpotPayload, db: Session = Depends(get_db)):
    try:
        spot = db.query(ParkingSpot).filter(ParkingSpot.name == s.name).first()
        if not spot: spot = ParkingSpot(name=s.name); db.add(spot)
        if s.city: spot.city = s.city
        if s.state_id: spot.state_id = s.state_id
        if s.district_id: spot.district_id = s.district_id
        if s.coordinates: spot.coordinates = s.coordinates
        if s.is_disabled_friendly is not None: spot.is_disabled_friendly = s.is_disabled_friendly
        if s.is_ev is not None: spot.is_ev = s.is_ev
        if s.is_paid is not None: spot.is_paid = s.is_paid
        db.commit(); return {"status": "updated"}
    except Exception as e: db.rollback(); raise HTTPException(500, str(e))

@app.post("/api/v1/iot/update")
async def iot_update_http(data: dict, db: Session = Depends(get_db)):
    try:
        name = data.get("name"); stat = int(data.get("status", 0))
        spot = db.query(ParkingSpot).filter(ParkingSpot.name == name).first()
        if not spot: spot = ParkingSpot(name=name, current_status=stat); db.add(spot)
        if data.get("district_id"): spot.district_id = int(data.get("district_id"))
        if data.get("state_id"): spot.state_id = int(data.get("state_id"))
        prev = spot.current_status; spot.current_status = stat; spot.last_seen = now_utc()
        if prev != stat:
            today = check_if_holiday(datetime.date.today())
            if today: db.add(HolidayData(sensor_id=name, status=stat, holiday_name=today))
            else: db.add(HistoricalData(sensor_id=name, status=stat))
            if stat == 1:
                subs = db.query(DeviceSubscription).filter(DeviceSubscription.sensor_name == name).all()
                for s in subs: send_push(s.device_token, "Zajęto miejsce!", f"{name} zajęte", data={"action": "find_alt"}); db.delete(s)
        db.commit(); await manager.broadcast({"sensor_id": name, "status": stat}); return {"status": "updated"}
    except Exception as e: db.rollback(); raise HTTPException(500, str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
