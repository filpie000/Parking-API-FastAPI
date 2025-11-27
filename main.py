import os
import datetime
import logging
import secrets
import json
import random
import traceback
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, Request, WebSocket, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float, Text, Table, func, Date, DECIMAL, extract, and_, or_, text, distinct
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
# DATABASE_URL = "sqlite:///./parking.db"

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
    password_hash = Column(String(255), nullable=False)
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
class TicketPurchase(BaseModel): token: str; place_name: str; plate_number: str; duration_hours: float; total_price: Optional[float] = None
class VehicleAdd(BaseModel): token: str; name: str; plate_number: str
class VehicleDelete(BaseModel): token: str; vehicle_id: int

class StatsRequest(BaseModel):
    start_date: str 
    end_date: str
    districts: List[int]
    include_weekends: bool = True
    include_holidays: bool = True
    only_disabled: bool = False
    only_paid: bool = False
    only_ev: bool = False

class AirbnbAdd(BaseModel):
    token: str; title: str; description: Optional[str] = None; price: float; h_availability: Optional[str] = None
    contact: str; district_id: Optional[int] = None; start_date: Optional[str] = None; end_date: Optional[str] = None
    latitude: Optional[float] = None; longitude: Optional[float] = None

class AirbnbUpdate(BaseModel):
    token: str; offer_id: int; title: Optional[str] = None; description: Optional[str] = None
    price: Optional[float] = None; h_availability: Optional[str] = None; contact: Optional[str] = None
    district_id: Optional[int] = None; start_date: Optional[str] = None; end_date: Optional[str] = None
    latitude: Optional[float] = None; longitude: Optional[float] = None

class AirbnbDelete(BaseModel): token: str; offer_id: int

class AirbnbResponse(BaseModel):
    id: int; title: Optional[str] = "Brak tytułu"; description: Optional[str] = None; price: Optional[float] = 0.0
    h_availability: Optional[str] = None; contact: Optional[str] = None; owner_name: Optional[str] = None
    owner_user_id: Optional[int] = None; latitude: Optional[float] = None; longitude: Optional[float] = None
    state_name: Optional[str] = "Inny"; start_date: Optional[datetime.date] = None; end_date: Optional[datetime.date] = None
    class Config: orm_mode = True

class AdminUserUpdate(BaseModel): user_id: int; is_blocked: Optional[bool] = None; is_disabled: Optional[bool] = None

class AdminPayload(BaseModel):
    id: Optional[int] = None; username: str; password: Optional[str] = None; city: str = "ALL"
    view_disabled: bool = False; view_ev: bool = False; allowed_state: str = ""

class AdminLogin(BaseModel): username: str; password: str

class DistrictPayload(BaseModel):
    id: Optional[int] = None; district: Optional[str] = None; city: Optional[str] = None
    description: Optional[str] = None; price_info: Optional[str] = None; capacity: Optional[int] = None

class StatePayload(BaseModel): state_id: Optional[int] = None; name: Optional[str] = None; city: Optional[str] = None

class SpotPayload(BaseModel):
    name: str; city: Optional[str] = None; state_id: Optional[int] = None; district_id: Optional[int] = None
    coordinates: Optional[str] = None; is_disabled_friendly: Optional[bool] = None
    is_ev: Optional[bool] = None; is_paid: Optional[bool] = None

# ==========================================
#              POMOCNIKI
# ==========================================
def get_password_hash(p: str) -> str: return bcrypt.hashpw(p.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
def verify_password(plain: str, hashed: str) -> bool:
    if not hashed: return False
    try: return bcrypt.checkpw(plain.encode('utf-8'), hashed.encode('utf-8'))
    except: return plain == hashed

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
        event_time_str = payload.get("timestamp")
        event_time = datetime.datetime.fromisoformat(event_time_str.replace("Z", "+00:00")) if event_time_str else now_utc()

        if not sensor_name: return
        with SessionLocal() as db:
            spot = db.query(ParkingSpot).filter(ParkingSpot.name == sensor_name).first()
            if spot:
                prev = spot.current_status
                spot.current_status = status
                spot.last_seen = event_time 
                
                if prev != status:
                    today_holiday = check_if_holiday(event_time.date())
                    if today_holiday:
                        db.add(HolidayData(sensor_id=sensor_name, status=status, holiday_name=today_holiday, timestamp=event_time))
                    else:
                        db.add(HistoricalData(sensor_id=sensor_name, status=status, timestamp=event_time))
                    
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
async def startup_event():
    start_mqtt()
    try:
        with SessionLocal() as db:
            admin = db.query(Admin).filter(Admin.username == "admin").first()
            default_pass_hash = get_password_hash("admin123")
            if admin:
                admin.password_hash = default_pass_hash
                db.commit()
            else:
                new_admin = Admin(username="admin", password_hash=default_pass_hash, badge_name="Super Admin")
                db.add(new_admin)
                db.commit()
    except Exception as e:
        logger.error(f"STARTUP ERROR: {e}")

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try: 
        while True: await ws.receive_text()
    except: manager.disconnect(ws)

# --- ADMIN / DASHBOARD ---
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Brak pliku dashboard.html"

@app.post("/api/v1/admin/auth")
def admin_login(d: AdminLogin, db: Session = Depends(get_db)):
    if d.username == "admin" and d.password == "admin123":
        return {
            "status": "ok", "username": "admin", "is_superadmin": True,
            "permissions": { "city": "ALL", "view_disabled": True, "view_ev": True, "allowed_state": "" }
        }
    try:
        admin = db.query(Admin).filter(Admin.username == d.username).first()
        if admin and verify_password(d.password, admin.password_hash):
             perms = admin.permissions 
             return {
                "status": "ok", "username": admin.username, "is_superadmin": (admin.username == 'admin'),
                "permissions": {
                    "city": perms.city if perms else "ALL",
                    "view_disabled": perms.view_disabled if perms else False,
                    "view_ev": perms.view_ev if perms else False,
                    "allowed_state": perms.allowed_state if perms else ""
                }
            }
    except Exception as e: pass
    raise HTTPException(401, "Błędne dane")

@app.get("/api/v1/admin/list")
def list_admins(db: Session = Depends(get_db)):
    admins = db.query(Admin).options(joinedload(Admin.permissions)).all()
    res = []
    for a in admins:
        res.append({
            "id": a.admin_id, "username": a.username,
            "permissions": {
                "city": a.permissions.city if a.permissions else "ALL",
                "view_disabled": a.permissions.view_disabled if a.permissions else False,
                "view_ev": a.permissions.view_ev if a.permissions else False,
                "allowed_state": a.permissions.allowed_state if a.permissions else ""
            }
        })
    return res

@app.post("/api/v1/admin/create")
def create_admin(d: AdminPayload, db: Session = Depends(get_db)):
    if db.query(Admin).filter(Admin.username == d.username).first(): raise HTTPException(400, "Nazwa zajęta")
    new_admin = Admin(username=d.username, password_hash=get_password_hash(d.password or "admin123"))
    db.add(new_admin); db.flush()
    perms = AdminPermissions(admin_id=new_admin.admin_id, city=d.city, view_disabled=d.view_disabled, view_ev=d.view_ev, allowed_state=d.allowed_state)
    db.add(perms); db.commit(); return {"status": "created"}

@app.post("/api/v1/admin/update")
def update_admin(d: AdminPayload, db: Session = Depends(get_db)):
    if not d.id: raise HTTPException(400, "ID wymagane")
    admin = db.query(Admin).filter(Admin.admin_id == d.id).first()
    if not admin: raise HTTPException(404)
    if d.password: admin.password_hash = get_password_hash(d.password)
    if admin.permissions:
        admin.permissions.city = d.city
        admin.permissions.view_disabled = d.view_disabled
        admin.permissions.view_ev = d.view_ev
        admin.permissions.allowed_state = d.allowed_state
    else:
        db.add(AdminPermissions(admin_id=admin.admin_id, city=d.city, view_disabled=d.view_disabled, view_ev=d.view_ev, allowed_state=d.allowed_state))
    db.commit(); return {"status": "updated"}

@app.post("/api/v1/admin/delete")
def delete_admin(data: dict, db: Session = Depends(get_db)):
    target_id = data.get("target_id")
    admin = db.query(Admin).filter(Admin.admin_id == target_id).first()
    if not admin: raise HTTPException(404)
    if admin.username == "admin": raise HTTPException(400, "Nie można usunąć Superadmina")
    db.delete(admin); db.commit(); return {"status": "deleted"}

@app.get("/api/v1/admin/users")
def list_users(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [{
        "user_id": u.user_id, "email": u.email, "phone": u.phone_number,
        "is_disabled": u.is_disabled, "is_blocked": u.is_blocked,
        "vehicle_count": len(u.vehicles)
    } for u in users]

@app.post("/api/v1/admin/user/update")
def update_user_status(d: AdminUserUpdate, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.user_id == d.user_id).first()
    if not u: raise HTTPException(404, "User not found")
    if d.is_blocked is not None: u.is_blocked = d.is_blocked
    if d.is_disabled is not None: u.is_disabled = d.is_disabled
    db.commit(); return {"status": "updated"}

# --- NOWE ENDPOINTY DO WYKRESÓW ---

@app.get("/api/v1/districts")
def get_districts(db: Session = Depends(get_db)):
    # Zwraca listę dzielnic dla dropdownów
    districts = db.query(District).all()
    return [{"id": d.id, "name": d.district, "city": d.city} for d in districts]

@app.get("/api/v1/admin/search_hint")
def search_admin_hint(q: str = "", db: Session = Depends(get_db)):
    if not q: return []
    admins = db.query(Admin.username).filter(Admin.username.ilike(f"%{q}%")).limit(5).all()
    return [a[0] for a in admins]

@app.post("/api/v1/admin/stats/advanced")
def get_advanced_stats(req: StatsRequest, db: Session = Depends(get_db)):
    start = datetime.datetime.strptime(req.start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(req.end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
    
    response_datasets = []
    
    for dist_id in req.districts:
        district = db.query(District).filter(District.id == dist_id).first()
        if not district: continue
        
        # 1. Filtrowanie Miejsc (Hardware)
        spots_query = db.query(ParkingSpot).filter(ParkingSpot.district_id == dist_id)
        if req.only_disabled: spots_query = spots_query.filter(ParkingSpot.is_disabled_friendly == True)
        if req.only_ev: spots_query = spots_query.filter(ParkingSpot.is_ev == True)
        if req.only_paid: spots_query = spots_query.filter(ParkingSpot.is_paid == True)
        
        valid_spots_names = [s.name for s in spots_query.all()]
        if not valid_spots_names: continue

        # Pojemność (dla obliczenia %)
        capacity = len(valid_spots_names)
        if capacity == 0: capacity = 1

        hourly_counts = {h: 0 for h in range(24)}
        hourly_samples = {h: 0 for h in range(24)} # Ile dni wzięliśmy pod uwagę dla tej godziny

        # 2. Pobieramy dane historyczne
        history_data = db.query(HistoricalData).filter(
            HistoricalData.sensor_id.in_(valid_spots_names),
            HistoricalData.status == 1,
            HistoricalData.timestamp >= start,
            HistoricalData.timestamp < end
        ).all()

        # 3. Analiza w Pythonie (żeby obsłużyć Weekend/Święto)
        for entry in history_data:
            ts = entry.timestamp
            
            # Filtr Weekendy (Sob=5, Nd=6)
            if not req.include_weekends and ts.weekday() >= 5: continue
            
            # Filtr Święta
            is_holiday = check_if_holiday(ts.date())
            if not req.include_holidays and is_holiday: continue

            # Zliczamy
            h = ts.hour
            hourly_counts[h] += 1
        
        # Normalizacja danych (Średnia)
        # Przyjmujemy uproszczenie: Liczba zajęć / (Pojemność * Liczba Dni)
        # Oblicz ile dni w zakresie spełnia kryteria
        valid_days = 0
        current = start
        while current < end:
            is_w = current.weekday() >= 5
            is_h = check_if_holiday(current.date())
            
            skip = False
            if not req.include_weekends and is_w: skip = True
            if not req.include_holidays and is_h: skip = True
            
            if not skip: valid_days += 1
            current += datetime.timedelta(days=1)
        
        if valid_days == 0: valid_days = 1

        final_data = []
        for h in range(24):
            # Obliczamy % zajętości
            # (Suma zajęć w danej godzinie przez wszystkie dni) / (Pojemność * Liczba dni)
            # * 100
            val = (hourly_counts[h] / (capacity * valid_days)) * 100
            # Skalowanie, żeby wykres był ładny (symulacja czasu trwania)
            # Zakładamy, że auto stoi średnio 1h, więc 1 wjazd = 1h zajętości
            if val > 100: val = 100
            final_data.append(round(val, 1))

        # Kolor
        color = f"rgba({random.randint(50,220)}, {random.randint(50,220)}, {random.randint(50,220)}, 1)"
        
        response_datasets.append({
            "label": f"{district.district}",
            "data": final_data,
            "borderColor": color,
            "backgroundColor": color.replace(", 1)", ", 0.1)"),
            "fill": True,
            "tension": 0.4
        })
        
    return {"datasets": response_datasets}

# ... (Reszta endpointów Auth i IoT - bez zmian) ...
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
        return {"token": token, "email": user.email, "user_id": user.user_id, "is_disabled": user.is_disabled, "dark_mode": False}
    except Exception as e: db.rollback(); logger.error(f"Login: {e}"); raise HTTPException(500, str(e))

@app.get("/api/v1/aktualny_stan")
def get_spots(limit: int = 1000, db: Session = Depends(get_db)):
    spots = db.query(ParkingSpot).limit(limit).all()
    res = []
    for s in spots:
        dist_obj = s.district_rel
        res.append({
            "sensor_id": s.name, "status": s.current_status, "city": s.city, 
            "state": s.state_rel.name if s.state_rel else "Brak danych", 
            "place_name": dist_obj.district if dist_obj else "Parking Ogólny", 
            "district_id": s.district_id, "state_id": s.state_id,
            "wspolrzedne": {"latitude": float(s.coordinates.split(',')[0]), "longitude": float(s.coordinates.split(',')[1])} if s.coordinates else None, 
            "is_disabled_friendly": s.is_disabled_friendly, "is_ev": s.is_ev, "is_paid": s.is_paid
        })
    return res

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
