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

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float, Text, Table, func, Date, DECIMAL, extract, and_, or_, text, distinct, case
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship, joinedload
from sqlalchemy.exc import IntegrityError, ProgrammingError

import bcrypt
import msgpack
import paho.mqtt.client as mqtt

# --- KONFIGURACJA ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC = "parking/+/status" 

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

# --- MODELE ---
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
    city = Column(String(255))
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

# --- SCHEMATY ---
class UserRegister(BaseModel): email: str; password: str; phone_number: Optional[str] = None
class UserLogin(BaseModel): email: str; password: str
class SubscriptionRequest(BaseModel): device_token: str; sensor_name: str
class TicketPurchase(BaseModel): token: str; place_name: str; plate_number: str; duration_hours: float; total_price: Optional[float] = None
class VehicleAdd(BaseModel): token: str; name: str; plate_number: str
class VehicleDelete(BaseModel): token: str; vehicle_id: int

class StatsRequest(BaseModel):
    start_date: str; end_date: str; districts: List[int]
    include_weekends: bool = True; include_holidays: bool = True
    only_disabled: bool = False; only_paid: bool = False; only_ev: bool = False

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
    is_mine: bool = False 
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

class StatystykiZapytanie(BaseModel): 
    sensor_id: str
    selected_date: str
    selected_hour: int

# --- POMOCNIKI ---
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
    return holidays.get(target_date) # Zwraca nazwę święta lub None

# --- MQTT ---
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

# --- APP ---
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

# --- ADMIN ENDPOINTS ---
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Brak pliku dashboard.html"

@app.post("/api/v1/admin/auth")
def admin_login(d: AdminLogin, db: Session = Depends(get_db)):
    if d.username == "admin" and d.password == "admin123":
        return { "status": "ok", "username": "admin", "is_superadmin": True, "permissions": { "city": "ALL", "view_disabled": True, "view_ev": True, "allowed_state": "" } }
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

@app.get("/api/v1/districts")
def get_districts(db: Session = Depends(get_db)):
    districts = db.query(District).all()
    return [{"id": d.id, "name": d.district, "city": d.city} for d in districts]

@app.get("/api/v1/admin/search_hint")
def search_admin_hint(q: str = "", db: Session = Depends(get_db)):
    if not q: return []
    admins = db.query(Admin.username).filter(Admin.username.ilike(f"%{q}%")).limit(5).all()
    return [a[0] for a in admins]

# --- NOWE STATYSTYKI ADMINA (POPRAWIONE SQL) ---
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

        # 2. SQL Agregacja (Działa szybko dla 400k)
        # Zwraca: (godzina, średnia_zajetosc_jako_float)
        # avg(status) działa, bo status to 0 lub 1
        raw_stats = db.query(
            extract('hour', HistoricalData.timestamp).label('hour'),
            func.avg(HistoricalData.status).label('avg_status')
        ).filter(
            HistoricalData.sensor_id.in_(valid_spots_names),
            HistoricalData.timestamp >= start,
            HistoricalData.timestamp < end
        ).group_by('hour').all()

        # Konwersja do mapy {0: 0.1, 1: 0.2 ...}
        hourly_map = {int(r.hour): float(r.avg_status) * 100 for r in raw_stats}
        
        final_data = []
        for h in range(24):
            val = hourly_map.get(h, 0)
            final_data.append(round(val, 1))

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

# ==========================================
#           MOBILE APP ENDPOINTS
# ==========================================

@app.post("/api/v1/auth/register")
def register(u: UserRegister, db: Session = Depends(get_db)):
    try:
        if db.query(User).filter(User.email == u.email).first(): raise HTTPException(400, "Email zajęty")
        new_user = User(email=u.email, password_hash=get_password_hash(u.password), phone_number=u.phone_number)
        db.add(new_user); db.commit(); return {"status": "registered", "user_id": new_user.user_id}
    except Exception as e: db.rollback(); raise HTTPException(500, str(e))

@app.post("/api/v1/auth/login")
def login(u: UserLogin, db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.email == u.email).first()
        if not user: raise HTTPException(401, "Błędne dane")
        if not verify_password(u.password, user.password_hash): raise HTTPException(401, "Błędne dane")
        token = secrets.token_hex(16); user.token = token; db.commit()
        return {"token": token, "email": user.email, "user_id": user.user_id, "is_disabled": user.is_disabled, "dark_mode": False}
    except Exception as e: db.rollback(); raise HTTPException(500, str(e))

@app.get("/api/v1/user/me")
def get_me(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401, "Token error")
    return {"user_id": user.user_id, "email": user.email, "is_disabled": user.is_disabled, "phone": user.phone_number, "vehicles": []}

@app.get("/api/v1/aktualny_stan")
def get_spots(limit: int = 1000, db: Session = Depends(get_db)):
    spots = db.query(ParkingSpot).limit(limit).all()
    res = []
    for s in spots:
        coords = None
        if s.coordinates and ',' in s.coordinates:
            try:
                p = s.coordinates.split(',')
                coords = {"latitude": float(p[0]), "longitude": float(p[1])}
            except: coords = None
        
        dist_obj = s.district_rel
        place_name = dist_obj.district if dist_obj else "Parking Ogólny"
        place_desc = dist_obj.description if dist_obj else ""
        place_price = dist_obj.price_info if dist_obj else ""
        place_cap = dist_obj.capacity if dist_obj else 0
        
        res.append({
            "sensor_id": s.name, 
            "status": s.current_status, 
            "city": s.city, 
            "district_id": s.district_id, 
            "state_id": s.state_id,
            "coordinates": s.coordinates, 
            "wspolrzedne": coords,
            "is_disabled_friendly": s.is_disabled_friendly,
            "is_ev": s.is_ev,
            "is_paid": s.is_paid,
            "place_name": place_name,
            "place_description": place_desc,
            "place_price": place_price,
            "place_capacity": place_cap
        })
    return res

# --- WAŻNE: STATYSTYKI PREDYKCYJNE (ZAAWANSOWANA LOGIKA) ---
@app.post("/api/v1/statystyki/zajetosc")
def get_stats_mobile(req: StatystykiZapytanie, db: Session = Depends(get_db)):
    try:
        target_date = datetime.datetime.strptime(req.selected_date, "%Y-%m-%d").date()
        target_hour = req.selected_hour
        target_weekday = target_date.weekday() # 0=Pon, 4=Pt, 5=Sob, 6=Nd
        
        # 1. Określ TYP DNIA
        target_holiday_name = check_if_holiday(target_date)
        is_target_weekend = target_weekday >= 4 # Piątek, Sobota, Niedziela (wg opisu: weekend to Pt, Sob, Nd)
        
        # 2. Znajdź District ID
        spot = db.query(ParkingSpot).filter(ParkingSpot.name == req.sensor_id).first()
        if not spot or not spot.district_id:
            valid_sensors = [req.sensor_id]
        else:
            all_spots = db.query(ParkingSpot.name).filter(ParkingSpot.district_id == spot.district_id).all()
            valid_sensors = [s[0] for s in all_spots]

        # 3. Okno czasowe X-1 do X+1
        hour_min = target_hour - 1
        hour_max = target_hour + 1
        
        # 4. Generuj listę DAT HISTORYCZNYCH do porównania (Ostatnie 2 lata)
        dates_to_check = []
        lookback_start = target_date - datetime.timedelta(days=730)
        
        # Pobieramy wszystkie możliwe daty z historii, które są w tym samym typie dnia
        # Żeby nie mielić w Pythonie, zapytamy bazę o UNIKALNE DATY z historii
        available_dates = db.query(distinct(func.date(HistoricalData.timestamp))).filter(
            HistoricalData.timestamp >= lookback_start,
            HistoricalData.timestamp < target_date
        ).all()
        
        for d_row in available_dates:
            past_date = d_row[0]
            past_weekday = past_date.weekday()
            past_holiday_name = check_if_holiday(past_date)
            
            match = False
            
            # SCENARIUSZ 1: ŚWIĘTA
            if target_holiday_name:
                if past_holiday_name == target_holiday_name:
                    match = True # Np. Wigilia == Wigilia
            
            # SCENARIUSZ 2: WEEKEND (Pt, Sob, Nd - każdy osobno)
            elif is_target_weekend:
                if not past_holiday_name and past_weekday == target_weekday:
                    match = True # Np. Piątek == Piątek (i nie święto)
            
            # SCENARIUSZ 3: DNI POWSZEDNIE (Pn-Czw razem)
            else:
                if not past_holiday_name and past_weekday in [0, 1, 2, 3]:
                    match = True # Pon, Wt, Śr lub Czw
            
            if match:
                dates_to_check.append(past_date)

        if not dates_to_check:
            return {"wynik": {"procent_zajetosci": 0, "liczba_pomiarow": 0}}

        # 5. Zapytanie SQL (Agregacja dla wybranych dat i godzin)
        result = db.query(func.avg(HistoricalData.status)).filter(
            HistoricalData.sensor_id.in_(valid_sensors),
            func.date(HistoricalData.timestamp).in_(dates_to_check),
            extract('hour', HistoricalData.timestamp).between(hour_min, hour_max)
        ).scalar()
        
        probability = int((result or 0) * 100)
        # Liczba "pomiarów" to liczba dni historycznych, które znaleźliśmy
        count = len(dates_to_check)

        return {"wynik": {"procent_zajetosci": probability, "liczba_pomiarow": count}}
        
    except Exception as e:
        logger.error(f"Stats Mobile Error: {e}")
        traceback.print_exc()
        return {"wynik": {"procent_zajetosci": 0, "liczba_pomiarow": 0}}

@app.post("/api/v1/user/buy_ticket")
def buy_ticket(req: TicketPurchase, db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.token == req.token).first()
        if not user: raise HTTPException(401, "Nieprawidłowy token")
        
        start = now_utc()
        end = start + datetime.timedelta(hours=req.duration_hours)
        final_price = req.total_price if req.total_price is not None else (5.0 * req.duration_hours)
        
        nt = Ticket(place_name=req.place_name, plate_number=req.plate_number, start_time=start, end_time=end, price=final_price, user_id=user.user_id)
        db.add(nt); db.flush()
        user.ticket_id = nt.id
        
        spot = db.query(ParkingSpot).filter(ParkingSpot.name == req.place_name).first()
        if spot: 
            spot.current_status = 1
            db.add(HistoricalData(sensor_id=spot.name, status=1))
            
        db.commit()
        return {"status": "ok", "ticket_id": nt.id, "end_time": end.isoformat()}
    except Exception as e: 
        db.rollback(); logger.error(f"Buy Ticket Error: {e}"); raise HTTPException(500, str(e))

@app.get("/api/v1/user/ticket/active")
def get_active_ticket(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401)
    if not user.ticket_id: return {"status": "no_ticket"}
    t = db.query(Ticket).filter(Ticket.id == user.ticket_id).first()
    if not t:
        user.ticket_id = None; db.commit(); return {"status": "no_ticket"}
    if t.end_time < now_utc().replace(tzinfo=None): return {"status": "expired"}
    return {"status": "found", "id": t.id, "place_name": t.place_name, "end_time": t.end_time, "plate_number": t.plate_number, "price": float(t.price)}

# --- AIRBNB (NAPRAWIONE: TOKEN OBSŁUGIWANY) ---
@app.post("/api/v1/airbnb/add")
def add_airbnb(a: AirbnbAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == a.token).first()
    if not u: raise HTTPException(401, "Nieautoryzowany")
    try:
        db.add(AirbnbOffer(title=a.title, description=a.description, price=a.price, h_availability=a.h_availability, owner_name=u.email, owner_user_id=u.user_id, contact=a.contact, state_id=a.district_id, start_date=datetime.datetime.strptime(a.start_date, "%Y-%m-%d").date() if a.start_date else None, end_date=datetime.datetime.strptime(a.end_date, "%Y-%m-%d").date() if a.end_date else None, latitude=a.latitude, longitude=a.longitude))
        db.commit(); return {"status": "ok"}
    except Exception as e: db.rollback(); raise HTTPException(500, f"DB Error: {str(e)}")

@app.post("/api/v1/airbnb/update")
def update_airbnb(u: AirbnbUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == u.token).first()
    if not user: raise HTTPException(401, "Nieautoryzowany")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == u.offer_id).first()
    if not offer: raise HTTPException(404, "Nie znaleziono oferty")
    if offer.owner_user_id != user.user_id: raise HTTPException(403, "Brak uprawnień")
    try:
        if u.title is not None: offer.title = u.title
        if u.description is not None: offer.description = u.description
        if u.price is not None: offer.price = u.price
        if u.h_availability is not None: offer.h_availability = u.h_availability
        if u.contact is not None: offer.contact = u.contact
        if u.district_id is not None: offer.state_id = u.district_id
        if u.latitude is not None: offer.latitude = u.latitude
        if u.longitude is not None: offer.longitude = u.longitude
        if u.start_date: offer.start_date = datetime.datetime.strptime(u.start_date, "%Y-%m-%d").date()
        if u.end_date: offer.end_date = datetime.datetime.strptime(u.end_date, "%Y-%m-%d").date()
        db.commit(); return {"status": "updated"}
    except Exception as e: db.rollback(); logger.error(f"Airbnb Update: {e}"); raise HTTPException(500, f"Błąd: {str(e)}")

@app.post("/api/v1/airbnb/delete")
def delete_airbnb(d: AirbnbDelete, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == d.token).first()
    if not user: raise HTTPException(401, "Nieautoryzowany")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == d.offer_id).first()
    if not offer: raise HTTPException(404, "Nie znaleziono oferty")
    if offer.owner_user_id != user.user_id: raise HTTPException(403, "Brak uprawnień")
    try: db.delete(offer); db.commit(); return {"status": "deleted"}
    except Exception as e: db.rollback(); logger.error(f"Airbnb Delete: {e}"); raise HTTPException(500, f"Błąd bazy: {str(e)}")

@app.get("/api/v1/airbnb/offers", response_model=List[AirbnbResponse])
def get_airbnb(district_id: Optional[int] = None, token: Optional[str] = None, db: Session = Depends(get_db)):
    try:
        current_user_id = None
        if token:
            u = db.query(User).filter(User.token == token).first()
            if u: current_user_id = u.user_id

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
                    "h_availability": o.h_availability, "contact": o.contact,
                    "owner_name": o.owner_name, "owner_user_id": o.owner_user_id,
                    "latitude": float(o.latitude) if o.latitude is not None else 0.0,
                    "longitude": float(o.longitude) if o.longitude is not None else 0.0,
                    "state_name": s_name, "start_date": o.start_date, "end_date": o.end_date,
                    "is_mine": (current_user_id is not None and o.owner_user_id == current_user_id) # FIX DLA EDYCJI
                })
            except Exception as e: continue
        return res
    except Exception as e: logger.error(f"Global Airbnb Error: {e}"); return []

@app.post("/api/v1/user/vehicle/add")
def add_user_vehicle(v: VehicleAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first()
    if not user: raise HTTPException(401, "Nieautoryzowany")
    try: db.add(Vehicle(name=v.name, plate_number=v.plate_number, user_id=user.user_id)); db.commit(); return {"status": "ok"}
    except Exception as e: db.rollback(); raise HTTPException(500, f"Błąd bazy: {str(e)}")

@app.post("/api/v1/user/vehicle/delete")
def delete_user_vehicle(v: VehicleDelete, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first()
    if not user: raise HTTPException(401, "Nieautoryzowany")
    try:
        veh = db.query(Vehicle).filter(Vehicle.id_veh == v.vehicle_id, Vehicle.user_id == user.user_id).first()
        if veh: db.delete(veh); db.commit(); return {"status": "deleted"}
        else: raise HTTPException(404, "Pojazd nie znaleziony")
    except Exception as e: db.rollback(); raise HTTPException(500, f"Błąd bazy: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
