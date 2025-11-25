import os
import datetime
import logging
import secrets
import json
import threading
import time
import traceback
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, Request, WebSocket, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float, Text, Table, func, Date, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
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
    password_hashed = Column(String(255), nullable=False)
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
    district_id = Column(Integer, ForeignKey("districts.id"))
    start_date = Column(Date)
    end_date = Column(Date)
    created_at = Column(DateTime, default=now_utc)

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

# --- MODELE DO EDYCJI DANYCH (ADMIN) ---
class DistrictPayload(BaseModel):
    id: Optional[int] = None
    district: str
    city: str = "Inowrocław"
    description: Optional[str] = None
    price_info: Optional[str] = None
    capacity: int = 0

class StatePayload(BaseModel):
    state_id: Optional[int] = None
    name: str 
    city: str = "Inowrocław"

class SpotPayload(BaseModel):
    name: str # Klucz główny sensora (np. BUD_4)
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
        requests.post("[https://exp.host/--/api/v2/push/send](https://exp.host/--/api/v2/push/send)", json=payload, timeout=2)
        logger.info(f"PUSH SENT: {token}")
    except Exception as e: logger.error(f"PUSH ERROR: {e}")

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
                    db.add(HistoricalData(sensor_id=sensor_name, status=status))
                    if status == 1:
                        subs = db.query(DeviceSubscription).filter(DeviceSubscription.sensor_name == sensor_name).all()
                        for sub in subs:
                            send_push(
                                sub.device_token, 
                                "⚠️ Ktoś zajął Twoje miejsce!", 
                                f"Miejsce {sensor_name} zajęte. Kliknij, aby znaleźć alternatywę.", 
                                data={"action": "find_alt"}
                            )
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
    if db.query(User).filter(User.email == u.email).first(): raise HTTPException(400, "Email zajęty")
    db.add(User(email=u.email, password_hashed=get_password_hash(u.password), phone_number=u.phone_number)); db.commit(); return {"status": "registered"}

@app.post("/api/v1/auth/login")
def login(u: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == u.email).first()
    if not user or not verify_password(u.password, user.password_hashed): raise HTTPException(401, "Błędne dane")
    token = secrets.token_hex(16); user.token = token; db.commit()
    return {"token": token, "email": user.email, "user_id": user.user_id, "is_disabled": user.is_disabled}

@app.get("/api/v1/aktualny_stan")
def get_spots(
    city: Optional[str] = None,
    is_disabled_friendly: Optional[bool] = None,
    is_ev: Optional[bool] = None,
    db: Session = Depends(get_db)
):
    query = db.query(ParkingSpot)
    if city: query = query.filter(ParkingSpot.city == city)
    if is_disabled_friendly: query = query.filter(ParkingSpot.is_disabled_friendly == True)
    if is_ev: query = query.filter(ParkingSpot.is_ev == True)

    spots = query.all()
    res = []
    
    for s in spots:
        override_disabled = True if s.name == "euro_4" else s.is_disabled_friendly
        
        dist_obj = s.district_rel
        parking_name = dist_obj.district if dist_obj else "Parking Ogólny"
        parking_desc = dist_obj.description if dist_obj else ""
        parking_price = dist_obj.price_info if dist_obj else ""
        parking_capacity = dist_obj.capacity if dist_obj else 0
        rejon_name = s.state_rel.name if s.state_rel else "Brak danych"

        coords_obj = None
        if s.coordinates and ',' in s.coordinates:
            try: parts = s.coordinates.split(','); coords_obj = {"latitude": float(parts[0]), "longitude": float(parts[1])}
            except: pass
        
        res.append({
            "sensor_id": s.name,
            "status": s.current_status,
            "city": s.city,
            "state": rejon_name, 
            "place_name": parking_name, 
            "place_description": parking_desc,
            "place_price": parking_price,
            "place_capacity": parking_capacity,
            "district_id": s.district_id,
            "state_id": s.state_id,
            "wspolrzedne": coords_obj,
            "is_disabled_friendly": override_disabled,
            "is_ev": s.is_ev,
            "is_paid": s.is_paid
        })
    return res

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

@app.get("/api/v1/airbnb/offers")
def get_airbnb(district_id: Optional[int] = None, db: Session = Depends(get_db)):
    q = db.query(AirbnbOffer)
    if district_id: q = q.filter(AirbnbOffer.district_id == district_id)
    return q.all()

@app.post("/api/v1/statystyki/zajetosc")
def stats_mobile(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    try: t = datetime.datetime.strptime(z.selected_date, "%Y-%m-%d").date()
    except: raise HTTPException(400)
    s = datetime.datetime.combine(t, datetime.time(z.selected_hour, 0)); e = s + datetime.timedelta(hours=1)
    tot = db.query(HistoricalData).filter(HistoricalData.sensor_id==z.sensor_id, HistoricalData.timestamp>=s, HistoricalData.timestamp<e).count()
    occ = db.query(HistoricalData).filter(HistoricalData.sensor_id==z.sensor_id, HistoricalData.timestamp>=s, HistoricalData.timestamp<e, HistoricalData.status==1).count()
    return {"wynik": {"procent_zajetosci": int((occ/tot)*100) if tot>0 else 0, "liczba_pomiarow": tot}}

# --- ADMIN MANAGEMENT ENDPOINTS (CRUD) ---

@app.post("/api/v1/admin/manage/district")
async def manage_district(d: DistrictPayload, db: Session = Depends(get_db)):
    try:
        if d.id:
            existing = db.query(District).filter(District.id == d.id).first()
            if existing:
                existing.district = d.district
                existing.city = d.city
                existing.description = d.description
                existing.price_info = d.price_info
                existing.capacity = d.capacity
                db.commit()
                return {"status": "updated", "id": existing.id}
            else: raise HTTPException(404, "District not found")
        else:
            new_dist = District(district=d.district, city=d.city, description=d.description, price_info=d.price_info, capacity=d.capacity)
            db.add(new_dist); db.commit(); return {"status": "created", "id": new_dist.id}
    except Exception as e: db.rollback(); raise HTTPException(500, f"Error: {e}")

@app.post("/api/v1/admin/manage/state")
async def manage_state(s: StatePayload, db: Session = Depends(get_db)):
    try:
        if s.state_id:
            existing = db.query(State).filter(State.state_id == s.state_id).first()
            if existing: existing.name = s.name; existing.city = s.city; db.commit(); return {"status": "updated", "state_id": existing.state_id}
            else: raise HTTPException(404, "State not found")
        else:
            new_state = State(name=s.name, city=s.city); db.add(new_state); db.commit(); return {"status": "created", "state_id": new_state.state_id}
    except Exception as e: db.rollback(); raise HTTPException(500, f"Error: {e}")

@app.post("/api/v1/admin/manage/spot")
async def manage_spot(s: SpotPayload, db: Session = Depends(get_db)):
    """
    Edytuje statyczne dane miejsca (nie status).
    Tworzy miejsce jeśli nie istnieje.
    """
    try:
        spot = db.query(ParkingSpot).filter(ParkingSpot.name == s.name).first()
        if not spot:
            spot = ParkingSpot(name=s.name)
            db.add(spot)
        
        # Aktualizuj tylko te pola, które zostały przesłane (nie są null)
        if s.city is not None: spot.city = s.city
        if s.state_id is not None: spot.state_id = s.state_id
        if s.district_id is not None: spot.district_id = s.district_id
        if s.coordinates is not None: spot.coordinates = s.coordinates
        if s.is_disabled_friendly is not None: spot.is_disabled_friendly = s.is_disabled_friendly
        if s.is_ev is not None: spot.is_ev = s.is_ev
        if s.is_paid is not None: spot.is_paid = s.is_paid
        
        db.commit()
        return {"status": "updated", "name": spot.name}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Error: {e}")

# --- SIMULATION ENDPOINT ---
@app.post("/api/v1/iot/update")
async def iot_update_http(data: dict, db: Session = Depends(get_db)):
    try:
        name = data.get("name"); stat = int(data.get("status", 0)); dist_id = data.get("district_id"); state_id = data.get("state_id") 
        if not name: raise HTTPException(400, "Brak nazwy")
        spot = db.query(ParkingSpot).filter(ParkingSpot.name == name).first()
        if not spot: spot = ParkingSpot(name=name, current_status=stat); db.add(spot)
        if dist_id is not None:
            if not db.query(District).filter(District.id == int(dist_id)).first(): raise HTTPException(400, f"District {dist_id} not found")
            spot.district_id = int(dist_id)
        if state_id is not None: 
            if not db.query(State).filter(State.state_id == int(state_id)).first(): raise HTTPException(400, f"State {state_id} not found")
            spot.state_id = int(state_id)
        prev = spot.current_status; spot.current_status = stat; spot.last_seen = now_utc()
        if prev != stat:
            db.add(HistoricalData(sensor_id=name, status=stat))
            if stat == 1:
                subs = db.query(DeviceSubscription).filter(DeviceSubscription.sensor_name == name).all()
                for sub in subs: send_push(sub.device_token, "⚠️ Ktoś zajął Twoje miejsce!", f"Miejsce {name} zajęte. Kliknij, aby znaleźć alternatywę.", data={"action": "find_alt"}); db.delete(sub)
        db.commit()
        await manager.broadcast({"sensor_id": name, "status": stat})
        return {"status": "updated"}
    except Exception as e: db.rollback(); raise HTTPException(500, detail=f"Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
