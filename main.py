import os
import datetime
import logging
import secrets
import json
import asyncio
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy import func

import bcrypt
import msgpack

# --- KONFIGURACJA LOGOWANIA ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

# --- BAZA DANYCH (FIX NA ZRYWANIE POŁĄCZENIA) ---
DATABASE_URL = os.environ.get('DATABASE_URL', "postgresql://postgres:postgres@localhost:5432/postgres")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,  # <--- TO NAPRAWIA PROBLEM "ZWISU" PO NOCY
    pool_recycle=3600,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- MODELE BAZY DANYCH (NOWA STRUKTURA) ---

class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    badge_name = Column(String, default="Admin")
    # Relacja 1:1 do tabeli uprawnień
    permissions = relationship("AdminPermissions", back_populates="admin", uselist=False, cascade="all, delete-orphan")

class AdminPermissions(Base):
    __tablename__ = "admin_permissions"
    admin_id = Column(Integer, ForeignKey("admins.id"), primary_key=True)
    city = Column(String, default='ALL')
    view_disabled_only = Column(Boolean, default=False)
    view_ev_only = Column(Boolean, default=False)
    view_paid_only = Column(Boolean, default=False)
    allowed_states = Column(Text, nullable=True) # Lista po przecinku np. "Centrum,Północ"
    admin = relationship("Admin", back_populates="permissions")

class ParkingSpot(Base):
    __tablename__ = "parking_spots"
    # name jest kluczem głównym (np. EURO_3)
    name = Column(String, primary_key=True) 
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    category = Column(String, default='public_spot')
    current_status = Column(String, default='FREE') # FREE, OCCUPIED, BLOCKED
    last_status_change = Column(DateTime(timezone=True), default=now_utc)
    
    # Cechy miejsca (do filtrów)
    is_disabled_friendly = Column(Boolean, default=False)
    is_ev = Column(Boolean, default=False)
    is_paid = Column(Boolean, default=True)

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime(timezone=True), default=now_utc, index=True)
    spot_name = Column(String, index=True) 
    status = Column(String) # FREE/OCCUPIED

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, unique=True, index=True)
    password_hash = Column(String)
    token = Column(String, index=True, nullable=True)
    
    is_disabled = Column(Boolean, default=False)        
    is_disabled_person = Column(Boolean, default=False) 
    dark_mode = Column(Boolean, default=False)
    
    active_ticket_id = Column(Integer, ForeignKey("tickets.id"), nullable=True)
    
    vehicles = relationship("Vehicle", back_populates="owner", cascade="all, delete-orphan")
    tickets = relationship("Ticket", back_populates="owner", foreign_keys="[Ticket.user_id]")

class Vehicle(Base):
    __tablename__ = "vehicles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=True) # np. "Moje Audi"
    plate_number = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="vehicles")

class Ticket(Base):
    __tablename__ = "tickets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    vehicle_id = Column(Integer, ForeignKey("vehicles.id"))
    spot_name = Column(String, ForeignKey("parking_spots.name"))
    
    start_time = Column(DateTime(timezone=True), default=now_utc)
    end_time = Column(DateTime(timezone=True), nullable=True)
    status = Column(String, default='ACTIVE') # ACTIVE, COMPLETED
    price = Column(Float, default=0.0)

    owner = relationship("User", back_populates="tickets", foreign_keys=[user_id])
    vehicle = relationship("Vehicle")

class AirbnbOffer(Base):
    __tablename__ = "airbnb_offers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String)
    description = Column(String)
    price = Column(String)
    availability = Column(String)
    owner_name = Column(String)
    contact = Column(String)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    district = Column(String, nullable=True)
    start_date = Column(String, nullable=True)
    end_date = Column(String, nullable=True)
    rating = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), default=now_utc)

# Tworzenie tabel
Base.metadata.create_all(bind=engine)

# --- SCHEMATY PYDANTIC ---

class AdminLogin(BaseModel):
    username: str
    password: str

class AdminPayload(BaseModel):
    id: Optional[int] = None
    username: str
    password: Optional[str] = None
    badge_name: str = "Admin"
    city: str = "ALL"
    allowed_states: str = ""
    view_disabled_only: bool = False
    view_ev_only: bool = False
    view_paid_only: bool = False

class AdminDelete(BaseModel):
    target_id: int

class UserAuth(BaseModel):
    email: str
    password: str

class UserToggle(BaseModel):
    target_email: str
    is_disabled: bool

class VehicleAdd(BaseModel):
    token: str
    name: str
    license_plate: str

class TicketAdd(BaseModel):
    token: str
    spot_name: str
    vehicle_id: int
    price: float = 0.0

class RaportRequest(BaseModel):
    start_date: str
    end_date: str
    groups: List[str]
    include_workdays: bool
    include_weekends: bool
    include_holidays: bool
    requester_permissions: Optional[Dict] = None # Zmieniono na dict

class AirbnbAdd(BaseModel):
    token: str
    title: str
    description: str
    price: str
    availability: str
    latitude: Optional[float]
    longitude: Optional[float]
    district: str
    start_date: str
    end_date: str

class AirbnbDelete(BaseModel):
    token: str
    offer_id: int

# --- UTILS ---
def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain: str, hashed: str) -> bool:
    if not hashed: return False
    return bcrypt.checkpw(plain.encode('utf-8'), hashed.encode('utf-8'))

# Ręczna mapa świąt (uproszczenie)
MANUALNA_MAPA_SWIAT = {
    datetime.date(2025, 1, 1): "Nowy Rok",
    datetime.date(2025, 4, 20): "Wielkanoc"
}

# --- WEBSOCKET MANAGER ---
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
        try:
            msg_bin = msgpack.packb(message)
            for conn in list(self.active_connections):
                try: await conn.send_bytes(msg_bin)
                except: self.disconnect(conn)
        except: pass

manager = ConnectionManager()

# --- APP START ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    # Tworzenie domyślnych adminów jeśli pusto
    with SessionLocal() as db:
        if not db.query(Admin).first():
            logger.info("Creating default Super Admin")
            sa = Admin(username="admin", password_hash=get_password_hash("admin123"), badge_name="Super Admin")
            db.add(sa); db.flush()
            db.add(AdminPermissions(admin_id=sa.id, city="ALL"))
            
            ea = Admin(username="euro_admin", password_hash=get_password_hash("euro123"), badge_name="Admin EURO")
            db.add(ea); db.flush()
            db.add(AdminPermissions(admin_id=ea.id, city="Inowrocław", view_disabled_only=True)) # Przykład logiki
            
            db.commit()

@app.get("/")
def root(): return {"status": "Parking System V2 Ready", "db": "PostgreSQL"}

@app.get("/dashboard", response_class=HTMLResponse)
def dash():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Brak dashboard.html"

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True: await ws.receive_text()
    except: manager.disconnect(ws)

# --- ADMIN API ---

@app.post("/api/v1/admin/auth")
def admin_login(data: AdminLogin, db: Session = Depends(get_db)):
    admin = db.query(Admin).filter(Admin.username == data.username).first()
    if not admin or not verify_password(data.password, admin.password_hash):
        raise HTTPException(401, "Błąd")
    
    is_super = (admin.username == 'admin')
    perms = {}
    if admin.permissions:
        perms = {
            "city": admin.permissions.city,
            "allowed_states": admin.permissions.allowed_states,
            "view_disabled_only": admin.permissions.view_disabled_only
        }
    
    return {
        "username": admin.username, 
        "is_superadmin": is_super, 
        "badge": admin.badge_name,
        "permissions": perms
    }

@app.get("/api/v1/admin/list")
def list_admins(db: Session = Depends(get_db)):
    admins = db.query(Admin).all()
    res = []
    for a in admins:
        p = a.permissions
        pd = {}
        if p:
            pd = {
                "city": p.city, "view_disabled_only": p.view_disabled_only, 
                "view_ev_only": p.view_ev_only, "allowed_states": p.allowed_states,
                "view_paid_only": p.view_paid_only
            }
        res.append({"id": a.id, "username": a.username, "badge_name": a.badge_name, "permissions": pd})
    return res

@app.post("/api/v1/admin/create")
def create_admin(d: AdminPayload, db: Session = Depends(get_db)):
    if db.query(Admin).filter(Admin.username == d.username).first(): raise HTTPException(400, "Zajęte")
    na = Admin(username=d.username, password_hash=get_password_hash(d.password or "123"), badge_name=d.badge_name)
    db.add(na)
    db.flush()
    db.add(AdminPermissions(admin_id=na.id, city=d.city, allowed_states=d.allowed_states,
                            view_disabled_only=d.view_disabled_only, view_ev_only=d.view_ev_only, view_paid_only=d.view_paid_only))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/admin/update")
def update_admin(d: AdminPayload, db: Session = Depends(get_db)):
    a = db.query(Admin).filter(Admin.id == d.id).first()
    if not a: raise HTTPException(404)
    if d.password: a.password_hash = get_password_hash(d.password)
    a.badge_name = d.badge_name
    
    if not a.permissions: a.permissions = AdminPermissions(admin_id=a.id)
    p = a.permissions
    p.city = d.city; p.allowed_states = d.allowed_states
    p.view_disabled_only = d.view_disabled_only; p.view_ev_only = d.view_ev_only; p.view_paid_only = d.view_paid_only
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/admin/delete")
def delete_admin(d: AdminDelete, db: Session = Depends(get_db)):
    a = db.query(Admin).filter(Admin.id == d.target_id).first()
    if not a: raise HTTPException(404)
    if a.username == 'admin': raise HTTPException(400, "Root protected")
    db.delete(a)
    db.commit()
    return {"status": "ok"}

# --- ADMIN USERS VIEW ---
@app.get("/api/v1/admin/users")
def get_users_list(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [{
        "email": u.email, 
        "is_disabled": u.is_disabled, 
        "vehicle_count": len(u.vehicles),
        "perm_disabled": u.is_disabled_person # mapujemy flagę
    } for u in users]

@app.post("/api/v1/admin/toggle_user")
def toggle_user(d: UserToggle, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.email == d.target_email).first()
    if u: u.is_disabled = d.is_disabled; db.commit()
    return {"status": "ok"}

@app.get("/api/v1/admin/groups")
def get_groups(db: Session = Depends(get_db)):
    # Dynamiczne grupy na podstawie nazw miejsc (np. EURO, BUD)
    # Pobieramy unikalne prefixy z nazw miejsc
    spots = db.query(ParkingSpot.name).all()
    prefixes = set()
    for s in spots:
        if "_" in s.name: prefixes.add(s.name.split('_')[0])
    return [{"name": p} for p in prefixes]

# --- USER API (MOBILE) ---

@app.post("/api/v1/auth/register")
def register(u: UserAuth, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == u.email).first(): raise HTTPException(400, "Zajęty")
    db.add(User(email=u.email, password_hash=get_password_hash(u.password)))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/auth/login")
def login(u: UserAuth, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == u.email).first()
    if not user or not verify_password(u.password, user.password_hash): raise HTTPException(401, "Błąd")
    token = secrets.token_hex(16)
    user.token = token
    db.commit()
    return {"token": token, "email": user.email, "is_disabled": user.is_disabled, "dark_mode": user.dark_mode}

@app.get("/api/v1/user/me")
def me(token: str, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == token).first()
    if not u: raise HTTPException(401)
    return {
        "email": u.email, 
        "is_disabled": u.is_disabled, 
        "vehicles": [{"id": v.id, "plate": v.plate_number, "name": v.name} for v in u.vehicles],
        "active_ticket_id": u.active_ticket_id
    }

@app.post("/api/v1/user/vehicle")
def add_veh(v: VehicleAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == v.token).first()
    if not u: raise HTTPException(401)
    db.add(Vehicle(name=v.name, plate_number=v.license_plate, user_id=u.id))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/user/ticket")
def buy_ticket(t: TicketAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == t.token).first()
    if not u: raise HTTPException(401)
    
    # Check if spot exists
    spot = db.query(ParkingSpot).filter(ParkingSpot.name == t.spot_name).first()
    if not spot: raise HTTPException(404, "Miejsce nie istnieje")
    
    # Create ticket
    nt = Ticket(user_id=u.id, vehicle_id=t.vehicle_id, spot_name=t.spot_name, price=t.price, start_time=now_utc())
    db.add(nt)
    db.flush()
    
    # Update User & Spot
    u.active_ticket_id = nt.id
    spot.current_status = 'OCCUPIED'
    spot.last_status_change = now_utc()
    
    # Add history
    db.add(DaneHistoryczne(spot_name=t.spot_name, status='OCCUPIED', czas_pomiaru=now_utc()))
    
    db.commit()
    return {"status": "ok", "ticket_id": nt.id}

@app.get("/api/v1/user/ticket/active")
def get_active_ticket(token: str, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == token).first()
    if not u or not u.active_ticket_id: return None
    t = db.query(Ticket).filter(Ticket.id == u.active_ticket_id).first()
    if not t: return None
    
    # Auto-close if needed logic here (skipped for brevity)
    return {
        "id": t.id, "spot_name": t.spot_name, 
        "start_time": t.start_time.isoformat(), 
        "vehicle_id": t.vehicle_id
    }

# --- IOT / STATUS ---
@app.get("/api/v1/aktualny_stan")
def get_spots(limit: int=100, db: Session = Depends(get_db)):
    spots = db.query(ParkingSpot).limit(limit).all()
    # Tutaj można dodać logikę filtrowania jeśli user przekaże parametry
    return [{
        "sensor_id": s.name, # Alias dla kompatybilności z frontendem
        "name": s.name, 
        "status": 1 if s.current_status == 'OCCUPIED' else 0, # Mapowanie statusu na int dla starego frontu
        "current_status": s.current_status,
        "city": s.city,
        "is_disabled_friendly": s.is_disabled_friendly
    } for s in spots]

# Symulacja IOT Update
@app.post("/api/v1/iot/update")
async def iot_update(data: dict, db: Session = Depends(get_db)):
    name = data.get("name")
    status = data.get("status") # FREE / OCCUPIED
    
    spot = db.query(ParkingSpot).filter(ParkingSpot.name == name).first()
    if spot:
        prev = spot.current_status
        spot.current_status = status
        spot.last_status_change = now_utc()
        
        if prev != status:
            db.add(DaneHistoryczne(spot_name=name, status=status, czas_pomiaru=now_utc()))
            await manager.broadcast({"sensor_id": name, "status": 1 if status=='OCCUPIED' else 0})
            
        db.commit()
    return {"status": "ok"}

# --- AIRBNB ---
@app.get("/api/v1/airbnb/offers")
def get_airbnb(db: Session = Depends(get_db)):
    offers = db.query(AirbnbOffer).order_by(AirbnbOffer.created_at.desc()).all()
    return [{"id": o.id, "title": o.title, "description": o.description, "price": o.price, 
             "owner": o.owner_name, "lat": o.latitude, "lon": o.longitude} for o in offers]

@app.post("/api/v1/airbnb/add")
def add_airbnb(a: AirbnbAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == a.token).first()
    if not u: raise HTTPException(401)
    
    offer = AirbnbOffer(
        title=a.title, description=a.description, price=a.price, availability=a.availability,
        owner_name=u.email.split('@')[0], contact=u.email,
        latitude=a.latitude, longitude=a.longitude, district=a.district,
        start_date=a.start_date, end_date=a.end_date
    )
    db.add(offer)
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/airbnb/delete")
def del_airbnb(d: AirbnbDelete, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == d.token).first()
    if not u: raise HTTPException(401)
    off = db.query(AirbnbOffer).filter(AirbnbOffer.id == d.offer_id).first()
    if off and off.contact == u.email:
        db.delete(off)
        db.commit()
    return {"status": "ok"}

# --- RAPORTY ---
@app.post("/api/v1/dashboard/raport")
def get_report(r: RaportRequest, db: Session = Depends(get_db)):
    try:
        s_date = datetime.datetime.strptime(r.start_date, "%Y-%m-%d")
        e_date = datetime.datetime.strptime(r.end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
    except: raise HTTPException(400, "Zła data")
    
    # Prosta logika raportu na podstawie DaneHistoryczne
    history = db.query(DaneHistoryczne).filter(
        DaneHistoryczne.czas_pomiaru >= s_date,
        DaneHistoryczne.czas_pomiaru < e_date
    ).all()
    
    # Grupowanie (np. EURO_1 -> EURO)
    result = {}
    for h in history:
        group = h.spot_name.split('_')[0]
        if group in r.groups:
            # Sprawdź dni wolne/święta (uproszczone)
            dt = h.czas_pomiaru.astimezone(PL_TZ)
            is_weekend = dt.weekday() >= 5
            is_holiday = dt.date() in MANUALNA_MAPA_SWIAT
            
            if is_weekend and not r.include_weekends: continue
            if not is_weekend and not is_holiday and not r.include_workdays: continue
            if is_holiday and not r.include_holidays: continue
            
            if group not in result: result[group] = [0]*24
            # Logika: zliczamy zajętość (uproszczona, 1 punkt za każdy wpis w danej godzinie)
            if h.status == 'OCCUPIED' or h.status == '1':
                result[group][dt.hour] += 1
                
    # Normalizacja do procentów (mockup, bo to wymaga skomplikowanej matematyki czasu trwania)
    for g in result:
        max_val = max(result[g]) if result[g] else 1
        result[g] = [round((x/max_val)*100, 1) if max_val > 0 else 0 for x in result[g]]
        
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
