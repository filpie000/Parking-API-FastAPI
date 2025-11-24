import os
import datetime
import logging
import secrets
import json
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float, Text, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

import bcrypt
import msgpack

# --- KONFIGURACJA LOGOWANIA ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

# --- BAZA DANYCH (STABILNE POŁĄCZENIE) ---
# Domyślny URL bazy - upewnij się, że pasuje do Twojego środowiska
DATABASE_URL = os.environ.get('DATABASE_URL', "postgresql://postgres:postgres@localhost:5432/postgres")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,  # KLUCZOWE: Sprawdza połączenie przed każdym zapytaniem (fix na nocne zwisy)
    pool_recycle=3600,   # Odświeża połączenie co godzinę
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

# --- MODELE BAZY DANYCH ---

# Tabela łącząca: MIEJSCA <-> GRUPY (Many-to-Many)
spot_group_members = Table('spot_group_members', Base.metadata,
    Column('spot_name', String, ForeignKey('parking_spots.name', ondelete="CASCADE"), primary_key=True),
    Column('group_id', Integer, ForeignKey('groups.id', ondelete="CASCADE"), primary_key=True)
)

class Group(Base):
    __tablename__ = 'groups'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    # Relacja: Grupa ma wiele miejsc
    spots = relationship("ParkingSpot", secondary=spot_group_members, back_populates="groups")

class ParkingSpot(Base):
    __tablename__ = "parking_spots"
    name = Column(String, primary_key=True) # np. EURO_3
    city = Column(String, nullable=True)
    state = Column(String, nullable=True) # Rejon
    coordinates = Column(String, nullable=True) # Format "LAT,LON"
    
    # Status jako INT (0=Free, 1=Occupied, 2=Blocked)
    current_status = Column(Integer, default=0) 
    last_status_change = Column(DateTime(timezone=True), default=now_utc)
    
    # Cechy
    is_disabled_friendly = Column(Boolean, default=False)
    is_ev = Column(Boolean, default=False)
    is_paid = Column(Boolean, default=True)
    
    # Relacja: Miejsce może być w wielu grupach
    groups = relationship("Group", secondary=spot_group_members, back_populates="spots")

class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    badge_name = Column(String, default="Admin")
    # Relacja 1:1 do uprawnień
    permissions = relationship("AdminPermissions", back_populates="admin", uselist=False, cascade="all, delete-orphan")

class AdminPermissions(Base):
    __tablename__ = "admin_permissions"
    admin_id = Column(Integer, ForeignKey("admins.id"), primary_key=True)
    city = Column(String, default='ALL')
    view_disabled_only = Column(Boolean, default=False)
    view_ev_only = Column(Boolean, default=False)
    view_paid_only = Column(Boolean, default=False)
    allowed_states = Column(Text, nullable=True) # Lista po przecinku
    admin = relationship("Admin", back_populates="permissions")

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime(timezone=True), default=now_utc, index=True)
    spot_name = Column(String, index=True) 
    status = Column(Integer) # Historia też trzyma INT

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, unique=True, index=True)
    password_hash = Column(String) # Ujednolicona nazwa
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
    name = Column(String, nullable=True)
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
    status = Column(String, default='ACTIVE')
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
    created_at = Column(DateTime(timezone=True), default=now_utc)

# Bezpieczne tworzenie tabel
Base.metadata.create_all(bind=engine)

# --- PYDANTIC SCHEMAS ---
class AdminLogin(BaseModel):
    username: str
    password: str

class AdminPayload(BaseModel):
    id: Optional[int] = None
    username: str
    password: Optional[str] = None
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
    requester_permissions: Optional[Dict] = None

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

class UserPermissionsUpdate(BaseModel):
    target_email: str
    perm_disabled: bool

# --- UTILS ---
def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain: str, hashed: str) -> bool:
    if not hashed: return False
    try:
        return bcrypt.checkpw(plain.encode('utf-8'), hashed.encode('utf-8'))
    except:
        return False

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

# --- APP STARTUP ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    # Inicjalizacja domyślnego admina, jeśli baza jest pusta
    with SessionLocal() as db:
        try:
            if not db.query(Admin).first():
                logger.info("Inicjalizacja: Tworzenie konta admin/admin123")
                sa = Admin(username="admin", password_hash=get_password_hash("admin123"), badge_name="Super Admin")
                db.add(sa); db.flush()
                db.add(AdminPermissions(admin_id=sa.id, city="ALL"))
                db.commit()
        except Exception as e:
            logger.error(f"Błąd startupu: {e}")

@app.get("/")
def root(): return {"status": "Parking System Online", "version": "2.1 Stable"}

@app.get("/dashboard", response_class=HTMLResponse)
def dash():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Brak pliku dashboard.html"

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True: await ws.receive_text()
    except: manager.disconnect(ws)

# --- ENDPOINTY DLA DASHBOARDU I APLIKACJI (NOWE) ---

@app.get("/api/v1/options/filters")
def get_filter_options(db: Session = Depends(get_db)):
    # Pobieramy unikalne miasta z miejsc
    cities = db.query(ParkingSpot.city).distinct().all()
    # Pobieramy nazwy grup z nowej tabeli groups
    groups = db.query(Group.name).all()
    
    return {
        "cities": [c[0] for c in cities if c[0]],
        "states": [g.name for g in groups] # Frontend używa 'states' jako listy grup do wyboru
    }

@app.get("/api/v1/aktualny_stan")
def get_spots(limit: int=100, db: Session = Depends(get_db)):
    spots = db.query(ParkingSpot).limit(limit).all()
    res = []
    for s in spots:
        # Pobieramy nazwy grup do których należy to miejsce
        grp_names = [g.name for g in s.groups]
        
        # Parsowanie współrzędnych (jeśli są w bazie)
        coords_obj = None
        if s.coordinates and ',' in s.coordinates:
            try:
                parts = s.coordinates.split(',')
                coords_obj = {"latitude": float(parts[0]), "longitude": float(parts[1])}
            except: pass

        res.append({
            "sensor_id": s.name,
            "name": s.name,
            "status": s.current_status, # INT (0,1,2)
            "groups": grp_names,        # Lista grup
            "city": s.city,
            "state": s.state,
            "wspolrzedne": coords_obj,
            "is_disabled_friendly": s.is_disabled_friendly,
            "is_ev": s.is_ev,
            "is_paid": s.is_paid,
            "adres": f"{s.state or ''}, {s.city or ''}".strip(', '),
            "typ": 'niepelnosprawni' if s.is_disabled_friendly else ('ev' if s.is_ev else 'zwykle'),
            "cennik": "3.00 PLN/h" if s.is_paid else "Bezpłatny"
        })
    return res

@app.post("/api/v1/dashboard/raport")
def get_report(r: RaportRequest, db: Session = Depends(get_db)):
    try:
        s_date = datetime.datetime.strptime(r.start_date, "%Y-%m-%d")
        e_date = datetime.datetime.strptime(r.end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
    except: raise HTTPException(400, "Zła data")

    # Logika Many-to-Many: Znajdź sensory należące do wybranych grup
    target_spots = []
    if r.groups:
        spots_in_groups = db.query(ParkingSpot).join(ParkingSpot.groups).filter(Group.name.in_(r.groups)).all()
        target_spots = [s.name for s in spots_in_groups]
    
    if not target_spots: return {} # Pusty raport jeśli brak sensorów

    # Pobierz historię tylko dla tych sensorów
    history = db.query(DaneHistoryczne).filter(
        DaneHistoryczne.czas_pomiaru >= s_date,
        DaneHistoryczne.czas_pomiaru < e_date,
        DaneHistoryczne.spot_name.in_(target_spots)
    ).all()

    result = {g: [0]*24 for g in r.groups}

    # Cache mapowania: Sensor -> Grupy
    all_spots = db.query(ParkingSpot).filter(ParkingSpot.name.in_(target_spots)).all()
    sensor_to_groups = {s.name: [g.name for g in s.groups] for s in all_spots}

    for h in history:
        # Dla każdego wpisu historycznego sprawdź, do których grup należy czujnik
        affected_groups = sensor_to_groups.get(h.spot_name, [])
        
        for grp in affected_groups:
            if grp in result:
                local_time = h.czas_pomiaru.replace(tzinfo=UTC).astimezone(PL_TZ)
                # 1 = Zajęte (INT)
                if h.status == 1:
                    result[grp][local_time.hour] += 1

    # Normalizacja (prosta, procentowa)
    for g in result:
        mx = max(result[g]) if result[g] else 1
        result[g] = [round((x/mx)*100, 1) if mx>0 else 0 for x in result[g]]

    return result

# --- ADMIN API ---
@app.post("/api/v1/admin/auth")
def admin_login(data: AdminLogin, db: Session = Depends(get_db)):
    admin = db.query(Admin).filter(Admin.username == data.username).first()
    if not admin or not verify_password(data.password, admin.password_hash): raise HTTPException(401)
    
    perms = {}
    if admin.permissions:
        perms = {"city": admin.permissions.city, "allowed_states": admin.permissions.allowed_states, "view_disabled_only": admin.permissions.view_disabled_only}
    return {"username": admin.username, "is_superadmin": (admin.username == 'admin'), "permissions": perms}

@app.get("/api/v1/admin/list")
def list_admins(db: Session = Depends(get_db)):
    return [{"id": a.id, "username": a.username, "permissions": {
        "city": a.permissions.city if a.permissions else "ALL",
        "allowed_states": a.permissions.allowed_states if a.permissions else ""
    }} for a in db.query(Admin).all()]

@app.post("/api/v1/admin/create")
def create_admin(d: AdminPayload, db: Session = Depends(get_db)):
    if db.query(Admin).filter(Admin.username == d.username).first(): raise HTTPException(400, "Zajęte")
    na = Admin(username=d.username, password_hash=get_password_hash(d.password or "123"))
    db.add(na); db.flush()
    db.add(AdminPermissions(admin_id=na.id, city=d.city, allowed_states=d.allowed_states, view_disabled_only=d.view_disabled_only))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/admin/update")
def update_admin(d: AdminPayload, db: Session = Depends(get_db)):
    a = db.query(Admin).filter(Admin.id == d.id).first()
    if not a: raise HTTPException(404)
    if d.password: a.password_hash = get_password_hash(d.password)
    if not a.permissions: a.permissions = AdminPermissions(admin_id=a.id)
    a.permissions.city = d.city
    a.permissions.allowed_states = d.allowed_states
    a.permissions.view_disabled_only = d.view_disabled_only
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/admin/delete")
def delete_admin(d: AdminDelete, db: Session = Depends(get_db)):
    a = db.query(Admin).filter(Admin.id == d.target_id).first()
    if not a: raise HTTPException(404)
    if a.username == 'admin': raise HTTPException(400, "Root protected")
    db.delete(a); db.commit()
    return {"status": "ok"}

@app.get("/api/v1/admin/users")
def get_users(db: Session = Depends(get_db)):
    return [{"email": u.email, "is_disabled": u.is_disabled, "vehicle_count": len(u.vehicles), "perm_disabled": u.is_disabled_person} for u in db.query(User).all()]

@app.post("/api/v1/admin/toggle_user")
def toggle_user(d: UserToggle, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.email == d.target_email).first()
    if u: u.is_disabled = d.is_disabled; db.commit()
    return {"status": "ok"}

@app.post("/api/v1/admin/update_permissions")
def update_user_permissions(u: UserPermissionsUpdate, db: Session = Depends(get_db)):
    target = db.query(User).filter(User.email == u.target_email).first()
    if not target: raise HTTPException(404, "User not found")
    target.is_disabled_person = u.perm_disabled
    db.commit()
    return {"status": "updated"}

# --- USER AUTH API (Z FIXEM NA BŁĄD 500) ---
@app.post("/api/v1/auth/login")
def ulogin(u: UserAuth, db: Session = Depends(get_db)):
    try:
        usr = db.query(User).filter(User.email == u.email).first()
        if not usr:
            raise HTTPException(401, "Błędne dane")
        
        # Zabezpieczenie przed NULL w haśle
        if not usr.password_hash:
            raise HTTPException(500, "Konto uszkodzone (brak hasła). Zresetuj konto.")
            
        if not verify_password(u.password, usr.password_hash):
            raise HTTPException(401, "Błędne dane")
            
        tok = secrets.token_hex(16)
        usr.token = tok
        db.commit()
        
        # Zwracamy pełny profil usera
        return {
            "token": tok, 
            "email": usr.email, 
            "is_disabled": usr.is_disabled, # Flaga blokady konta
            "is_disabled_person": usr.is_disabled_person, # Flaga niepełnosprawności
            "dark_mode": usr.dark_mode
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"LOGIN ERROR: {e}")
        raise HTTPException(500, "Błąd serwera logowania")

@app.post("/api/v1/auth/register")
def uregister(u: UserAuth, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == u.email).first(): raise HTTPException(400, "Zajęty")
    db.add(User(email=u.email, password_hash=get_password_hash(u.password)))
    db.commit()
    return {"status": "ok"}

@app.get("/api/v1/user/me")
def ume(token: str, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == token).first()
    if not u: raise HTTPException(401)
    return {
        "email": u.email, 
        "is_disabled": u.is_disabled, 
        "is_disabled_person": u.is_disabled_person,
        "active_ticket_id": u.active_ticket_id,
        "vehicles": [{"id":v.id, "plate":v.plate_number} for v in u.vehicles]
    }

@app.post("/api/v1/user/vehicle")
def uaddveh(v: VehicleAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == v.token).first()
    if not u: raise HTTPException(401)
    db.add(Vehicle(name=v.name, plate_number=v.license_plate, user_id=u.id))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/user/ticket")
def ubuyticket(t: TicketAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == t.token).first()
    if not u: raise HTTPException(401)
    spot = db.query(ParkingSpot).filter(ParkingSpot.name == t.spot_name).first()
    if not spot: raise HTTPException(404, "Brak miejsca")
    
    nt = Ticket(user_id=u.id, vehicle_id=t.vehicle_id, spot_name=t.spot_name, price=t.price)
    db.add(nt); db.flush()
    u.active_ticket_id = nt.id
    
    # Aktualizacja statusu miejsca (INT)
    spot.current_status = 1 # Occupied
    spot.last_status_change = now_utc()
    db.add(DaneHistoryczne(spot_name=spot.name, status=1))
    
    db.commit()
    return {"status": "ok", "id": nt.id}

@app.get("/api/v1/user/ticket/active")
def uactive(token: str, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == token).first()
    if not u or not u.active_ticket_id: return None
    t = db.query(Ticket).filter(Ticket.id == u.active_ticket_id).first()
    if not t: return None
    return {"id": t.id, "spot_name": t.spot_name, "start_time": t.start_time.isoformat()}

# --- IOT UPDATE (Symulacja czujnika) ---
@app.post("/api/v1/iot/update")
async def iot(d: dict, db: Session = Depends(get_db)):
    name = d.get("name")
    try:
        # Konwersja na INT
        new_status = int(d.get("status"))
    except:
        return {"error": "Status must be int"}

    s = db.query(ParkingSpot).filter(ParkingSpot.name == name).first()
    if s:
        prev = s.current_status
        s.current_status = new_status
        s.last_status_change = now_utc()
        if prev != new_status:
            db.add(DaneHistoryczne(spot_name=s.name, status=new_status))
            await manager.broadcast({"sensor_id": s.name, "status": new_status})
        db.commit()
    return {"status": "ok"}

# --- AIRBNB ---
@app.get("/api/v1/airbnb/offers")
def get_airbnb(db: Session = Depends(get_db)):
    return [{"id": o.id, "title": o.title, "price": o.price, "district": o.district} for o in db.query(AirbnbOffer).all()]

@app.post("/api/v1/airbnb/add")
def add_airbnb(a: AirbnbAdd, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == a.token).first()
    if not u: raise HTTPException(401)
    db.add(AirbnbOffer(title=a.title, description=a.description, price=a.price, availability=a.availability,
                       latitude=a.latitude, longitude=a.longitude, district=a.district, owner_name=u.email))
    db.commit()
    return {"status": "ok"}

@app.post("/api/v1/airbnb/delete")
def del_airbnb(d: AirbnbDelete, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.token == d.token).first()
    if not u: raise HTTPException(401)
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == d.offer_id).first()
    if offer and offer.owner_name == u.email:
        db.delete(offer); db.commit()
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
