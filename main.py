import os
import datetime
import json
import logging
import threading
import time
import asyncio
import secrets
from typing import Optional, List
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

import bcrypt
import requests
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

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

REDIS_URL = os.environ.get('REDIS_URL')
redis_client = None
if REDIS_URL:
    try:
        import redis
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        logger.warning("Redis not available: %s", e)

SENSOR_MAP = {
    1: "EURO_1", 2: "EURO_2", 3: "EURO_3", 4: "EURO_4",
    5: "BUD_1", 6: "BUD_2", 7: "BUD_3", 8: "BUD_4"
}
GRUPY_SENSOROW = ["EURO", "BUD"]
MANUALNA_MAPA_SWIAT = {
    datetime.date(2025, 1, 1): "Nowy Rok",
    datetime.date(2025, 4, 20): "Wielkanoc"
}

# --- DB MODELS ---

class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    role = Column(String)  # 'ALL', 'EURO', 'BUD' or 'CUSTOM'
    badge_name = Column(String)
    permissions = Column(String, default="")  # e.g. "VIEW_EURO,MANAGE_USERS"


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

    # uprawnienia
    perm_euro = Column(Boolean, default=False)
    perm_ev = Column(Boolean, default=False)
    perm_disabled = Column(Boolean, default=False)

    # relacje
    vehicles = relationship("Vehicle", back_populates="owner", cascade="all, delete-orphan")
    tickets = relationship("Ticket", back_populates="owner", cascade="all, delete-orphan")


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


# New: permissions and groups tables
class PermissionOption(Base):
    __tablename__ = "permission_options"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True, index=True)  # e.g. VIEW_EURO, MANAGE_USERS
    description = Column(String, nullable=True)


class GroupOption(Base):
    __tablename__ = "group_options"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, index=True)  # e.g. EURO, BUD
    description = Column(String, nullable=True)


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


# --- PYDANTIC MODELS ---
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
    requester_permissions: Optional[str] = "ALL"


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


class UserList(BaseModel):
    email: str
    is_disabled: bool
    vehicle_count: int
    perm_euro: bool
    perm_ev: bool
    perm_disabled: bool


class NewAdmin(BaseModel):
    username: str
    password: str
    badge_name: str
    permissions: str


class AdminList(BaseModel):
    id: int
    username: str
    badge_name: str
    permissions: str


class AdminDelete(BaseModel):
    target_id: int


class AdminUpdate(BaseModel):
    id: int
    password: Optional[str] = None
    badge_name: str
    permissions: str


class PermissionCreate(BaseModel):
    code: str
    description: Optional[str] = None


class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = None


# --- UTILS ---
def get_password_hash(password: str) -> str:
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pwd_bytes, salt)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception:
        return False


def calculate_occupancy_stats(sensor_prefix, selected_date_obj, selected_hour, db):
    return {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": "Brak"}


# --- APP ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# WebSocket Manager
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
            message_binary = msgpack.packb(message)
            for connection in list(self.active_connections):
                try:
                    await connection.send_bytes(message_binary)
                except Exception:
                    self.disconnect(connection)
        except Exception:
            pass


manager = ConnectionManager()


def send_push_notification(token, title, body, data):
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={"to": token, "title": title, "body": body, "data": data, "sound": "default", "priority": "high"}, timeout=3)
    except Exception as e:
        logger.error(f"PUSH ERROR: {e}")


async def process_parking_update(dane: dict, db: Session):
    if "sensor_id" not in dane:
        return
    sid = dane["sensor_id"]
    status = dane["status"]
    teraz = now_utc()
    m = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sid).first()
    prev_status = None
    if m:
        prev_status = m.status
        m.ostatnia_aktualizacja = teraz
        m.status = status
    else:
        db.add(AktualnyStan(sensor_id=sid, status=status, ostatnia_aktualizacja=teraz))

    # zapis historyczny jeśli zmiana
    if prev_status != status:
        db.add(DaneHistoryczne(czas_pomiaru=teraz, sensor_id=sid, status=status))
        db.commit()
        chg = {"sensor_id": sid, "status": status}
        if status == 1:
            limit = datetime.timedelta(hours=12)
            obs = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.sensor_id == sid, (teraz - ObserwowaneMiejsca.czas_dodania) < limit).all()
            if obs:
                for o in obs:
                    send_push_notification(o.device_token, "Zajęte!", f"{sid} zajęte.", {"action": "reroute"})
                    db.delete(o)
                db.commit()
        if manager.active_connections:
            await manager.broadcast([chg])
    else:
        db.commit()


# === ENDPOINTS ===
@app.get("/")
def root():
    return {"msg": "API OK"}


@app.get("/dashboard", response_class=HTMLResponse)
def dash():
    try:
        return open("dashboard.html", "r", encoding="utf-8").read()
    except Exception:
        return "Błąd dashboard.html"


# --- ADMIN AUTH ---
@app.post("/api/v1/admin/auth")
def admin_login(data: AdminLogin, db: Session = Depends(get_db)):
    admin = db.query(Admin).filter(Admin.username == data.username).first()
    if not admin or not verify_password(data.password, admin.password_hash):
        raise HTTPException(status_code=401, detail="Błędny login/hasło")
    perms = "ALL" if admin.role == "ALL" else (admin.permissions or "")
    return {"status": "ok", "username": admin.username, "role": admin.role, "permissions": perms, "badge": admin.badge_name}


@app.get("/api/v1/admin/list")
def list_admins(db: Session = Depends(get_db)):
    admins = db.query(Admin).all()
    return [AdminList(id=a.id, username=a.username, badge_name=a.badge_name, permissions=a.permissions or ("ALL" if a.role == "ALL" else "")) for a in admins]


@app.post("/api/v1/admin/create")
def create_admin(a: NewAdmin, db: Session = Depends(get_db)):
    if db.query(Admin).filter(Admin.username == a.username).first():
        raise HTTPException(status_code=400, detail="Zajęty")
    db.add(Admin(username=a.username, password_hash=get_password_hash(a.password), role="CUSTOM", badge_name=a.badge_name, permissions=a.permissions))
    db.commit()
    return {"status": "created"}


@app.post("/api/v1/admin/update")
def update_admin(u: AdminUpdate, db: Session = Depends(get_db)):
    target = db.query(Admin).filter(Admin.id == u.id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Not found")
    target.badge_name = u.badge_name
    target.permissions = u.permissions
    if u.password:
        target.password_hash = get_password_hash(u.password)
    db.commit()
    return {"status": "updated"}


@app.post("/api/v1/admin/delete")
def delete_admin(d: AdminDelete, db: Session = Depends(get_db)):
    target = db.query(Admin).filter(Admin.id == d.target_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Not found")
    if target.username == "admin":
        raise HTTPException(status_code=403, detail="Super Admin protected")
    db.delete(target)
    db.commit()
    return {"status": "deleted"}


# Permission list endpoints
@app.get("/api/v1/admin/permissions")
def list_permissions(db: Session = Depends(get_db)):
    perms = db.query(PermissionOption).all()
    return [{"code": p.code, "description": p.description} for p in perms]


@app.post("/api/v1/admin/permissions/create")
def create_permission(p: PermissionCreate, db: Session = Depends(get_db)):
    if not p.code or len(p.code.strip()) == 0:
        raise HTTPException(status_code=400, detail="Zły kod")
    code = p.code.strip().upper()
    if db.query(PermissionOption).filter(PermissionOption.code == code).first():
        raise HTTPException(status_code=400, detail="Permission exists")
    db.add(PermissionOption(code=code, description=p.description))
    db.commit()
    return {"status": "created", "code": code}


# Group list endpoints
@app.get("/api/v1/admin/groups")
def list_groups(db: Session = Depends(get_db)):
    groups = db.query(GroupOption).all()
    return [{"name": g.name, "description": g.description} for g in groups]


@app.post("/api/v1/admin/groups/create")
def create_group(g: GroupCreate, db: Session = Depends(get_db)):
    name = g.name.strip().upper()
    if db.query(GroupOption).filter(GroupOption.name == name).first():
        raise HTTPException(status_code=400, detail="Group exists")
    db.add(GroupOption(name=name, description=g.description))
    db.commit()
    return {"status": "created", "name": name}


# --- USER API ---
@app.post("/api/v1/auth/register")
def register(u: UserAuth, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == u.email).first():
        raise HTTPException(status_code=400, detail="Zajęty")
    db.add(User(email=u.email, hashed_password=get_password_hash(u.password), perm_euro=False, perm_ev=False, perm_disabled=False))
    db.commit()
    return {"status": "ok"}


@app.post("/api/v1/auth/login")
def login(u: UserAuth, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == u.email).first()
    if not user or not verify_password(u.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Błędne")
    token = secrets.token_hex(16)
    user.token = token
    db.commit()
    dm = getattr(user, 'dark_mode', False)
    return {"token": token, "email": user.email, "is_disabled": user.is_disabled, "dark_mode": dm}


@app.get("/api/v1/user/me")
def me(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    dm = getattr(user, 'dark_mode', False)
    return {
        "email": user.email,
        "is_disabled": user.is_disabled,
        "perm_disabled": getattr(user, 'perm_disabled', False),
        "perm_ev": getattr(user, 'perm_ev', False),
        "perm_euro": getattr(user, 'perm_euro', False),
        "dark_mode": dm,
        "vehicles": [{"name": v.name, "plate": v.license_plate} for v in user.vehicles]
    }


@app.post("/api/v1/user/status")
def status(s: StatusUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    user.is_disabled = s.is_disabled
    user.perm_disabled = s.is_disabled
    db.commit()
    return {"status": "updated"}


@app.post("/api/v1/user/darkmode")
def update_darkmode(s: DarkModeUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    if hasattr(user, 'dark_mode'):
        user.dark_mode = s.dark_mode
        db.commit()
    return {"status": "updated"}


@app.post("/api/v1/user/vehicle")
def add_veh(v: VehicleAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    db.add(Vehicle(name=v.name, license_plate=v.license_plate, user_id=user.id))
    db.commit()
    return {"status": "added"}


@app.post("/api/v1/user/ticket")
def buy_ticket(t: TicketAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == t.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    try:
        end_dt = datetime.datetime.fromisoformat(t.end_time.replace('Z', '+00:00'))
    except Exception:
        end_dt = now_utc() + datetime.timedelta(hours=1)
    new_ticket = Ticket(user_id=user.id, sensor_id=t.sensor_id, place_name=t.place_name, plate=t.plate, start_time=now_utc(), end_time=end_dt, price=t.price)
    db.add(new_ticket)
    db.commit()
    return {"status": "ticket_created", "id": new_ticket.id}


@app.get("/api/v1/user/ticket/active")
def get_active_ticket(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    ticket = db.query(Ticket).filter(Ticket.user_id == user.id, Ticket.end_time > now_utc()).order_by(Ticket.end_time.desc()).first()
    if ticket:
        return {"placeName": ticket.place_name, "sensorId": ticket.sensor_id, "plate": ticket.plate, "startTime": ticket.start_time.isoformat(), "endTime": ticket.end_time.isoformat(), "price": ticket.price}
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
    if istniejacy:
        istniejacy.sensor_id = r.sensor_id
        istniejacy.czas_dodania = now_utc()
    else:
        db.add(ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id))
    try:
        db.commit()
        return {"status": "ok"}
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="Error")


# === RAPORT (z filtrowaniem uprawnień i grup) ===
@app.post("/api/v1/dashboard/raport")
def rep(r: RaportRequest, request: Request, db: Session = Depends(get_db)):
    client_ip = request.client.host
    limit_key = f"ratelimit:report:{client_ip}"
    if redis_client:
        try:
            val = redis_client.get(limit_key)
            if val and int(val) >= 5:
                raise HTTPException(status_code=429, detail="Limit")
            redis_client.incr(limit_key)
            redis_client.expire(limit_key, 60)
        except Exception:
            pass

    try:
        s_date = datetime.datetime.strptime(r.start_date, "%Y-%m-%d").date()
        e_date = datetime.datetime.strptime(r.end_date, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Zła data")

    admin_perms = r.requester_permissions or "ALL"

    allowed_groups = []
    if "ALL" in admin_perms:
        allowed_groups = r.groups
    else:
        for g in r.groups:
            if f"VIEW_{g}" in admin_perms:
                allowed_groups.append(g)

    if not allowed_groups:
        return {}

    agg = {g: {h: [] for h in range(24)} for g in allowed_groups}

    cols = (DaneHistoryczne.sensor_id, DaneHistoryczne.status, DaneHistoryczne.czas_pomiaru)
    query = db.query(*cols).filter(DaneHistoryczne.czas_pomiaru >= datetime.datetime.combine(s_date, datetime.time.min), DaneHistoryczne.czas_pomiaru <= datetime.datetime.combine(e_date, datetime.time.max)).yield_per(1000)

    for row in query:
        sid, stat, czas = row
        if not czas:
            continue

        # uprawnienia dodatkowe: jeśli requester nie ma ALL, sprawdzamy szczegóły dla sensorów
        if "ALL" not in admin_perms:
            is_ev = sid == "EURO_3"
            is_disabled = sid == "EURO_4"
            is_euro = sid.startswith("EURO")
            is_bud = sid.startswith("BUD")
            pass_check = False
            if "VIEW_EURO" in admin_perms and is_euro:
                pass_check = True
            if "VIEW_BUD" in admin_perms and is_bud:
                pass_check = True
            if "VIEW_EV" in admin_perms and is_ev:
                pass_check = True
            if "VIEW_DISABLED" in admin_perms and is_disabled:
                pass_check = True
            if not pass_check:
                continue

        # strefa czasowa i filtry dni
        local_time = czas.astimezone(PL_TZ) if czas.tzinfo else czas.replace(tzinfo=UTC).astimezone(PL_TZ)
        is_weekend = local_time.weekday() >= 5
        if is_weekend and not r.include_weekends:
            continue
        if not is_weekend and not r.include_workdays:
            continue
        # święta - jeżeli nie chcemy świąt, pomijamy (proste mapowanie)
        if not r.include_holidays:
            if local_time.date() in MANUALNA_MAPA_SWIAT:
                continue

        try:
            group = sid.split('_')[0]
            if group in agg:
                agg[group][local_time.hour].append(stat)
        except Exception:
            pass

    result = {}
    for g in allowed_groups:
        data_points = []
        for h in range(24):
            values = agg[g][h]
            if not values:
                data_points.append(0)
            else:
                data_points.append(round((sum(values) / len(values)) * 100, 1))
        result[g] = data_points
    return result


# === OTHER API (airbnb, admin users) ===
@app.get("/api/v1/airbnb/offers")
def get_airbnb_offers(db: Session = Depends(get_db)):
    offers = db.query(AirbnbOffer).order_by(AirbnbOffer.created_at.desc()).all()
    return [{"id": str(o.id), "title": o.title, "description": o.description, "price": o.price, "availability": o.availability, "period": f"{o.start_date or ''} - {o.end_date or ''}", "owner": o.owner_name, "rating": o.rating, "latitude": o.latitude, "longitude": o.longitude, "district": o.district, "start_date": o.start_date, "end_date": o.end_date} for o in offers]


@app.post("/api/v1/airbnb/add")
def add_airbnb_offer(a: AirbnbAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == a.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    new_offer = AirbnbOffer(title=a.title, description=a.description, price=a.price, availability=a.availability, period="", owner_name=user.email.split('@')[0], contact=user.email, latitude=a.latitude, longitude=a.longitude, district=a.district, start_date=a.start_date, end_date=a.end_date)
    db.add(new_offer)
    db.commit()
    return {"status": "added"}


@app.post("/api/v1/airbnb/location")
def update_offer_location(u: AirbnbLocationUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == u.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == int(u.offer_id)).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Not found")
    if offer.owner_name != user.email.split('@')[0]:
        raise HTTPException(status_code=403, detail="Brak uprawnień")
    offer.latitude = u.latitude
    offer.longitude = u.longitude
    db.commit()
    return {"status": "location_updated"}


@app.post("/api/v1/airbnb/delete")
def delete_offer(d: AirbnbDelete, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == d.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    try:
        oid = int(d.offer_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Złe ID")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == oid).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Not found")
    if offer.owner_name != user.email.split('@')[0]:
        raise HTTPException(status_code=403, detail="Brak uprawnień")
    db.delete(offer)
    db.commit()
    return {"status": "deleted"}


@app.post("/api/v1/airbnb/update")
def update_offer_details(u: AirbnbUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == u.token).first()
    if not user:
        raise HTTPException(status_code=401, detail="Auth error")
    try:
        oid = int(u.offer_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Złe ID")
    offer = db.query(AirbnbOffer).filter(AirbnbOffer.id == oid).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Not found")
    if offer.owner_name != user.email.split('@')[0]:
        raise HTTPException(status_code=403, detail="Brak uprawnień")
    offer.title = u.title
    offer.description = u.description
    offer.price = u.price
    offer.availability = u.availability
    if u.district:
        offer.district = u.district
    db.commit()
    return {"status": "updated"}


# === FIX: USER MANAGEMENT (admin view) ===
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
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    target.is_disabled = u.is_disabled
    db.commit()
    return {"status": "updated"}


@app.post("/api/v1/admin/update_permissions")
def update_user_permissions(u: UserPermissionsUpdate, db: Session = Depends(get_db)):
    target = db.query(User).filter(User.email == u.target_email).first()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    target.perm_euro = u.perm_euro
    target.perm_ev = u.perm_ev
    target.perm_disabled = u.perm_disabled
    target.is_disabled = u.perm_disabled
    db.commit()
    return {"status": "updated"}


def create_default_admins(db: Session):
    if db.query(Admin).first():
        return
    admins = [
        {"user": "admin", "pass": "admin123", "role": "ALL", "badge": "Super Admin", "perms": "ALL"},
        {"user": "euro_admin", "pass": "euro123", "role": "EURO", "badge": "Admin EURO", "perms": "VIEW_EURO"},
        {"user": "bud_admin", "pass": "bud123", "role": "BUD", "badge": "Admin BUD", "perms": "VIEW_BUD"},
    ]
    for a in admins:
        db.add(Admin(username=a["user"], password_hash=get_password_hash(a["pass"]), role=a["role"], badge_name=a["badge"], permissions=a["perms"]))
    # default groups
    for g in GRUPY_SENSOROW:
        if not db.query(GroupOption).filter(GroupOption.name == g).first():
            db.add(GroupOption(name=g, description=f"Domyślna grupa {g}"))
    # default permissions
    default_perms = ["VIEW_EURO", "VIEW_BUD", "VIEW_EV", "VIEW_DISABLED", "MANAGE_USERS"]
    for p in default_perms:
        if not db.query(PermissionOption).filter(PermissionOption.code == p).first():
            db.add(PermissionOption(code=p, description=p.replace("_", " ").title()))
    db.commit()


# --- lightweight MQTT/websocket loops are left as "best-effort" --- (kept simple)
@app.on_event("startup")
async def start():
    # start background threads if needed (mqtt omitted in detail here)
    with SessionLocal() as db:
        create_default_admins(db)


@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except Exception:
        manager.disconnect(ws)
