import os
import datetime
import logging
import secrets
import json
from typing import Optional, List
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Depends, HTTPException, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Text, Table, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

import bcrypt
import msgpack
import paho.mqtt.client as mqtt
import requests

# --- LOGOWANIE ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)

# --- MQTT ---
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
    pool_recycle=3600
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- POPRAWIONA FUNKCJA GET_DB ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- MODELE ---
spot_group_members = Table('spot_group_members', Base.metadata,
    Column('spot_name', String, ForeignKey('parking_spots.name', ondelete="CASCADE"), primary_key=True),
    Column('group_id', Integer, ForeignKey('groups.id', ondelete="CASCADE"), primary_key=True)
)

class Group(Base):
    __tablename__ = 'groups'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    spots = relationship("ParkingSpot", secondary=spot_group_members, back_populates="groups")

class ParkingSpot(Base):
    __tablename__ = "parking_spots"
    name = Column(String, primary_key=True) 
    current_status = Column(Integer, default=0) 
    last_seen = Column(DateTime(timezone=True), default=now_utc)
    city = Column(String, nullable=True); state = Column(String, nullable=True)
    category = Column(String, default='public_spot'); coordinates = Column(String, nullable=True)
    is_disabled_friendly = Column(Boolean, default=False); is_ev = Column(Boolean, default=False); is_paid = Column(Boolean, default=True)
    groups = relationship("Group", secondary=spot_group_members, back_populates="spots")

class Admin(Base):
    __tablename__ = "admins"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, index=True); password_hash = Column(String); badge_name = Column(String, default="Admin")
    permissions = relationship("AdminPermissions", back_populates="admin", uselist=False, cascade="all, delete-orphan")

class AdminPermissions(Base):
    __tablename__ = "admin_permissions"
    admin_id = Column(Integer, ForeignKey("admins.id"), primary_key=True)
    city = Column(String, default='ALL'); view_disabled_only = Column(Boolean, default=False); view_ev_only = Column(Boolean, default=False); view_paid_only = Column(Boolean, default=False); allowed_states = Column(Text, nullable=True)
    admin = relationship("Admin", back_populates="permissions")

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime(timezone=True), default=now_utc, index=True)
    spot_name = Column(String, index=True); status = Column(Integer)

# NOWY MODEL OBSERWACJI (Z ID jako kluczem)
class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_token = Column(String, index=True)
    sensor_id = Column(String, index=True)
    czas_dodania = Column(DateTime(timezone=True), default=now_utc)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True); email = Column(String, unique=True, index=True); password_hash = Column(String); token = Column(String, index=True, nullable=True)
    is_disabled = Column(Boolean, default=False); is_disabled_person = Column(Boolean, default=False); dark_mode = Column(Boolean, default=False); active_ticket_id = Column(Integer, ForeignKey("tickets.id"), nullable=True)
    vehicles = relationship("Vehicle", back_populates="owner", cascade="all, delete-orphan")
    tickets = relationship("Ticket", back_populates="owner", foreign_keys="[Ticket.user_id]")

class Vehicle(Base):
    __tablename__ = "vehicles"
    id = Column(Integer, primary_key=True, autoincrement=True); name = Column(String, nullable=True); plate_number = Column(String); user_id = Column(Integer, ForeignKey("users.id")); owner = relationship("User", back_populates="vehicles")

class Ticket(Base):
    __tablename__ = "tickets"
    id = Column(Integer, primary_key=True, autoincrement=True); user_id = Column(Integer, ForeignKey("users.id")); vehicle_id = Column(Integer, ForeignKey("vehicles.id")); spot_name = Column(String, ForeignKey("parking_spots.name"))
    start_time = Column(DateTime(timezone=True), default=now_utc); end_time = Column(DateTime(timezone=True), nullable=True); status = Column(String, default='ACTIVE'); price = Column(Float, default=0.0)
    owner = relationship("User", back_populates="tickets", foreign_keys=[user_id]); vehicle = relationship("Vehicle")

class AirbnbOffer(Base):
    __tablename__ = "airbnb_offers"
    id = Column(Integer, primary_key=True, autoincrement=True); title = Column(String); description = Column(String); price = Column(String); availability = Column(String); owner_name = Column(String); contact = Column(String); latitude = Column(Float, nullable=True); longitude = Column(Float, nullable=True); district = Column(String, nullable=True); start_date = Column(String, nullable=True); end_date = Column(String, nullable=True); created_at = Column(DateTime(timezone=True), default=now_utc)

Base.metadata.create_all(bind=engine)

# --- PYDANTIC ---
class AdminLogin(BaseModel): username: str; password: str
class AdminPayload(BaseModel): id: Optional[int]=None; username: str; password: Optional[str]=None; city: str="ALL"; allowed_states: str=""; view_disabled_only: bool=False; view_ev_only: bool=False; view_paid_only: bool=False
class AdminDelete(BaseModel): target_id: int
class UserAuth(BaseModel): email: str; password: str
class UserToggle(BaseModel): target_email: str; is_disabled: bool
class VehicleAdd(BaseModel): token: str; name: str; license_plate: str
class TicketAdd(BaseModel): token: str; spot_name: str; vehicle_id: int; price: float = 0.0
class RaportRequest(BaseModel): start_date: str; end_date: str; groups: List[str]
class AirbnbAdd(BaseModel): token: str; title: str; description: str; price: str; availability: str; latitude: Optional[float]; longitude: Optional[float]; district: str; start_date: str; end_date: str
class AirbnbDelete(BaseModel): token: str; offer_id: int
class UserPermissionsUpdate(BaseModel): target_email: str; perm_disabled: bool
class ObserwujRequest(BaseModel): sensor_id: str; device_token: str
class StatystykiZapytanie(BaseModel): sensor_id: str; selected_date: str; selected_hour: int

# --- UTILS ---
def get_password_hash(p: str) -> str: return bcrypt.hashpw(p.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
def verify_password(p: str, h: str): return bcrypt.checkpw(p.encode('utf-8'), h.encode('utf-8')) if h else False

def send_push(token, title, body):
    try:
        import requests
        requests.post("https://exp.host/--/api/v2/push/send", json={"to": token, "title": title, "body": body}, timeout=2)
        logger.info(f"PUSH SENT: {token} - {title}")
    except Exception as e:
        logger.error(f"PUSH ERROR: {e}")

# --- WEBSOCKET ---
class ConnectionManager:
    def __init__(self): self.active_connections = []
    async def connect(self, ws: WebSocket): await ws.accept(); self.active_connections.append(ws)
    def disconnect(self, ws: WebSocket): 
        if ws in self.active_connections: self.active_connections.remove(ws)
    async def broadcast(self, msg: dict):
        try:
            bin_msg = msgpack.packb(msg)
            for c in list(self.active_connections):
                try: await c.send_bytes(bin_msg)
                except: self.disconnect(c)
        except: pass
manager = ConnectionManager()

# --- MQTT ---
def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        name = payload.get("name") or payload.get("sensor_id")
        status = int(payload.get("status", 0))
        if not name: return
        
        with SessionLocal() as db:
            spot = db.query(ParkingSpot).filter(ParkingSpot.name == name).first()
            if spot:
                prev = spot.current_status
                spot.current_status = status
                spot.last_seen = now_utc()
                if prev != status:
                    db.add(DaneHistoryczne(spot_name=name, status=status))
                    if status == 0:
                        obs = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.sensor_id == name).all()
                        for o in obs:
                            send_push(o.device_token, "Wolne Miejsce!", f"{name} jest wolne!")
                            db.delete(o)
                db.commit()
    except: pass

def start_mqtt():
    try:
        c = mqtt.Client()
        c.on_message = on_mqtt_message
        c.connect(MQTT_BROKER, MQTT_PORT, 60)
        c.subscribe(MQTT_TOPIC)
        c.loop_start()
    except: pass

# --- APP ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup():
    start_mqtt()
    with SessionLocal() as db:
        try:
            if not db.query(Admin).first():
                db.add(Admin(username="admin", password_hash=get_password_hash("admin123"), badge_name="Super Admin"))
                db.commit()
        except: pass

@app.get("/")
def root(): return {"status": "OK"}

@app.get("/dashboard", response_class=HTMLResponse)
def dash():
    try: return open("dashboard.html", "r", encoding="utf-8").read()
    except: return "Error"

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try: 
        while True: await ws.receive_text()
    except: manager.disconnect(ws)

# --- ENDPOINTS ---
@app.get("/api/v1/options/filters")
def opts(db: Session=Depends(get_db)):
    return {"cities": [c[0] for c in db.query(ParkingSpot.city).distinct().all() if c[0]], "states": [g.name for g in db.query(Group.name).all()]}

@app.get("/api/v1/aktualny_stan")
def status(limit: int=100, db: Session=Depends(get_db)):
    res = []
    for s in db.query(ParkingSpot).limit(limit).all():
        co = None
        if s.coordinates and ',' in s.coordinates:
            try: p=s.coordinates.split(','); co={"latitude":float(p[0]),"longitude":float(p[1])}
            except: pass
        res.append({"sensor_id": s.name, "name": s.name, "status": s.current_status, "groups": [g.name for g in s.groups], "city": s.city, "state": s.state, "wspolrzedne": co, "is_disabled_friendly": s.is_disabled_friendly, "is_ev": s.is_ev, "is_paid": s.is_paid, "adres": f"{s.state or ''}, {s.city or ''}".strip(', '), "typ": 'niepelnosprawni' if s.is_disabled_friendly else ('ev' if s.is_ev else 'zwykle'), "cennik": "Płatny" if s.is_paid else "Bezpłatny"})
    return res

# --- OBSERWACJA (NAPRAWIONA) ---
@app.post("/api/v1/obserwuj_miejsce")
def obs(r: ObserwujRequest, db: Session=Depends(get_db)):
    logger.info(f"ZAPIS OBSERWACJI: {r.sensor_id} -> {r.device_token}")
    
    # Sprawdź czy już istnieje
    exists = db.query(ObserwowaneMiejsca).filter(
        ObserwowaneMiejsca.device_token == r.device_token,
        ObserwowaneMiejsca.sensor_id == r.sensor_id
    ).first()
    
    if not exists:
        new_obs = ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id)
        db.add(new_obs)
        try:
            db.commit()
            logger.info("SUKCES: Zapisano w bazie.")
        except Exception as e:
            db.rollback()
            logger.error(f"BLAD ZAPISU: {e}")
            raise HTTPException(500, str(e))
            
    return {"status": "registered"}

@app.post("/api/v1/statystyki/zajetosc")
def stats_mobile(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    try: target = datetime.datetime.strptime(z.selected_date, "%Y-%m-%d").date()
    except: raise HTTPException(400, "Format")
    
    start = datetime.datetime.combine(target, datetime.time(z.selected_hour, 0))
    end = start + datetime.timedelta(hours=1)
    
    # Używamy spot_name, ignorujemy wielkość liter
    total = db.query(DaneHistoryczne).filter(func.lower(DaneHistoryczne.spot_name) == z.sensor_id.lower(), DaneHistoryczne.czas_pomiaru >= start, DaneHistoryczne.czas_pomiaru < end).count()
    occ = db.query(DaneHistoryczne).filter(func.lower(DaneHistoryczne.spot_name) == z.sensor_id.lower(), DaneHistoryczne.czas_pomiaru >= start, DaneHistoryczne.czas_pomiaru < end, DaneHistoryczne.status == 1).count()
    
    pct = int((occ/total)*100) if total > 0 else 0
    return {"procent_zajetosci": pct, "liczba_pomiarow": total}

@app.post("/api/v1/iot/update")
async def iot(d: dict, db: Session=Depends(get_db)):
    name = d.get("name"); status = int(d.get("status"))
    s = db.query(ParkingSpot).filter(ParkingSpot.name==name).first()
    if s:
        prev = s.current_status; s.current_status = status; s.last_seen = now_utc()
        if prev != status:
            db.add(DaneHistoryczne(spot_name=name, status=status))
            if status == 0: 
                obs = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.sensor_id == name).all()
                for o in obs:
                    send_push(o.device_token, "Wolne Miejsce!", f"{name} jest wolne.")
                    db.delete(o)
            await manager.broadcast({"sensor_id": name, "status": status})
        db.commit()
    return {"status": "ok"}

@app.post("/api/v1/dashboard/raport")
def report(r: RaportRequest, db: Session = Depends(get_db)):
    try: s = datetime.datetime.strptime(r.start_date, "%Y-%m-%d"); e = datetime.datetime.strptime(r.end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
    except: return {}
    targets = [sp.name for sp in db.query(ParkingSpot).join(ParkingSpot.groups).filter(Group.name.in_(r.groups)).all()]
    if not targets: return {}
    hist = db.query(DaneHistoryczne).filter(DaneHistoryczne.czas_pomiaru >= s, DaneHistoryczne.czas_pomiaru < e, DaneHistoryczne.spot_name.in_(targets)).all()
    res = {g:[0]*24 for g in r.groups}
    map_s_g = {sp.name: [g.name for g in sp.groups] for sp in db.query(ParkingSpot).filter(ParkingSpot.name.in_(targets)).all()}
    for h in hist:
        for g in map_s_g.get(h.spot_name, []):
            if g in res and h.status == 1: res[g][h.czas_pomiaru.astimezone(PL_TZ).hour] += 1
    for g in res:
        mx = max(res[g]) if res[g] else 1
        res[g] = [round((x/mx)*100, 1) if mx>0 else 0 for x in res[g]]
    return res

@app.get("/api/v1/airbnb/offers")
def airbnb_ls(db: Session=Depends(get_db)):
    return [{"id": o.id, "title": o.title, "price": o.price, "district": o.district} for o in db.query(AirbnbOffer).all()]

@app.post("/api/v1/airbnb/add")
def airbnb_mk(a: AirbnbAdd, db: Session=Depends(get_db)):
    u = db.query(User).filter(User.token==a.token).first()
    if u: db.add(AirbnbOffer(title=a.title, description=a.description, price=a.price, owner_name=u.email)); db.commit()
    return {"status":"ok"}

@app.post("/api/v1/admin/auth")
def alogin(d: AdminLogin, db: Session=Depends(get_db)):
    a = db.query(Admin).filter(Admin.username==d.username).first()
    if not a or not verify_password(d.password, a.password_hash): raise HTTPException(401)
    perms = {"city": a.permissions.city} if a.permissions else {}
    return {"username": a.username, "is_superadmin": a.username=='admin', "permissions": perms}

@app.get("/api/v1/admin/list")
def alist(db: Session=Depends(get_db)): return [{"username": a.username} for a in db.query(Admin).all()]

@app.post("/api/v1/auth/login")
def ulogin(u: UserAuth, db: Session=Depends(get_db)):
    usr = db.query(User).filter(User.email==u.email).first()
    if not usr or not verify_password(u.password, usr.password_hash): raise HTTPException(401)
    tok = secrets.token_hex(16); usr.token = tok; db.commit()
    return {"token": tok, "email": usr.email, "is_disabled": usr.is_disabled}

@app.post("/api/v1/auth/register")
def ureg(u: UserAuth, db: Session=Depends(get_db)):
    if db.query(User).filter(User.email==u.email).first(): raise HTTPException(400)
    db.add(User(email=u.email, password_hash=get_password_hash(u.password))); db.commit(); return {"status":"ok"}

@app.get("/api/v1/user/me")
def ume(token: str, db: Session=Depends(get_db)):
    u = db.query(User).filter(User.token==token).first()
    return {"email":u.email, "is_disabled":u.is_disabled, "vehicles": [{"id":v.id, "plate":v.plate_number} for v in u.vehicles]} if u else {}

@app.post("/api/v1/user/vehicle")
def veh(v: VehicleAdd, db: Session=Depends(get_db)):
    u = db.query(User).filter(User.token==v.token).first()
    if u: db.add(Vehicle(name=v.name, plate_number=v.license_plate, user_id=u.id)); db.commit()
    return {"status":"ok"}

@app.post("/api/v1/user/ticket")
def tic(t: TicketAdd, db: Session=Depends(get_db)):
    u = db.query(User).filter(User.token==t.token).first()
    s = db.query(ParkingSpot).filter(ParkingSpot.name==t.spot_name).first()
    if u and s:
        nt = Ticket(user_id=u.id, vehicle_id=t.vehicle_id, spot_name=t.spot_name, price=t.price)
        db.add(nt); db.flush()
        u.active_ticket_id = nt.id; s.current_status = 1; s.last_seen = now_utc()
        db.add(DaneHistoryczne(spot_name=s.name, status=1)); db.commit()
    return {"status":"ok"}

@app.get("/api/v1/user/ticket/active")
def actic(token: str, db: Session=Depends(get_db)):
    u = db.query(User).filter(User.token==token).first()
    if u and u.active_ticket_id:
        t = db.query(Ticket).filter(Ticket.id==u.active_ticket_id).first()
        return {"id": t.id, "spot_name": t.spot_name}
    return None

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
