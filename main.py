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
    vehicles = relationship("Vehicle", back_populates="owner")
    tickets = relationship("Ticket", back_populates="owner")
    # Opcjonalne pole dark_mode (jeśli dodałeś do bazy)
    # dark_mode = Column(Boolean, default=False) 

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

# --- UTILS ---
def get_time_with_offset(base_hour, offset_minutes):
    base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
    offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
    return offset_dt.time()

def get_password_hash(password: str) -> str:
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pwd_bytes, salt)
    return hashed.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    try: return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except: return False

def calculate_occupancy_stats(sensor_prefix: str, selected_date_obj: date, selected_hour: int, db: Session) -> dict:
    if not sensor_prefix: sensor_prefix = "EURO"
    nazwa_swieta = MANUALNA_MAPA_SWIAT.get(selected_date_obj)
    query = None
    kategoria_str = ""
    dni_do_uwzglednienia = []

    if nazwa_swieta:
        kategoria_str = f"Święto: {nazwa_swieta}"
        query = db.query(DaneSwieta).filter(DaneSwieta.sensor_id.startswith(sensor_prefix), DaneSwieta.nazwa_swieta == nazwa_swieta)
    else:
        wd = selected_date_obj.weekday()
        if wd <= 3: kategoria_str = "Dni robocze"; dni_do_uwzglednienia = [0,1,2,3]
        elif wd == 4: kategoria_str = "Piątek"; dni_do_uwzglednienia = [4]
        else: kategoria_str = "Weekend"; dni_do_uwzglednienia = [5,6]
        query = db.query(DaneHistoryczne).filter(DaneHistoryczne.sensor_id.startswith(sensor_prefix))

    OFFSET = 60
    try:
        pocz = get_time_with_offset(selected_hour, -OFFSET)
        kon = get_time_with_offset(selected_hour, OFFSET)
    except: return {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": "Błąd czasu"}

    rows = query.all()
    pasujace = []
    for r in rows:
        if not r.czas_pomiaru: continue
        if isinstance(r.czas_pomiaru, str):
             try: czas_db = datetime.datetime.fromisoformat(r.czas_pomiaru)
             except: continue
        else: czas_db = r.czas_pomiaru
        if czas_db.tzinfo is None: czas_pl = czas_db.replace(tzinfo=PL_TZ)
        else: czas_pl = czas_db.astimezone(PL_TZ)
        if not nazwa_swieta and czas_pl.weekday() not in dni_do_uwzglednienia: continue
        t = czas_pl.time()
        match = False
        if pocz > kon: 
            if t >= pocz or t < kon: match = True
        else:
            if pocz <= t < kon: match = True
        if match: pasujace.append(r.status)

    if not pasujace: return {"procent_zajetosci": 0, "liczba_pomiarow": 0, "kategoria": kategoria_str}
    zajete = pasujace.count(1)
    return {"procent_zajetosci": round((zajete / len(pasujace)) * 100, 1), "liczba_pomiarow": len(pasujace), "kategoria": kategoria_str, "przedzial_czasu": f"{pocz} - {kon}"}

# --- APP ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class ConnectionManager:
    def __init__(self): self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections: self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        try:
            message_binary = msgpack.packb(message)
            for connection in list(self.active_connections):
                try: await connection.send_bytes(message_binary)
                except: self.disconnect(connection)
        except: pass
manager = ConnectionManager()

def send_push_notification(token, title, body, data):
    logger.info(f"PUSH TRY -> {token[:15]}... | {title}")
    try: 
        payload = {
            "to": token,
            "title": title,
            "body": body,
            "data": data,
            "sound": "default", 
            "priority": "high",
            "channelId": "parking_channel", 
            "_displayInForeground": True 
        }
        res = requests.post("https://exp.host/--/api/v2/push/send", json=payload, timeout=5)
        logger.info(f"PUSH RESULT: {res.status_code} | {res.text}") 
    except Exception as e: 
        logger.error(f"PUSH NETWORK ERROR: {e}")

async def process_parking_update(dane: dict, db: Session):
    if "sensor_id" not in dane: return
    sid = dane["sensor_id"]
    status = dane["status"]
    teraz = now_utc()
    
    m = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sid).first()
    prev_status = -1
    
    if m:
        prev_status = m.status
        m.ostatnia_aktualizacja = teraz
        m.status = status
    else:
        db.add(AktualnyStan(sensor_id=sid, status=status, ostatnia_aktualizacja=teraz))
    
    if prev_status != status:
        db.add(DaneHistoryczne(czas_pomiaru=teraz, sensor_id=sid, status=status))
        chg = {"sensor_id": sid, "status": status}
        if status == 1:
             limit = datetime.timedelta(hours=24)
             obs = db.query(ObserwowaneMiejsca).filter(
                 ObserwowaneMiejsca.sensor_id == sid, 
                 (teraz - ObserwowaneMiejsca.czas_dodania) < limit
             ).all()
             if obs:
                 grp = sid.split('_')[0]
                 wolny = db.query(AktualnyStan).filter(AktualnyStan.sensor_id.startswith(grp), AktualnyStan.sensor_id != sid, AktualnyStan.status == 0).first()
                 tytul = "❌ Miejsce zajęte!"
                 tresc = f"Miejsce {sid} zostało zajęte."
                 akcja = "reroute"
                 target = None
                 if wolny:
                     tytul = "⚠️ Zmiana miejsca"
                     tresc = f"{sid} zajęte. Jedź na {wolny.sensor_id}!"
                     akcja = "info"
                     target = wolny.sensor_id
                 for o in obs:
                     send_push_notification(o.device_token, tytul, tresc, {"action": akcja, "new_target": target})
                     db.delete(o)
        db.commit()
        if manager.active_connections: await manager.broadcast([chg])
    else:
        db.commit()

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
    token = secrets.token_hex(16)
    user.token = token
    db.commit()
    # Bezpieczny dostęp do dark_mode (jeśli kolumna nie istnieje, zwróć False)
    dm = getattr(user, 'dark_mode', False)
    return {"token": token, "email": user.email, "is_disabled": user.is_disabled, "dark_mode": dm}

@app.get("/api/v1/user/me")
def me(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401, "Auth error")
    dm = getattr(user, 'dark_mode', False)
    return {"email": user.email, "is_disabled": user.is_disabled, "dark_mode": dm, "vehicles": [{"name": v.name, "plate": v.license_plate} for v in user.vehicles]}

@app.post("/api/v1/user/status")
def status(s: StatusUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user: raise HTTPException(401, "Auth error")
    user.is_disabled = s.is_disabled
    db.commit()
    return {"status": "updated"}

@app.post("/api/v1/user/darkmode")
def update_darkmode(s: DarkModeUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == s.token).first()
    if not user: raise HTTPException(401, "Auth error")
    # Sprawdź czy model ma pole, jeśli nie - zignoruj (dla kompatybilności)
    if hasattr(user, 'dark_mode'):
        user.dark_mode = s.dark_mode
        db.commit()
    return {"status": "updated"}

@app.post("/api/v1/user/vehicle")
def add_veh(v: VehicleAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == v.token).first()
    if not user: raise HTTPException(401, "Auth error")
    db.add(Vehicle(name=v.name, license_plate=v.license_plate, user_id=user.id))
    db.commit()
    return {"status": "added"}

@app.post("/api/v1/user/ticket")
def buy_ticket(t: TicketAdd, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == t.token).first()
    if not user: raise HTTPException(401, "Auth error")
    try:
        end_time_str = t.end_time.replace('Z', '+00:00')
        end_dt = datetime.datetime.fromisoformat(end_time_str)
    except: end_dt = now_utc() + datetime.timedelta(hours=1)
    new_ticket = Ticket(user_id=user.id, sensor_id=t.sensor_id, place_name=t.place_name, plate=t.plate, start_time=now_utc(), end_time=end_dt, price=t.price)
    db.add(new_ticket)
    db.commit()
    return {"status": "ticket_created", "id": new_ticket.id}

@app.get("/api/v1/user/ticket/active")
def get_active_ticket(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.token == token).first()
    if not user: raise HTTPException(401, "Auth error")
    teraz = now_utc()
    ticket = db.query(Ticket).filter(Ticket.user_id == user.id, Ticket.end_time > teraz).order_by(Ticket.end_time.desc()).first()
    if ticket:
        return {"placeName": ticket.place_name, "sensorId": ticket.sensor_id, "plate": ticket.plate, "startTime": ticket.start_time.isoformat(), "endTime": ticket.end_time.isoformat(), "price": ticket.price}
    return None

@app.get("/api/v1/aktualny_stan")
def get_stat(limit: int = 100, db: Session = Depends(get_db)):
    return [m.to_dict() for m in db.query(AktualnyStan).limit(limit).all()]

@app.get("/api/v1/prognoza/wszystkie_miejsca")
def forecast(target_date: Optional[str] = None, target_hour: Optional[int] = None, db: Session = Depends(get_db)):
    teraz_pl = now_utc().astimezone(PL_TZ)
    if target_date and target_hour is not None:
        try: dt = datetime.datetime.strptime(target_date, "%Y-%m-%d").date(); hour = int(target_hour)
        except: dt = teraz_pl.date(); hour = teraz_pl.hour
    else: dt = teraz_pl.date(); hour = teraz_pl.hour
    prognozy = {}
    for grupa in GRUPY_SENSOROW:
        try:
            wynik = calculate_occupancy_stats(grupa, dt, hour, db)
            prognozy[grupa] = wynik["procent_zajetosci"]
        except: prognozy[grupa] = 0.0
    return prognozy

@app.post("/api/v1/statystyki/zajetosc")
def stats(z: StatystykiZapytanie, db: Session = Depends(get_db)):
    key = f"stats:{z.sensor_id}:{z.selected_date}:{z.selected_hour}"
    if redis_client:
        c = redis_client.get(key)
        if c: return {"wynik_dynamiczny": json.loads(c)}
    try: dt = datetime.datetime.strptime(z.selected_date, "%Y-%m-%d").date()
    except: raise HTTPException(400, "Zła data")
    wynik = calculate_occupancy_stats(z.sensor_id, dt, z.selected_hour, db)
    if redis_client: redis_client.set(key, json.dumps(wynik), ex=3600)
    return {"wynik_dynamiczny": wynik}

@app.post("/api/v1/obserwuj_miejsce")
def obs(r: ObserwujRequest, db: Session = Depends(get_db)):
    logger.info(f"OBSERWACJA: Rejestruję token {r.device_token[:10]}... dla {r.sensor_id}")
    istniejacy = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == r.device_token).first()
    if istniejacy:
        istniejacy.sensor_id = r.sensor_id
        istniejacy.czas_dodania = now_utc()
    else:
        db.add(ObserwowaneMiejsca(device_token=r.device_token, sensor_id=r.sensor_id))
    try:
        db.commit()
        return {"status": "ok"}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Błąd bazy: {e}")

# === FIX: STRUMIENIOWE POBIERANIE DANYCH (ZMNIEJSZA RAM) ===
@app.post("/api/v1/dashboard/raport")
def rep(r: RaportRequest, request: Request, db: Session = Depends(get_db)):
    client_ip = request.client.host
    limit_key = f"ratelimit:report:{client_ip}"
    if redis_client:
        current = redis_client.get(limit_key)
        if current and int(current) >= 2:
            ttl = redis_client.ttl(limit_key)
            raise HTTPException(429, f"Limit zapytań. Czekaj {ttl}s.")
        pipe = redis_client.pipeline()
        pipe.incr(limit_key)
        if not current: pipe.expire(limit_key, 60)
        pipe.execute()
    
    try:
        s_date = datetime.datetime.strptime(r.start_date, "%Y-%m-%d").date()
        e_date = datetime.datetime.strptime(r.end_date, "%Y-%m-%d").date()
    except: raise HTTPException(400, "Zła data")
    
    # Używamy słownika do agregacji w locie (zamiast listy obiektów)
    agg = {g: {h: [] for h in range(24)} for g in r.groups}
    
    # Krok 1: Dane Historyczne (Partiami)
    if r.include_workdays or r.include_weekends:
        query = db.query(DaneHistoryczne.sensor_id, DaneHistoryczne.status, DaneHistoryczne.czas_pomiaru).filter(
            DaneHistoryczne.czas_pomiaru >= s_date, 
            DaneHistoryczne.czas_pomiaru <= e_date + datetime.timedelta(days=1)
        ).yield_per(1000) # Pobieraj po 1000

        for sid, stat, czas in query:
            if not czas: continue
            local_time = czas.replace(tzinfo=UTC).astimezone(PL_TZ) if czas.tzinfo is None else czas.astimezone(PL_TZ)
            
            is_weekend = local_time.weekday() >= 5
            if is_weekend and not r.include_weekends: continue
            if not is_weekend and not r.include_workdays: continue
            
            try:
                group = sid.split('_')[0]
                if group in agg:
                    agg[group][local_time.hour].append(stat)
            except: pass

    # Krok 2: Dane Świąteczne (Partiami)
    if r.include_holidays:
        query_hol = db.query(DaneSwieta.sensor_id, DaneSwieta.status, DaneSwieta.czas_pomiaru).filter(
            DaneSwieta.czas_pomiaru >= s_date, 
            DaneSwieta.czas_pomiaru <= e_date + datetime.timedelta(days=1)
        ).yield_per(1000)

        for sid, stat, czas in query_hol:
             if not czas: continue
             local_time = czas.replace(tzinfo=UTC).astimezone(PL_TZ) if czas.tzinfo is None else czas.astimezone(PL_TZ)
             try:
                group = sid.split('_')[0]
                if group in agg:
                    agg[group][local_time.hour].append(stat)
             except: pass

    # Wyniki
    result = {}
    for g in r.groups:
        data_points = []
        for h in range(24):
            values = agg[g][h]
            if not values: data_points.append(0)
            else: data_points.append(round((sum(values) / len(values)) * 100, 1))
        result[g] = data_points
        
    return result

@app.post("/api/v1/debug/symulacja_sensora")
async def debug(d: dict, db: Session = Depends(get_db)):
    await process_parking_update(d, db)
    return {"status": "ok"}

mqtt_c = mqtt.Client()
def mqtt_loop():
    mqtt_c.on_connect = lambda c,u,f,r: c.subscribe(MQTT_TOPIC_SUBSCRIBE)
    def on_m(c,u,m):
        try:
            sid_str = SENSOR_MAP.get((m.payload[0]<<8)|m.payload[1])
            if sid_str:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                with SessionLocal() as db:
                    loop.run_until_complete(process_parking_update({"sensor_id": sid_str, "status": int(m.payload[2])}, db))
        except: pass
    mqtt_c.on_message = on_m
    try: mqtt_c.connect(MQTT_BROKER, MQTT_PORT, 60); mqtt_c.loop_forever()
    except: pass

def check_stale():
    while not threading.main_thread().is_alive() is False:
        try:
            db = SessionLocal()
            cut = now_utc() - datetime.timedelta(minutes=5)
            old = db.query(AktualnyStan).filter(AktualnyStan.status != 2, (AktualnyStan.ostatnia_aktualizacja < cut)|(AktualnyStan.ostatnia_aktualizacja == None)).all()
            if old:
                chg = []
                for s in old:
                    s.status = 2
                    s.ostatnia_aktualizacja = now_utc()
                    chg.append(s.to_dict())
                db.commit()
                if chg and manager.active_connections:
                    asyncio.run_coroutine_threadsafe(manager.broadcast(chg), asyncio.get_event_loop())
            db.close()
        except: pass
        time.sleep(60)

@app.on_event("startup")
async def start(): 
    threading.Thread(target=mqtt_loop, daemon=True).start()
    threading.Thread(target=check_stale, daemon=True).start()

@app.on_event("shutdown")
def stop(): mqtt_c.disconnect()

@app.websocket("/ws/stan")
async def ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True: await ws.receive_text()
    except: manager.disconnect(ws)
