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

class AirbnbUpdate(BaseModel):
    token: str
    offer_id: int
    title: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    h_availability: Optional[str] = None
    contact: Optional[str] = None
    district_id: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class AirbnbResponse(BaseModel):
    id: int
    title: Optional[str] = "Brak tytułu"
    description: Optional[str] = None
    price: Optional[float] = 0.0
    h_availability: Optional[str] = None
    contact: Optional[str] = None
    owner_name: Optional[str] = None
    owner_user_id: Optional[int] = None # DODANO do identyfikacji właściciela
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    state_name: Optional[str] = "Inny"
    start_date: Optional[datetime.date] = None # DODANO
    end_date: Optional[datetime.date] = None   # DODANO
    
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
    try: return bcrypt.check
