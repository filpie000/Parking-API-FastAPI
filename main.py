import os
import datetime
import json
import logging
import threading
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, Header, Request
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import requests
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt 

# Ustawienie loggera do diagnostyki
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===== KONFIGURACJA MQTT =====
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/tester/status") 

# === Konfiguracja Bazy Danych ===
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL, używam domyślnej bazy SQLite.")


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === Definicja Tabel ===
# (Bez zmian)
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0) 
    ostatnia_aktualizacja = Column(DateTime, default=datetime.datetime.now(datetime.UTC))

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime, index=True)
    sensor_id = Column(String, index=True)
    status = Column(Integer)

class OstatniStanBramki(Base):
    __tablename__ = "ostatni_stan_bramki"
    bramka_id = Column(String, primary_key=True, default="Bramka_A") 
    ostatni_kontakt = Column(DateTime)

class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    device_token = Column(String, primary_key=True, index=True) 
    sensor_id = Column(String, index=True)
    czas_dodania = Column(DateTime, default=datetime.datetime.now(datetime.UTC))

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

API_KEY = os.environ.get('API_KEY')
async def check_api_key(x_api_key: str = Header(None)):
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

app = FastAPI(title="Parking API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Modele Pydantic ===
class WymaganyFormat(BaseModel):
    sensor_id: str
    status: int 

class ObserwujRequest(BaseModel):
    sensor_id: str
    device_token: str

# ❗️ ZUPEŁNIE NOWY, UPROSZCZONY MODEL ZAPYTANIA O STATYSTYKI
class StatystykiZapytanie(BaseModel):
    sensor_id: Optional[str] = None
    selected_date: str  # Oczekuje stringa "YYYY-MM-DD"
    selected_hour: int  # Oczekuje int 0-23

# === Funkcje Pomocnicze ===
def send_push_notification(token: str, sensor_id: str):
    # ... (bez zmian) ...
    logger.info(f"Wysyłanie powiadomienia PUSH do: {token} dla: {sensor_id}")
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={
            "to": token, "sound": "default",
            "title": "❌ Miejsce parkingowe zajęte!",
            "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte. Kliknij, aby znaleźć nowe.",
            "data": { "sensor_id": sensor_id, "action": "reroute" } 
        })
        logger.info(f"Pomyślnie wysłano żądanie do Expo dla: {sensor_id}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Expo: {e}")

# === Endpointy ===
@app.get("/")
def read_root():
    return {"status": "Parking API działa! Słucham MQTT na: " + MQTT_TOPIC_SUBSCRIBE}

@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    # ... (bez zmian) ...
    token = request.device_token
    sensor_id = request.sensor_id
    teraz = datetime.datetime.now(datetime.UTC)
    wpis = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == token).first()
    if wpis:
        wpis.sensor_id = sensor_id
        wpis.czas_dodania = teraz
    else:
        nowy_obserwator = ObserwowaneMiejsca(
            device_token=token,
            sensor_id=sensor_id,
            czas_dodania=teraz
        )
        db.add(nowy_obserwator)
    db.commit()
    return {"status": "obserwowanie rozpoczęte", "miejsce": sensor_id}

# ❗️ GŁÓWNY ZAKTUALIZOWANY ENDPOINT STATYSTYK
@app.post("/api/v1/statystyki/zajetosc")
def pobierz_statystyki_zajetosci(zapytanie: StatystykiZapytanie, db: Session = Depends(get_db)):
    
    # 1. Definicje
    DNI_POWSZEDNIE = [0, 1, 2, 3] # Pon, Wt, Śr, Czw
    PIATEK = [4]
    WEEKEND = [5, 6] # Sob, Niedz
    HOLIDAYS = {
        "12-24": "Wigilia",
        "12-31": "Sylwester"
    }

    try:
        selected_date_obj = datetime.datetime.strptime(zapytanie.selected_date, "%Y-%m-%d").date()
        selected_hour = zapytanie.selected_hour
        selected_weekday = selected_date_obj.weekday()
    except ValueError:
        raise HTTPException(status_code=400, detail="Niepoprawny format daty. Oczekiwano YYYY-MM-DD.")
        
    # Funkcja pomocnicza do czasu
    def get_time_with_offset(base_hour, offset_minutes):
        base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
        offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
        return offset_dt.time()
    
    # 2. Inicjalizacja filtrów
    kategoria_str = ""
    dni_do_uwzglednienia = []
    czas_poczatek = None
    czas_koniec = None
    data_poczatek_query = None # Pełna data od
    data_koniec_query = None # Pełna data do

    # 3. SPRAWDZENIE LOGIKI ŚWIĄT (PUNKT 3)
    date_key = selected_date_obj.strftime("%m-%d")
    if date_key in HOLIDAYS:
        kategoria_str = f"{HOLIDAYS[date_key]} (dane z zeszłego roku)"
        
        # Ustawiamy zapytanie na ten sam dzień, ale rok wcześniej
        date_rok_wstecz = selected_date_obj.replace(year=selected_date_obj.year - 1)
        data_poczatek_query = datetime.datetime.combine(date_rok_wstecz, datetime.time(0, 0))
        data_koniec_query = datetime.datetime.combine(date_rok_wstecz, datetime.time(23, 59))
        
        # Filtrujemy tylko ten konkretny dzień
        dni_do_uwzglednienia = [data_poczatek_query.weekday()]
        
        # Na święta zawsze bierzemy +/- 1h
        czas_poczatek = get_time_with_offset(selected_hour, -60)
        czas_koniec = get_time_with_offset(selected_hour, 60)
        
    else:
        # 4. STANDARDOWA LOGIKA (PUNKT 1)
        # Ustawiamy zapytanie na wszystkie dane historyczne (bez filtra daty)
        data_poczatek_query = datetime.datetime(2000, 1, 1)
        data_koniec_query = datetime.datetime(2100, 1, 1)

        is_piatek_popoludnie = (selected_weekday == 4 and selected_hour >= 14)
        is_weekend = (selected_weekday == 5 or selected_weekday == 6)

        if is_piatek_popoludnie or is_weekend:
            # LOGIKA "WEEKENDOWA"
            czas_poczatek = get_time_with_offset(selected_hour, -60)
            czas_koniec = get_time_with_offset(selected_hour, 60)
            
            if is_piatek_popoludnie:
                kategoria_str = "Piątki po 14:00"
                dni_do_uwzglednienia = PIATEK
            elif selected_weekday == 5:
                kategoria_str = "Soboty"
                dni_do_uwzglednienia = [5]
            else:
                kategoria_str = "Niedziele"
                dni_do_uwzglednienia = [6]
        else:
            # LOGIKA "DNI POWSZEDNIE"
            czas_poczatek = get_time_with_offset(selected_hour, -30)
            czas_koniec = get_time_with_offset(selected_hour, 30)
            kategoria_str = "Dni powszednie"
            dni_do_uwzglednienia = DNI_POWSZEDNIE + PIATEK # Pn-Pt
            
    # 5. Pobranie i filtrowanie danych
    query = db.query(DaneHistoryczne).filter(
        DaneHistoryczne.sensor_id.startswith(zapytanie.sensor_id),
        DaneHistoryczne.czas_pomiaru >= data_poczatek_query,
        DaneHistoryczne.czas_pomiaru <= data_koniec_query
    )
    wszystkie_dane = query.all()
    
    dane_pasujace = []
    
    for rekord in wszystkie_dane:
        dzien_rekordu = rekord.czas_pomiaru.weekday()
        czas_rekordu = rekord.czas_pomiaru.time()
        
        if dzien_rekordu not in dni_do_uwzglednienia:
            continue
        
        # Filtrowanie czasu
        if czas_poczatek > czas_koniec: # Zawijanie o północy (np. 23:30 - 00:30)
            if not (czas_rekordu >= czas_poczatek or czas_rekordu < czas_koniec):
                continue
        else: # Normalny przedział
            if not (czas_poczatek <= czas_rekordu < czas_koniec):
                continue
        
        # Reguły specjalne
        if kategoria_str == "Dni powszednie":
            if dzien_rekordu == 4 and czas_rekordu >= datetime.time(14, 0):
                continue # Wyklucz Piątek po 14
        
        if kategoria_str == "Piątki po 14:00":
             if czas_rekordu < datetime.time(14, 0):
                continue # Wyklucz Piątek przed 14

        dane_pasujace.append(rekord.status)

    # 6. Obliczanie wyniku
    if not dane_pasujace:
        procent_zajetosci = 0
        liczba_pomiarow = 0
    else:
        zajete = dane_pasujace.count(1)
        wolne = dane_pasujace.count(0)
        suma_pomiarow = zajete + wolne
        procent_zajetosci = (zajete / suma_pomiarow) * 100 if suma_pomiarow > 0 else 0
        liczba_pomiarow = len(dane_pasujace)

    # 7. Zwracamy format odpowiedzi
    return {
        "wynik_dynamiczny": {
            "kategoria": kategoria_str,
            "przedzial_czasu": f"{czas_poczatek.strftime('%H:%M')} - {czas_koniec.strftime('%H:%M')}",
            "procent_zajetosci": round(procent_zajetosci, 1),
            "liczba_pomiarow": liczba_pomiarow
        }
    }


# === FUNKCJA GŁÓWNA PRZETWARZANIA DANYCH ===
def process_parking_update(dane_z_bramki: WymaganyFormat, db: Session, teraz: datetime.datetime):
    # ... (bez zmian) ...
    sensor_id = dane_z_bramki.sensor_id
    nowy_status = dane_z_bramki.status
    nowy_rekord_historyczny = DaneHistoryczne(
        czas_pomiaru=teraz, sensor_id=sensor_id, status=nowy_status
    )
    db.add(nowy_rekord_historyczny)
    miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    poprzedni_status = -1
    if miejsce_db:
        poprzedni_status = miejsce_db.status
        miejsce_db.status = nowy_status
        miejsce_db.ostatnia_aktualizacja = teraz
    else:
        nowe_miejsce = AktualnyStan(
            sensor_id=sensor_id, status=nowy_status, ostatnia_aktualizacja=teraz
        )
        db.add(nowe_miejsce)
    if poprzedni_status != 1 and nowy_status == 1:
        logger.info(f"Wykryto zajęcie miejsca: {sensor_id}. Sprawdzanie obserwatorów...")
        limit_czasu_obserwacji = datetime.timedelta(minutes=30)
        obserwatorzy = db.query(ObserwowaneMiejsca).filter(
            ObserwowaneMiejsca.sensor_id == sensor_id,
            (teraz - ObserwowaneMiejsca.czas_dodania) < limit_czasu_obserwacji
        ).all()
        tokeny_do_usuniecia = []
        for obserwator in obserwatorzy:
            send_push_notification(obserwator.device_token, obserwator.sensor_id)
            tokeny_do_usuniecia.append(obserwator.device_token)
            logger.info(f"Wysłano powiadomienie do tokena {obserwator.device_token[:5]}... dla miejsca {sensor_id}")
        if tokeny_do_usuniecia:
            db.query(ObserwowaneMiejsca).filter(
                ObserwowaneMiejsca.device_token.in_(tokeny_do_usuniecia)
            ).delete(synchronize_session=False)
    bramka_id_z_czujnika = sensor_id.split('_')[0] 
    bramka_db = db.query(OstatniStanBramki).filter(OstatniStanBramki.bramka_id == bramka_id_z_czujnika).first()
    if bramka_db:
        bramka_db.ostatni_kontakt = teraz
    else:
        nowa_bramka = OstatniStanBramki(bramka_id=bramka_id_z_czujnika, ostatni_kontakt=teraz)
        db.add(nowa_bramka)
    db.commit()
    return {"status": "zapisano"}

# Endpoint dla BRAMKI (HTTP Fallback)
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
async def aktualizuj_miejsce_http(request: Request, db: Session = Depends(get_db)):
    # ... (bez zmian) ...
    teraz = datetime.datetime.now(datetime.UTC)
    body_bytes = await request.body()
    raw_json_str = body_bytes.decode('latin-1').strip().replace('\x00', '')
    logger.info(f"Odebrano surowy payload (HTTP): {repr(raw_json_str)}") 
    try:
        dane = json.loads(raw_json_str)
        dane_z_bramki = WymaganyFormat(**dane) 
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.error(f"BŁĄD PARSOWANIA JSON (HTTP): {e} | Surowe dane: {repr(raw_json_str)}")
        raise HTTPException(status_code=400, detail=f"Niepoprawny format danych JSON. Szczegóły: {e}")
    return process_parking_update(dane_z_bramki, db, teraz)


# Endpoint dla APLIKACJI (Publiczny)
@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    # ... (bez zmian) ...
    teraz = datetime.datetime.now(datetime.UTC)
    limit_czasu_bramki = datetime.timedelta(minutes=15)
    bramki_offline = db.query(OstatniStanBramki).filter(
        (OstatniStanBramki.ostatni_kontakt == None) |
        ((teraz - OstatniStanBramki.ostatni_kontakt) > limit_czasu_bramki)
    ).all()
    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2})
    db.commit()
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca

# =========================================================
# === IMPLEMENTACJA KLIENTA MQTT ===
# =========================================================
# ... (bez zmian) ...
mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Klient MQTT podłączony do brokera {MQTT_BROKER}. Subskrybuję temat: {MQTT_TOPIC_SUBSCRIBE}")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"Nie udało się połączyć z MQTT, kod: {rc}")

def on_message(client, userdata, msg):
    teraz = datetime.datetime.now(datetime.UTC)
    try:
        raw_json_str = msg.payload.decode('utf-8').strip().replace('\x00', '')
        logger.info(f"ODEBRANO MQTT na temacie {msg.topic}: {repr(raw_json_str)}")
        dane = json.loads(raw_json_str)
        dane_z_bramki = WymaganyFormat(**dane) 
        db = next(get_db())
        process_parking_update(dane_z_bramki, db, teraz)
        db.close()
    except json.JSONDecodeError as e:
        logger.error(f"BŁĄD PARSOWANIA JSON (MQTT): {e} | Surowe dane: {repr(raw_json_str)}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY przetwarzania wiadomości MQTT: {e}")

def mqtt_listener_thread():
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        logger.error(f"BŁĄD połączenia z brokerem MQTT: {e}")

# === HOOKI STARTOWE I ZATRZYMUJĄCE ===
@app.on_event("startup")
async def startup_event():
    logger.info("Uruchamiam nasłuch MQTT...")
    thread = threading.Thread(target=mqtt_listener_thread)
    thread.daemon = True 
    thread.start()

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Zamykanie klienta MQTT...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
