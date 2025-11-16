import os
import datetime
import json
import logging
import threading
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from datetime import date
from fastapi import FastAPI, Depends, HTTPException, Header, Request
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import requests
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === KONFIGURACJA STREF CZASOWYCH ===
UTC = datetime.timezone.utc
PL_TZ = ZoneInfo("Europe/Warsaw")

# Konfiguracja MQTT
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.emqx.io')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC_SUBSCRIBE = os.environ.get('MQTT_TOPIC', "parking/tester/status")

# Konfiguracja bazy danych
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL, używam domyślnej bazy SQLite.")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === STAŁE APLIKACJI ===
GRUPY_SENSOROW = ["EURO", "BUD"]

# === RĘCZNA MAPA ŚWIĄT ===
MANUALNA_MAPA_SWIAT = {
    # ... (mapa świąt bez zmian) ...
    date(2024, 11, 1): "Wszystkich Świętych",
    date(2024, 11, 11): "Święto Niepodległości",
    date(2024, 12, 24): "Wigilia",
    date(2024, 12, 25): "Boże Narodzenie",
    date(2024, 12, 26): "Drugi Dzień Świąt",
    date(2024, 12, 31): "Sylwester",
    date(2025, 1, 1): "Nowy Rok",
}


# === Definicje tabel (Oryginalna, "naiwna" struktura) ===

class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0)
    # Ta kolumna będzie "naiwna", ale będziemy w niej zapisywać czas UTC
    ostatnia_aktualizacja = Column(DateTime, default=datetime.datetime.utcnow)

class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime, index=True) 
    sensor_id = Column(String, index=True)
    status = Column(Integer)

class DaneSwieta(Base):
    __tablename__ = "dane_swieta"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime, index=True) 
    sensor_id = Column(String, index=True)
    status = Column(Integer)
    nazwa_swieta = Column(String, index=True) 

class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    device_token = Column(String, primary_key=True, index=True)
    sensor_id = Column(String, index=True)
    czas_dodania = Column(DateTime, default=datetime.datetime.utcnow)

# =======================================================

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

class StatystykiZapytanie(BaseModel):
    sensor_id: Optional[str] = None
    selected_date: str
    selected_hour: int

# === Funkcje Pomocnicze ===
def send_push_notification(token: str, sensor_id: str):
    logger.info(f"Wysyłanie powiadomienia PUSH do: {token} dla: {sensor_id}")
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={
            "to": token, "sound": "default",
            "title": "❌ Miejsce parkingowe zajęte!",
            "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte. Kliknij, aby znaleźć nowe.",
            "data": { "sensor_id": sensor_id, "action": "reroute" } 
        })
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Expo: {e}")

def get_time_with_offset(base_hour, offset_minutes):
    base_dt = datetime.datetime(2000, 1, 1, base_hour, 0)
    offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
    return offset_dt.time()

# === WEWNĘTRZNA FUNKCJA DO OBLICZANIA STATYSTYK ===
def calculate_occupancy_stats(sensor_prefix: str, selected_date_obj: datetime.date, selected_hour: int, db: Session) -> dict:
    
    nazwa_swieta = MANUALNA_MAPA_SWIAT.get(selected_date_obj)
    query = None
    kategoria_str = ""
    dni_do_uwzglednienia = [] 

    if nazwa_swieta:
        kategoria_str = f"Dane historyczne dla: {nazwa_swieta}"
        query = db.query(DaneSwieta).filter(
            DaneSwieta.sensor_id.startswith(sensor_prefix),
            DaneSwieta.nazwa_swieta == nazwa_swieta
        )
    else:
        selected_weekday = selected_date_obj.weekday()
        if 0 <= selected_weekday <= 3: 
            kategoria_str = "Dane historyczne dla: Dni robocze (Pon-Czw)"
            dni_do_uwzglednienia = [0, 1, 2, 3]
        elif selected_weekday == 4:
            kategoria_str = "Dane historyczne dla: Piątek"
            dni_do_uwzglednienia = [4]
        elif selected_weekday == 5:
            kategoria_str = "Dane historyczne dla: Sobota"
            dni_do_uwzglednienia = [5]
        elif selected_weekday == 6:
            kategoria_str = "Dane historyczne dla: Niedziela"
            dni_do_uwzglednienia = [6]
        query = db.query(DaneHistoryczne).filter(
            DaneHistoryczne.sensor_id.startswith(sensor_prefix)
        )

    OFFSET_MINUT = 60
    czas_poczatek = get_time_with_offset(selected_hour, -OFFSET_MINUT)
    czas_koniec = get_time_with_offset(selected_hour, OFFSET_MINUT)

    wszystkie_dane_dla_sensora = query.all()
    dane_pasujace = []
    
    for rekord in wszystkie_dane_dla_sensora:
        czas_rekordu_dt_utc = rekord.czas_pomiaru
        if czas_rekordu_dt_utc.tzinfo is None:
            czas_rekordu_dt_utc = czas_rekordu_dt_utc.replace(tzinfo=UTC)
        
        czas_rekordu_dt_pl = czas_rekordu_dt_utc.astimezone(PL_TZ)

        if not nazwa_swieta:
            rekord_weekday = czas_rekordu_dt_pl.weekday()
            if rekord_weekday not in dni_do_uwzglednienia:
                continue
        
        czas_rekordu = czas_rekordu_dt_pl.time()
        
        if czas_poczatek > czas_koniec:
            if not (czas_rekordu >= czas_poczatek or czas_rekordu < czas_koniec):
                continue
        else:
            if not (czas_poczatek <= czas_rekordu < czas_koniec):
                continue
        dane_pasujace.append(rekord.status)

    if not dane_pasujace:
        procent_zajetosci = 0
        liczba_pomiarow = 0
    else:
        zajete = dane_pasujace.count(1)
        wolne = dane_pasujace.count(0)
        suma_pomiarow = zajete + wolne
        procent_zajetosci = (zajete / suma_pomiarow) * 100 if suma_pomiarow > 0 else 0
        liczba_pomiarow = len(dane_pasujace)

    return {
        "kategoria": kategoria_str,
        "przedzial_czasu": f"{czas_poczatek.strftime('%H:%M')} - {czas_koniec.strftime('%H:%M')}",
        "procent_zajetosci": round(procent_zajetosci, 1),
        "liczba_pomiarow": liczba_pomiarow
    }


# === Endpointy ===
@app.get("/")
def read_root():
    return {"status": "Parking API działa! Słucham MQTT na: " + MQTT_TOPIC_SUBSCRIBE}

@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    token = request.device_token
    sensor_id = request.sensor_id
    teraz = datetime.datetime.utcnow() # POPRAWKA: Używamy naiwnego UTC
    
    miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    
    if not miejsce_db or miejsce_db.status != 0:
        logger.warning(f"Odrzucono obserwację dla {sensor_id}. Stan: {miejsce_db.status if miejsce_db else 'Nieznany'}")
        raise HTTPException(status_code=409, detail="Niestety, to miejsce zostało właśnie zajęte lub jest offline.")
    
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
    logger.info(f"Rozpoczęto obserwację dla {sensor_id}")
    return {"status": "obserwowanie rozpoczęte", "miejsce": sensor_id}


@app.post("/api/v1/statystyki/zajetosc")
def pobierz_statystyki_zajetosci(zapytanie: StatystykiZapytanie, db: Session = Depends(get_db)):
    try:
        selected_date_obj = datetime.datetime.strptime(zapytanie.selected_date, "%Y-%m-%d").date()
        selected_hour = zapytanie.selected_hour
    except ValueError:
        raise HTTPException(status_code=400, detail="Niepoprawny format daty. Oczekiwano YYYY-MM-DD.")
    if not zapytanie.sensor_id:
        raise HTTPException(status_code=400, detail="Wymagany jest sensor_id (np. 'EURO' lub 'BUD').")
    try:
        wynik = calculate_occupancy_stats(zapytanie.sensor_id, selected_date_obj, selected_hour, db)
    except Exception as e:
        logger.error(f"Błąd przy /statystyki/zajetosc dla {zapytanie.sensor_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Wewnętrzny błąd serwera podczas obliczeń: {e}")
    return {"wynik_dynamiczny": wynik}

@app.get("/api/v1/prognoza/wszystkie_miejsca")
def pobierz_prognoze_dla_wszystkich(
    db: Session = Depends(get_db), 
    target_date: Optional[str] = None, 
    target_hour: Optional[int] = None
):
    prognozy: Dict[str, float] = {}
    selected_date_obj = None
    selected_hour = None
    use_fallback = True 

    if target_date and target_hour is not None:
        try:
            selected_hour_int = int(target_hour) 
            selected_date_obj_parsed = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
            selected_date_obj = selected_date_obj_parsed
            selected_hour = selected_hour_int
            use_fallback = False 
        except (ValueError, TypeError):
            use_fallback = True

    if use_fallback:
        teraz_pl = datetime.datetime.now(PL_TZ)
        selected_date_obj = teraz_pl.date()
        selected_hour = teraz_pl.hour

    for grupa in GRUPY_SENSOROW:
        try:
            wynik = calculate_occupancy_stats(grupa, selected_date_obj, selected_hour, db)
            prognozy[grupa] = wynik['procent_zajetosci']
        except Exception as e:
            logger.error(f"Błąd podczas obliczania prognozy dla grupy {grupa}: {e}")
            prognozy[grupa] = 0.0
    return prognozy


# === UPROSZCZONA FUNKCJA PRZETWARZANIA DANYCH ===
def process_parking_update(dane_z_bramki: dict, db: Session, teraz_utc_naiwny: datetime.datetime):
    
    # === SPRAWDŹ CZY TO WIADOMOŚĆ OD CZUJNIKA ===
    if "sensor_id" in dane_z_bramki:
        try:
            dane_czujnika = WymaganyFormat(**dane_z_bramki)
            sensor_id = dane_czujnika.sensor_id
            nowy_status = dane_czujnika.status
        except Exception as e: 
            logger.error(f"Błąd walidacji Pydantic dla wiadomości z czujnika: {e} | Dane: {dane_z_bramki}")
            return {"status": "błąd walidacji czujnika"}

        # Logika zapisu danych historycznych
        # Konwertujemy 'teraz_utc_naiwny' na 'świadomy', aby poprawnie obliczyć czas PL
        teraz_utc_swiadomy = teraz_utc_naiwny.replace(tzinfo=UTC)
        teraz_pl_date = teraz_utc_swiadomy.astimezone(PL_TZ).date()
        nazwa_swieta = MANUALNA_MAPA_SWIAT.get(teraz_pl_date)
        
        if nazwa_swieta:
            nowy_rekord = DaneSwieta(
                czas_pomiaru=teraz_utc_naiwny, sensor_id=sensor_id, 
                status=nowy_status, nazwa_swieta=nazwa_swieta
            )
        else:
            nowy_rekord = DaneHistoryczne(
                czas_pomiaru=teraz_utc_naiwny, sensor_id=sensor_id, status=nowy_status
            )
        db.add(nowy_rekord)
        
        # Logika aktualizacji AktualnyStan (UPSERT)
        miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
        poprzedni_status = -1
        if miejsce_db:
            poprzedni_status = miejsce_db.status
            miejsce_db.status = nowy_status
            miejsce_db.ostatnia_aktualizacja = teraz_utc_naiwny
        else:
            nowe_miejsce = AktualnyStan(
                sensor_id=sensor_id, status=nowy_status, ostatnia_aktualizacja=teraz_utc_naiwny
            )
            db.add(nowe_miejsce)
            
        # Logika powiadomień PUSH
        if poprzedni_status != 1 and nowy_status == 1:
            logger.info(f"Wykryto zajęcie miejsca: {sensor_id}. Sprawdzanie obserwatorów...")
            limit_czasu_obserwacji = datetime.timedelta(minutes=30)
            obserwatorzy = db.query(ObserwowaneMiejsca).filter(
                ObserwowaneMiejsca.sensor_id == sensor_id,
                # Porównujemy dwa naiwne czasy UTC
                (teraz_utc_naiwny - ObserwowaneMiejsca.czas_dodania) < limit_czasu_obserwacji
            ).all()
            tokeny_do_usuniecia = []
            for obserwator in obserwatorzy:
                send_push_notification(obserwator.device_token, obserwator.sensor_id)
                tokeny_do_usuniecia.append(obserwator.device_token)
            if tokeny_do_usuniecia:
                db.query(ObserwowaneMiejsca).filter(
                    ObserwowaneMiejsca.device_token.in_(tokeny_do_usuniecia)
                ).delete(synchronize_session=False)
        
        db.commit()
        return {"status": "zapisano"}

    elif "gateway_id" in dane_z_bramki:
        logger.info(f"Odebrano i zignorowano heartbeat bramki: {dane_z_bramki.get('gateway_id')}")
        return {"status": "heartbeat zignorowany"}
    
    else:
        logger.warning(f"Odebrano nieznany format danych: {dane_z_bramki}")
        return {"status": "nieznany format"}


# Endpoint dla BRAMKI (HTTP Fallback)
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
async def aktualizuj_miejsce_http(request: Request, db: Session = Depends(get_db)):
    # === POPRAWKA ===
    teraz_naiwny_utc = datetime.datetime.utcnow() # Używamy naiwnego UTC
    
    body_bytes = await request.body()
    raw_json_str = body_bytes.decode('latin-1').strip().replace('\x00', '')
    logger.info(f"Odebrano surowy payload (HTTP): {repr(raw_json_str)}") 
    
    try:
        dane = json.loads(raw_json_str)
        _ = WymaganyFormat(**dane) 
    except Exception as e: 
        logger.error(f"BŁĄD PARSOWANIA/WALIDACJI JSON (HTTP): {e} | Surowe dane: {repr(raw_json_str)}")
        raise HTTPException(status_code=400, detail=f"Niepoprawny format danych JSON. Szczegóły: {e}")
        
    return process_parking_update(dane, db, teraz_naiwny_utc)


# === POPRAWIONY ENDPOINT DLA APLIKACJI (Publiczny) ===
@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    
    # === KLUCZOWA ZMIANA ===
    # Używamy datetime.datetime.utcnow() (NAIWNY czas UTC)
    teraz_naiwny_utc = datetime.datetime.utcnow() 
    
    limit_czasu = datetime.timedelta(minutes=3)
    
    # 1. Znajdź wszystkie sensory, które nie są 'Nieznane' (status 0 lub 1)
    #    ALE ich ostatnia aktualizacja jest starsza niż 3 minuty LUB jest pusta (NULL).
    
    # To zapytanie porównuje teraz dwa "naiwne" czasy, co jest poprawne.
    sensory_offline = db.query(AktualnyStan).filter(
        AktualnyStan.status != 2, 
        (
            (AktualnyStan.ostatnia_aktualizacja == None) |
            (teraz_naiwny_utc - AktualnyStan.ostatnia_aktualizacja > limit_czasu)
        )
    ).all()

    # 2. Jeśli znaleziono takie sensory, zaktualizuj je
    if sensory_offline:
        ids_do_zmiany = [s.sensor_id for s in sensory_offline]
        logger.warning(f"Sensory offline (brak aktualizacji > 3 min): {ids_do_zmiany}. Ustawiam status 2.")
        
        # Aktualizujemy status na 2 i timestamp, aby nie sprawdzać ich ponownie
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.in_(ids_do_zmiany)
        ).update({"status": 2, "ostatnia_aktualizacja": teraz_naiwny_utc}, synchronize_session=False)
        
        db.commit()
    
    # 3. Zwróć *cały* stan (już zaktualizowany)
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca

# =========================================================
# === IMPLEMENTACJA KLIENTA MQTT ===
# =========================================================
mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Klient MQTT podłączony do brokera {MQTT_BROKER}. Subskrybuję temat: {MQTT_TOPIC_SUBSCRIBE}")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"Nie udało się połączyć z MQTT, kod: {rc}")

# === FUNKCJA on_message (przekazuje słownik) ===
def on_message(client, userdata, msg):
    # === POPRAWKA ===
    teraz_naiwny_utc = datetime.datetime.utcnow() # Używamy naiwnego UTC
    db = None 
    try:
        raw_json_str = msg.payload.decode('utf-8').strip().replace('\x00', '')
        logger.info(f"ODEBRANO MQTT na temacie {msg.topic}: {repr(raw_json_str)}")
        
        dane_slownik = json.loads(raw_json_str)
        db = next(get_db())
        
        # Przekaż surowy słownik do nowej funkcji
        process_parking_update(dane_slownik, db, teraz_naiwny_utc) 
        
    except json.JSONDecodeError as e:
        logger.error(f"BŁĄD PARSOWANIA JSON (MQTT): {e} | Surowe dane: {repr(raw_json_str)}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY przetwarzania wiadomości MQTT: {e}", exc_info=True)
    finally:
        if db:
            db.close()

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
