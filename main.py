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

# NOWA BIBLIOTEKA DO MQTT
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

# Użyj sqlite w przypadku braku zmiennej środowiskowej
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./parking_data.db"
    logger.warning("Brak DATABASE_URL, używam domyślnej bazy SQLite.")


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === Definicja Tabel ===
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0) # 0=wolne, 1=zajęte, 2=nieznane
    ostatnia_aktualizacja = Column(DateTime, default=datetime.datetime.utcnow)

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
    czas_dodania = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === Definicja Klucza API ===
API_KEY = os.environ.get('API_KEY')
async def check_api_key(x_api_key: str = Header(None)):
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

# === Uruchomienie FastAPI ===
app = FastAPI(title="Parking API")

# Dodajemy reguły CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Klasy Pydantic do walidacji danych
class WymaganyFormat(BaseModel):
    sensor_id: str
    status: int 

class ObserwujRequest(BaseModel):
    sensor_id: str
    device_token: str

# NOWA KLASA PYDANTIC DLA ZAPYTANIA O STATYSTYKI
class StatystykiZapytanie(BaseModel):
    sensor_id: Optional[str] = None  # Konkretny czujnik ('EURO_1') lub prefiks ('EURO')
    data_od: Optional[datetime.date] = None
    data_do: Optional[datetime.date] = None
    dzien_tygodnia: Optional[int] = None # 0=Pon, 6=Niedziela
    tylko_weekend: bool = False
    # tylko_swieta: bool = False # Zostawiamy do rozbudowy

@app.get("/")
def read_root():
    return {"status": "Parking API działa! Słucham MQTT na: " + MQTT_TOPIC_SUBSCRIBE}

# PROSTA FUNKCJA PUSH
def send_push_notification(token: str, sensor_id: str):
    logger.info(f"Wysyłanie powiadomienia PUSH (przez Expo) do tokena: {token} dla miejsca: {sensor_id}")
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={
            "to": token,
            "sound": "default",
            "title": "❌ Miejsce parkingowe zajęte!",
            "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte. Kliknij, aby znaleźć nowe.",
            # Dodajemy dane, które przekażemy do aplikacji (użyte w App.js)
            "data": { "sensor_id": sensor_id, "action": "reroute" } 
        })
        logger.info(f"Pomyślnie wysłano żądanie do Expo dla: {sensor_id}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Expo: {e}")

# Endpoint dla APLIKACJI MOBILNEJ (obserwacja)
@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    token = request.device_token
    sensor_id = request.sensor_id
    teraz = datetime.datetime.utcnow()

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


# === NOWY ENDPOINT DLA STATYSTYK ZAJĘTOŚCI ===
# (Ten, którego brakuje na Twoim serwerze)
@app.post("/api/v1/statystyki/zajetosc")
def pobierz_statystyki_zajetosci(zapytanie: StatystykiZapytanie, db: Session = Depends(get_db)):
    teraz = datetime.datetime.utcnow()
    
    # 1. Budowanie zapytania filtrującego DaneHistoryczne
    query = db.query(DaneHistoryczne)

    if zapytanie.data_od:
        query = query.filter(DaneHistoryczne.czas_pomiaru >= zapytanie.data_od)
    if zapytanie.data_do:
        # Uwzględnienie pełnego dnia
        query = query.filter(DaneHistoryczne.czas_pomiaru <= zapytanie.data_do + datetime.timedelta(days=1)) 
    
    # 2. Wykonanie zapytania i filtrowanie w Pythonie (łatwiejsza obsługa dat)
    wszystkie_dane = query.all()
    
    # 3. Filtr po sensor_id
    if zapytanie.sensor_id:
        if len(zapytanie.sensor_id) < 5: 
            # Filtrowanie prefiksem (np. "EURO")
            wszystkie_dane = [d for d in wszystkie_dane if d.sensor_id.startswith(zapytanie.sensor_id)]
        else:
            # Filtrowanie pełnym ID czujnika
            wszystkie_dane = [d for d in wszystkie_dane if d.sensor_id == zapytanie.sensor_id]
            
    # 4. Filtrowanie Dnia Tygodnia/Weekendu
    if zapytanie.dzien_tygodnia is not None:
        # weekday: 0=Poniedziałek, 6=Niedziela
        wszystkie_dane = [d for d in wszystkie_dane if d.czas_pomiaru.weekday() == zapytanie.dzien_tygodnia]
    
    if zapytanie.tylko_weekend:
        # 5=Sobota, 6=Niedziela
        wszystkie_dane = [d for d in wszystkie_dane if d.czas_pomiaru.weekday() >= 5] 

    # 5. Agregacja (Grupowanie według godziny)
    godzinowe_statusy = {} # { godzina: [status1, status2, ...], ...}
    
    for rekord in wszystkie_dane:
        godzina = rekord.czas_pomiaru.hour
        godzinowe_statusy.setdefault(godzina, []).append(rekord.status)
        
    wyniki = []
    for godzina, statusy in sorted(godzinowe_statusy.items()):
        # status: 0=wolne, 1=zajęte, 2=nieznane
        zajete = statusy.count(1)
        wolne = statusy.count(0)
        
        # Obliczenie procentu zajętości (ignorujemy status 2=nieznane w obliczeniach)
        suma_pomiarow = zajete + wolne
        procent_zajetosci = (zajete / suma_pomiarow) * 100 if suma_pomiarow > 0 else 0
        
        wyniki.append({
            "godzina": f"{godzina:02d}:00",
            "procent_zajetosci": round(procent_zajetosci, 1),
            "liczba_pomiarow": len(statusy)
        })

    return {"wyniki_godzinowe": wyniki}


# === KLUCZOWA FUNKCJA PRZETWARZAJĄCA ZAPIS DO BAZY DANYCH ===
def process_parking_update(dane_z_bramki: WymaganyFormat, db: Session, teraz: datetime.datetime):
    
    sensor_id = dane_z_bramki.sensor_id
    nowy_status = dane_z_bramki.status

    # 1. Zapisz do Danych Historycznych
    nowy_rekord_historyczny = DaneHistoryczne(
        czas_pomiaru=teraz,
        sensor_id=sensor_id,
        status=nowy_status
    )
    db.add(nowy_rekord_historyczny)

    # 2. Zaktualizuj Aktualny Stan
    miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    poprzedni_status = -1
    
    if miejsce_db:
        poprzedni_status = miejsce_db.status
        miejsce_db.status = nowy_status
        miejsce_db.ostatnia_aktualizacja = teraz
    else:
        nowe_miejsce = AktualnyStan(
            sensor_id=sensor_id,
            status=nowy_status,
            ostatnia_aktualizacja=teraz
        )
        db.add(nowe_miejsce)
    
    # 3. Logika Powiadomień: Poprzedni był wolny (0) lub nieznany (2), a teraz jest zajęty (1)
    if poprzedni_status != 1 and nowy_status == 1:
        logger.info(f"Wykryto zajęcie miejsca: {sensor_id}. Sprawdzanie obserwatorów...")
        
        # Czas, przez jaki token jest ważny (np. 30 minut)
        limit_czasu_obserwacji = datetime.timedelta(minutes=30)
        obserwatorzy = db.query(ObserwowaneMiejsca).filter(
            ObserwowaneMiejsca.sensor_id == sensor_id,
            (teraz - ObserwowaneMiejsca.czas_dodania) < limit_czasu_obserwacji
        ).all()

        tokeny_do_usuniecia = []
        for obserwator in obserwatorzy:
            send_push_notification(obserwator.device_token, obserwator.sensor_id)
            tokeny_do_usuniecia.append(obserwator.device_token) # Usuniemy, by nie spamować
            logger.info(f"Wysłano powiadomienie do tokena {obserwator.device_token[:5]}... dla miejsca {sensor_id}")

        if tokeny_do_usuniecia:
            db.query(ObserwowaneMiejsca).filter(
                ObserwowaneMiejsca.device_token.in_(tokeny_do_usuniecia)
            ).delete(synchronize_session=False)

    
    # 4. Zaktualizuj czas ostatniego kontaktu z bramką
    # Zakładamy, że ID bramki to prefiks (np. EURO, BUD)
    bramka_id_z_czujnika = sensor_id.split('_')[0] 
    bramka_db = db.query(OstatniStanBramki).filter(OstatniStanBramki.bramka_id == bramka_id_z_czujnika).first()
    if bramka_db:
        bramka_db.ostatni_kontakt = teraz
    else:
        nowa_bramka = OstatniStanBramki(bramka_id=bramka_id_z_czujnika, ostatni_kontakt=teraz)
        db.add(nowa_bramka)

    db.commit()
    return {"status": "zapisano"}

# Endpoint dla BRAMKI LILYGO (zachowany jako fallback HTTP)
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
async def aktualizuj_miejsce_http(request: Request, db: Session = Depends(get_db)):
    teraz = datetime.datetime.utcnow()
    
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


# Endpoint dla APLIKACJI MOBILNEJ (Publiczny)
@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    
    teraz = datetime.datetime.utcnow()
    limit_czasu_bramki = datetime.timedelta(minutes=15)
    
    # Logika zmiany statusu na Nieznany (2) jeśli bramka jest offline
    bramki_offline = db.query(OstatniStanBramki).filter(
        (OstatniStanBramki.ostatni_kontakt == None) |
        ((teraz - OstatniStanBramki.ostatni_kontakt) > limit_czasu_bramki)
    ).all()

    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2}) # 2 = Nieznany
    
    db.commit()
    
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca

# =========================================================
# === IMPLEMENTACJA KLIENTA MQTT ===
# =========================================================

mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    """
    Wywoływana, gdy klient pomyślnie połączy się z brokerem.
    """
    if rc == 0:
        logger.info(f"Klient MQTT podłączony do brokera {MQTT_BROKER}. Subskrybuję temat: {MQTT_TOPIC_SUBSCRIBE}")
        client.subscribe(MQTT_TOPIC_SUBSCRIBE)
    else:
        logger.error(f"Nie udało się połączyć z MQTT, kod: {rc}")

def on_message(client, userdata, msg):
    """
    Wywoływana, gdy zostanie odebrana wiadomość na subskrybowanym temacie.
    """
    teraz = datetime.datetime.utcnow()
    
    try:
        # Wczytanie ładunku (payload) jako JSON
        raw_json_str = msg.payload.decode('utf-8').strip().replace('\x00', '')
        logger.info(f"ODEBRANO MQTT na temacie {msg.topic}: {repr(raw_json_str)}")
        
        dane = json.loads(raw_json_str)
        dane_z_bramki = WymaganyFormat(**dane) 
        
        # Otwarcie sesji DB i przetworzenie danych
        db = next(get_db())
        process_parking_update(dane_z_bramki, db, teraz)
        db.close()
        
    except json.JSONDecodeError as e:
        logger.error(f"BŁĄD PARSOWANIA JSON (MQTT): {e} | Surowe dane: {repr(raw_json_str)}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY przetwarzania wiadomości MQTT: {e}")


def mqtt_listener_thread():
    """
    Główna funkcja nasłuchująca MQTT, uruchamiana w osobnym wątku.
    """
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    # Próba połączenia z brokerem
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        # Pętla nasłuchująca w tle
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
