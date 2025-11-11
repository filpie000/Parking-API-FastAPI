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

# ❗️ ZAKTUALIZOWANY MODEL STATYSTYK
class StatystykiZapytanie(BaseModel):
    sensor_id: Optional[str] = None
    data_od: Optional[datetime.date] = None
    data_do: Optional[datetime.date] = None
    dzien_tygodnia: Optional[int] = None 
    tylko_weekend: bool = False
    
    # NOWE POLA DLA DYNAMICZNEJ PROGNOZY
    aktualny_dzien_tygodnia: Optional[int] = None # 0=Pon, 6=Niedziela
    aktualna_godzina: Optional[int] = None # 0-23

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
        logger.info(f"Pomyślnie wysłano żądanie do Expo dla: {sensor_id}")
    except Exception as e:
        logger.error(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Expo: {e}")

# ❗️ NOWA FUNKCJA DO LOGIKI DYNAMICZNYCH STATYSTYK
def _pobierz_statystyki_dynamiczne(zapytanie: StatystykiZapytanie, db: Session):
    """
    Wykonuje nową, dynamiczną logikę statystyk na podstawie 
    aktualnego dnia i godziny.
    """
    dzien = zapytanie.aktualny_dzien_tygodnia # 0=Pon, 6=Niedziela
    godzina = zapytanie.aktualna_godzina # 0-23
    
    # 1. Funkcja pomocnicza do obsługi czasu (w tym zawijania o północy)
    def get_time_with_offset(base_hour, offset_minutes):
        base_dt = datetime.datetime(2000, 1, 1, base_hour, 0) # Używamy daty-wydmuszki
        offset_dt = base_dt + datetime.timedelta(minutes=offset_minutes)
        return offset_dt.time()

    # 2. Ustalanie parametrów filtrowania
    przedzial_str = ""
    kategoria_str = ""
    dni_do_uwzglednienia = [] # Lista dni tygodnia (0-6)
    czas_poczatek = None
    czas_koniec = None
    
    # Sprawdzamy, czy jesteśmy w kategorii "weekendowej" (Pt po 14, Sob, Niedz)
    if (dzien == 4 and godzina >= 14) or dzien == 5 or dzien == 6:
        # LOGIKA WEEKENDOWA: +/- 1 godzina
        czas_poczatek = get_time_with_offset(godzina, -60)
        czas_koniec = get_time_with_offset(godzina, 60)
        przedzial_str = f"{czas_poczatek.strftime('%H:%M')} - {czas_koniec.strftime('%H:%M')}"
        
        if (dzien == 4 and godzina >= 14):
            kategoria_str = "Piątek (po 14:00)"
            dni_do_uwzglednienia = [4] # Tylko piątki
        elif dzien == 5:
            kategoria_str = "Sobota"
            dni_do_uwzglednienia = [5] # Tylko soboty
        else: # dzien == 6
            kategoria_str = "Niedziela"
            dni_do_uwzglednienia = [6] # Tylko niedziele
    
    else:
        # LOGIKA DNI ROBOCZYCH: +/- 30 minut
        czas_poczatek = get_time_with_offset(godzina, -30)
        czas_koniec = get_time_with_offset(godzina, 30)
        przedzial_str = f"{czas_poczatek.strftime('%H:%M')} - {czas_koniec.strftime('%H:%M')}"
        kategoria_str = "Dni robocze (Pn-Pt)"
        dni_do_uwzglednienia = [0, 1, 2, 3, 4] # Mon-Fri
    
    # 3. Pobranie i filtrowanie danych
    # Pobieramy wszystkie rekordy dla danego sensora (np. "EURO")
    query = db.query(DaneHistoryczne).filter(
        DaneHistoryczne.sensor_id.startswith(zapytanie.sensor_id)
    )
    wszystkie_dane = query.all()
    
    # Filtrujemy dane w Pythonie (jest to łatwiejsze dla złożonej logiki dat)
    dane_pasujace = []
    
    for rekord in wszystkie_dane:
        dzien_rekordu = rekord.czas_pomiaru.weekday()
        czas_rekordu = rekord.czas_pomiaru.time()
        
        # 1. Czy zgadza się dzień tygodnia?
        if dzien_rekordu not in dni_do_uwzglednienia:
            continue
        
        # 2. Czy zgadza się przedział czasowy? (z obsługą zawijania o północy)
        if czas_poczatek > czas_koniec: # Np. 23:30 - 00:30
            if not (czas_rekordu >= czas_poczatek or czas_rekordu < czas_koniec):
                continue
        else: # Normalny przedział, np. 09:00 - 10:00
            if not (czas_poczatek <= czas_rekordu < czas_koniec):
                continue
        
        # 3. Dodatkowe reguły dla "Dni roboczych" (musimy wykluczyć Pt po 14)
        if kategoria_str == "Dni robocze (Pn-Pt)":
            if dzien_rekordu == 4 and czas_rekordu >= datetime.time(14, 0):
                continue # To jest piątek po 14, więc go wykluczamy
        
        # 4. Dodatkowe reguły dla "Piątek po 14" (musimy *uwzględnić* tylko Pt po 14)
        if kategoria_str == "Piątek (po 14:00)":
             if czas_rekordu < datetime.time(14, 0):
                continue # To jest piątek *przed* 14, więc go wykluczamy

        # Jeśli rekord przeszedł wszystkie testy, dodajemy go
        dane_pasujace.append(rekord.status)

    # 4. Obliczanie wyniku
    if not dane_pasujace:
        procent_zajetosci = 0
        liczba_pomiarow = 0
    else:
        zajete = dane_pasujace.count(1)
        wolne = dane_pasujace.count(0)
        suma_pomiarow = zajete + wolne
        procent_zajetosci = (zajete / suma_pomiarow) * 100 if suma_pomiarow > 0 else 0
        liczba_pomiarow = len(dane_pasujace)

    # 5. Zwracamy NOWY format odpowiedzi
    return {
        "wynik_dynamiczny": {
            "kategoria": kategoria_str,
            "przedzial_czasu": przedzial_str,
            "procent_zajetosci": round(procent_zajetosci, 1),
            "liczba_pomiarow": liczba_pomiarow
        }
    }


# === Endpointy ===
@app.get("/")
def read_root():
    return {"status": "Parking API działa! Słucham MQTT na: " + MQTT_TOPIC_SUBSCRIBE}

@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    # ... (ta funkcja pozostaje bez zmian) ...
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
    
    # --- NOWA LOGIKA DYNAMICZNA ---
    if zapytanie.aktualny_dzien_tygodnia is not None and zapytanie.aktualna_godzina is not None:
        logger.info("Wykonuję zapytanie o statystyki dynamiczne (prognozę)...")
        return _pobierz_statystyki_dynamiczne(zapytanie, db)
    
    # --- STARA LOGIKA (Fallback) ---
    logger.info("Wykonuję zapytanie o statystyki ogólne (godzinowe)...")
    
    query = db.query(DaneHistoryczne)
    if zapytanie.data_od:
        query = query.filter(DaneHistoryczne.czas_pomiaru >= zapytanie.data_od)
    if zapytanie.data_do:
        query = query.filter(DaneHistoryczne.czas_pomiaru <= zapytanie.data_do + datetime.timedelta(days=1)) 
    
    wszystkie_dane = query.all()
    
    if zapytanie.sensor_id:
        if len(zapytanie.sensor_id) < 5: 
            wszystkie_dane = [d for d in wszystkie_dane if d.sensor_id.startswith(zapytanie.sensor_id)]
        else:
            wszystkie_dane = [d for d in wszystkie_dane if d.sensor_id == zapytanie.sensor_id]
            
    if zapytanie.dzien_tygodnia is not None:
           wszystkie_dane = [d for d in wszystkie_dane if d.czas_pomiaru.weekday() == zapytanie.dzien_tygodnia]
    
    if zapytanie.tylko_weekend:
           wszystkie_dane = [d for d in wszystkie_dane if d.czas_pomiaru.weekday() >= 5] 

    godzinowe_statusy = {} 
    
    for rekord in wszystkie_dane:
        godzina = rekord.czas_pomiaru.hour
        godzinowe_statusy.setdefault(godzina, []).append(rekord.status)
        
    wyniki = []
    for godzina, statusy in sorted(godzinowe_statusy.items()):
        zajete = statusy.count(1)
        wolne = statusy.count(0)
        suma_pomiarow = zajete + wolne
        procent_zajetosci = (zajete / suma_pomiarow) * 100 if suma_pomiarow > 0 else 0
        
        wyniki.append({
            "godzina": f"{godzina:02d}:00",
            "procent_zajetosci": round(procent_zajetosci, 1),
            "liczba_pomiarow": len(statusy)
        })

    return {"wyniki_godzinowe": wyniki}


# === FUNKCJA GŁÓWNA PRZETWARZANIA DANYCH ===
def process_parking_update(dane_z_bramki: WymaganyFormat, db: Session, teraz: datetime.datetime):
    # ... (ta funkcja pozostaje bez zmian) ...
    sensor_id = dane_z_bramki.sensor_id
    nowy_status = dane_z_bramki.status

    # 1. Zapisz do Danych Historycznych
    nowy_rekord_historyczny = DaneHistoryczne(
        czas_pomiaru=teraz, sensor_id=sensor_id, status=nowy_status
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
            sensor_id=sensor_id, status=nowy_status, ostatnia_aktualizacja=teraz
        )
        db.add(nowe_miejsce)
    
    # 3. Logika Powiadomień
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

    
    # 4. Zaktualizuj czas ostatniego kontaktu z bramką
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
    # ... (ta funkcja pozostaje bez zmian) ...
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
    # ... (ta funkcja pozostaje bez zmian) ...
    teraz = datetime.datetime.now(datetime.UTC)
    limit_czasu_bramki = datetime.timedelta(minutes=15)
    
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
# ... (ta sekcja pozostaje bez zmian) ...

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
