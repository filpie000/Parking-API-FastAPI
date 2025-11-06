import os
import datetime
from fastapi import FastAPI, Depends, HTTPException, Header
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import json

# ❗️ NOWE IMPORTY DLA FIREBASE V1
import firebase_admin
from firebase_admin import credentials, messaging

# === KROK 1: Inicjalizacja Firebase Admin ===
# Pobierz zawartość pliku JSON ze zmiennej środowiskowej
firebase_json_str = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON')
if firebase_json_str:
    try:
        # Przekonwertuj tekst JSON na słownik Pythona
        firebase_creds_dict = json.loads(firebase_json_str)
        # Utwórz dane logowania
        cred = credentials.Certificate(firebase_creds_dict)
        # Zainicjuj aplikację Firebase
        firebase_admin.initialize_app(cred)
        print("INFO: Pomyślnie zainicjowano Firebase Admin SDK.")
    except Exception as e:
        print(f"BŁĄD KRYTYCZNY: Nie można zainicjować Firebase Admin: {e}")
else:
    print("OSTRZEŻENIE: Zmienna FIREBASE_SERVICE_ACCOUNT_JSON nie jest ustawiona. Powiadomienia nie będą działać.")


# === KROK 2: Konfiguracja Bazy Danych ===
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === KROK 3: Definicja Tabel ===
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

# === KROK 4: Definicja Klucza API (Bezpieczeństwo Bramki) ===
API_KEY = os.environ.get('API_KEY')
async def check_api_key(x_api_key: str = Header(None)):
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

# === KROK 5: Uruchomienie FastAPI i definicja endpointów ===
app = FastAPI(title="Parking API")

class StatusCzujnika(BaseModel):
    sensor_id: str
    status: int

class ObserwujRequest(BaseModel):
    sensor_id: str
    device_token: str

@app.get("/")
def read_root():
    return {"status": "Parking API działa!"}


# ❗️ NOWA FUNKCJA PUSH (używa firebase_admin)
def send_push_notification(token: str, sensor_id: str):
    """
    Wysyła powiadomienie PUSH za pomocą Firebase Admin SDK (API V1).
    """
    print(f"Wysyłanie powiadomienia PUSH do tokena: {token} dla miejsca: {sensor_id}")
    try:
        # Stwórz wiadomość
        message = messaging.Message(
            notification=messaging.Notification(
                title="❌ Miejsce parkingowe zajęte!",
                body=f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte.",
            ),
            data={ "sensor_id": sensor_id, "action": "reroute" },
            token=token,
        )
        # Wyślij wiadomość
        response = messaging.send(message)
        print("Pomyślnie wysłano powiadomienie:", response)
    except Exception as e:
        print(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Firebase Admin: {e}")


# Endpoint dla APLIKACJI MOBILNEJ (bez zmian)
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


# Endpoint dla BRAMKI LILYGO (logika powiadomień bez zmian)
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
def aktualizuj_miejsce(dane_z_bramki: StatusCzujnika, db: Session = Depends(get_db)):
    teraz = datetime.datetime.utcnow()
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
    
    # 3. Logika Powiadomień
    if poprzedni_status != 1 and nowy_status == 1:
        print(f"Wykryto zajęcie miejsca: {sensor_id}. Sprawdzanie obserwatorów...")
        limit_czasu = teraz - datetime.timedelta(minutes=30)
        obserwatorzy = db.query(ObserwowaneMiejsca).filter(
            ObserwowaneMiejsca.sensor_id == sensor_id,
            ObserwowaneMiejsca.czas_dodania > limit_czasu
        ).all()

        tokeny_do_usuniecia = []
        for obserwator in obserwatorzy:
            send_push_notification(obserwator.device_token, obserwator.sensor_id)
            tokeny_do_usuniecia.append(obserwator.device_token)

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


# Endpoint dla APLIKACJI MOBILNEJ (Publiczny)
@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    limit_czasu = datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
    bramki_offline = db.query(OstatniStanBramki).filter(OstatniStanBramki.ostatni_kontakt < limit_czasu).all()

    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2}) # 2 = Nieznany
    
    db.commit()
    
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca
