import os
import datetime
from fastapi import FastAPI, Depends, HTTPException, Header
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import requests # Używamy prostej biblioteki 'requests'

# === Konfiguracja Bazy Danych ===
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

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

# === Definicja Klucza API (Bezpieczeństwo Bramki) ===
API_KEY = os.environ.get('API_KEY')
async def check_api_key(x_api_key: str = Header(None)):
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

# === Uruchomienie FastAPI ===
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


# ❗️ PROSTA FUNKCJA PUSH (używa `requests` i serwerów Expo)
def send_push_notification(token: str, sensor_id: str):
    """
    Wysyła powiadomienie PUSH do serwerów Expo.
    Expo użyje naszego pliku .json, który wgraliśmy, aby uwierzytelnić się w Google.
    """
    print(f"Wysyłanie powiadomienia PUSH (przez Expo) do tokena: {token} dla miejsca: {sensor_id}")
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={
            "to": token,
            "sound": "default",
            "title": "❌ Miejsce parkingowe zajęte!",
            "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte.",
            "data": { "sensor_id": sensor_id, "action": "reroute" }
        })
        print(f"Pomyślnie wysłano żądanie do Expo dla: {sensor_id}")
    except Exception as e:
        print(f"BŁĄD KRYTYCZNY: Nie można wysłać PUSH przez Expo: {e}")


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
        limit_czasu = datetime.timedelta(minutes=30)
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
    limit_czasu = datetime.timedelta(minutes=15)
    bramki_offline = db.query(OstatniStanBramki).filter(OstatniStanBramki.ostatni_kontakt < limit_czasu).all()

    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2}) # 2 = Nieznany
    
    db.commit()
    
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca
