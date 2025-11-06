import os
import datetime
from fastapi import FastAPI, Depends, HTTPException, Header
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import requests # ❗️ Dodana biblioteka (dodaj 'requests' do requirements.txt)

# === KROK 1: Konfiguracja Bazy Danych ===

DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === KROK 2: Definicja Tabel ===

# Tabela 1: Przechowuje TYLKO ostatni znany stan
class AktualnyStan(Base):
    __tablename__ = "aktualny_stan"
    sensor_id = Column(String, primary_key=True, index=True)
    status = Column(Integer, default=0) # 0=wolne, 1=zajęte, 2=nieznane
    ostatnia_aktualizacja = Column(DateTime, default=datetime.datetime.utcnow)

# Tabela 2: Przechowuje CAŁĄ historię pomiarów
class DaneHistoryczne(Base):
    __tablename__ = "dane_historyczne"
    id = Column(Integer, primary_key=True, autoincrement=True)
    czas_pomiaru = Column(DateTime, index=True)
    sensor_id = Column(String, index=True)
    status = Column(Integer)

# Tabela 3: Przechowuje czas ostatniego kontaktu z BRAMKĄ
class OstatniStanBramki(Base):
    __tablename__ = "ostatni_stan_bramki"
    bramka_id = Column(String, primary_key=True, default="Bramka_A") 
    ostatni_kontakt = Column(DateTime)

# ❗️ NOWA TABELA 4: Przechowuje, kto obserwuje jakie miejsce
class ObserwowaneMiejsca(Base):
    __tablename__ = "obserwowane_miejsca"
    # Używamy tokena jako klucza, bo jeden użytkownik może obserwować tylko jedno miejsce naraz
    device_token = Column(String, primary_key=True, index=True) 
    sensor_id = Column(String, index=True)
    czas_dodania = Column(DateTime, default=datetime.datetime.utcnow)

# Stwórz tabele w bazie danych (jeśli nie istnieją)
Base.metadata.create_all(bind=engine)

# Funkcja pomocnicza do "wstrzykiwania" sesji bazy danych
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === KROK 3: Definicja Klucza API (Bezpieczeństwo) ===

API_KEY = os.environ.get('API_KEY')

async def check_api_key(x_api_key: str = Header(None)):
    """Sprawdza, czy klucz API w nagłówku jest poprawny."""
    if not API_KEY:
        return # Jeśli nie ustawiono klucza na serwerze, przepuść
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

# === KROK 4: Uruchomienie FastAPI i definicja endpointów ===

app = FastAPI(title="Parking API")

# Definicja danych, których spodziewamy się od Bramki LILYGO
class StatusCzujnika(BaseModel):
    sensor_id: str
    status: int # 0, 1 lub 2

# ❗️ NOWA DEFINICJA DANYCH: Dane, których oczekujemy od APLIKACJI
class ObserwujRequest(BaseModel):
    sensor_id: str
    device_token: str

@app.get("/")
def read_root():
    return {"status": "Parking API działa!"}


# ❗️ NOWA FUNKCJA: Do wysyłania powiadomień PUSH
def send_push_notification(token: str, sensor_id: str):
    """
    Wysyła powiadomienie PUSH do serwerów Expo, które przekażą je do telefonu.
    """
    print(f"Wysyłanie powiadomienia PUSH do tokena: {token} dla miejsca: {sensor_id}")
    try:
        requests.post("https://exp.host/--/api/v2/push/send", json={
            "to": token,
            "sound": "default",
            "title": "❌ Miejsce parkingowe zajęte!",
            "body": f"Miejsce {sensor_id}, do którego nawigujesz, zostało właśnie zajęte.",
            "data": { "sensor_id": sensor_id, "action": "reroute" } # Dodatkowe dane
        })
    except Exception as e:
        print(f"Błąd wysyłania PUSH: {e}")


# ❗️ NOWY ENDPOINT: Dla APLIKACJI MOBILNEJ
@app.post("/api/v1/obserwuj_miejsce")
def obserwuj_miejsce(request: ObserwujRequest, db: Session = Depends(get_db)):
    """
    Wywoływany przez aplikację, gdy użytkownik klika "Nawiguj".
    Zapisuje, że ten telefon obserwuje to miejsce.
    """
    token = request.device_token
    sensor_id = request.sensor_id
    teraz = datetime.datetime.utcnow()

    # Sprawdź, czy ten telefon już czegoś nie obserwuje
    wpis = db.query(ObserwowaneMiejsca).filter(ObserwowaneMiejsca.device_token == token).first()
    
    if wpis:
        # Jeśli tak, zaktualizuj obserwowane miejsce i czas
        wpis.sensor_id = sensor_id
        wpis.czas_dodania = teraz
    else:
        # Jeśli nie, stwórz nowy wpis
        nowy_obserwator = ObserwowaneMiejsca(
            device_token=token,
            sensor_id=sensor_id,
            czas_dodania=teraz
        )
        db.add(nowy_obserwator)
    
    db.commit()
    return {"status": "obserwowanie rozpoczęte", "miejsce": sensor_id}


# ❗️ ZAKTUALIZOWANY ENDPOINT: Dla BRAMKI LILYGO (Zabezpieczony Kluczem API)
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
def aktualizuj_miejsce(dane_z_bramki: StatusCzujnika, db: Session = Depends(get_db)):
    """
    Wywoływany przez Bramkę 4G.
    Teraz dodatkowo sprawdza, czy ktoś obserwuje to miejsce.
    """
    teraz = datetime.datetime.utcnow()
    sensor_id = dane_z_bramki.sensor_id
    nowy_status = dane_z_bramki.status

    # 1. Zapisz do Danych Historycznych (Tabela 2)
    nowy_rekord_historyczny = DaneHistoryczne(
        czas_pomiaru=teraz,
        sensor_id=sensor_id,
        status=nowy_status
    )
    db.add(nowy_rekord_historyczny)

    # 2. Zaktualizuj Aktualny Stan (Tabela 1)
    miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    
    poprzedni_status = -1 # Wartość domyślna, jeśli miejsce nie istnieje
    
    if miejsce_db:
        poprzedni_status = miejsce_db.status # Zapisz stary status, zanim go nadpiszesz
        miejsce_db.status = nowy_status
        miejsce_db.ostatnia_aktualizacja = teraz
    else:
        nowe_miejsce = AktualnyStan(
            sensor_id=sensor_id,
            status=nowy_status,
            ostatnia_aktualizacja=teraz
        )
        db.add(nowe_miejsce)
    
    # ❗️ NOWA LOGIKA POWIADOMIEŃ ❗️
    # Sprawdź, czy status zmienił się na ZAJĘTE (1)
    if poprzedni_status != 1 and nowy_status == 1:
        # Miejsce właśnie zostało zajęte!
        print(f"Wykryto zajęcie miejsca: {sensor_id}. Sprawdzanie obserwatorów...")
        
        # 1. Znajdź wszystkich, którzy obserwują to miejsce
        limit_czasu = teraz - datetime.timedelta(minutes=30) # Obserwacja wygasa po 30 min
        obserwatorzy = db.query(ObserwowaneMiejsca).filter(
            ObserwowaneMiejsca.sensor_id == sensor_id,
            ObserwowaneMiejsca.czas_dodania > limit_czasu
        ).all()

        # 2. Wyślij powiadomienie do każdego obserwatora
        tokeny_do_usuniecia = []
        for obserwator in obserwatorzy:
            send_push_notification(obserwator.device_token, obserwator.sensor_id)
            tokeny_do_usuniecia.append(obserwator.device_token)

        # 3. Usuń obserwatorów (aby nie dostawali więcej powiadomień)
        if tokeny_do_usuniecia:
            db.query(ObserwowaneMiejsca).filter(
                ObserwowaneMiejsca.device_token.in_(tokeny_do_usuniecia)
            ).delete(synchronize_session=False)

    
    # 3. Zaktualizuj czas ostatniego kontaktu z bramką (Tabela 3)
    bramka_id_z_czujnika = sensor_id.split('_')[0] 
    bramka_db = db.query(OstatniStanBramki).filter(OstatniStanBramki.bramka_id == bramka_id_z_czujnika).first()
    if bramka_db:
        bramka_db.ostatni_kontakt = teraz
    else:
        nowa_bramka = OstatniStanBramki(bramka_id=bramka_id_z_czujnika, ostatni_kontakt=teraz)
        db.add(nowa_bramka)

    db.commit()
    return {"status": "zapisano"}


# === Endpoint dla APLIKACJI MOBILNEJ (Publiczny) ===
@app.get("/api/v1/aktualny_stan")
def pobierz_aktualny_stan(db: Session = Depends(get_db)):
    """
    Ten endpoint jest wywoływany przez aplikację mobilną co 10 sekund.
    """
    # Tutaj logika sprawdzająca, czy bramki żyją (Twój status "Nieznany")
    limit_czasu = datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
    bramki_offline = db.query(OstatniStanBramki).filter(OstatniStanBramki.ostatni_kontakt < limit_czasu).all()

    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2}) # 2 = Nieznany
    
    db.commit()
    
    # Zwróć gotową listę wszystkich miejsc
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca
