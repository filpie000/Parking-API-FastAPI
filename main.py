import os
import datetime
from fastapi import FastAPI, Depends, HTTPException, Header
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel

# === KROK 1: Konfiguracja Bazy Danych ===

# Render poda Ci ten adres URL jako Zmienną Środowiskową
DATABASE_URL = os.environ.get('DATABASE_URL')

# Ta linijka jest potrzebna, bo Heroku/Render zmienia 'postgres://' na 'postgresql://'
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Utwórz silnik bazy danych
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# === KROK 2: Definicja Tabel (Twoje 3 Bazy) ===

# Tabela 1: Przechowuje TYLKO ostatni znany stan (8 wierszy)
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

# Tabela 3: Przechowuje tylko czas ostatniego kontaktu z BRAMKĄ
class OstatniStanBramki(Base):
    __tablename__ = "ostatni_stan_bramki"
    # Załóżmy, że bramki mają ID (np. "Bramka_Parking_A")
    bramka_id = Column(String, primary_key=True, default="Bramka_A") 
    ostatni_kontakt = Column(DateTime)

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

# Render poda Ci ten klucz jako Zmienną Środowiskową
API_KEY = os.environ.get('API_KEY')

async def check_api_key(x_api_key: str = Header(None)):
    """Sprawdza, czy klucz API w nagłówku jest poprawny."""
    if not API_KEY:
        # Jeśli nie ustawiono klucza na serwerze, przepuść (dla testów)
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Niepoprawny Klucz API")

# === KROK 4: Uruchomienie FastAPI i definicja endpointów ===

app = FastAPI(title="Parking API")

# Definicja danych, których spodziewamy się od Bramki LILYGO
class StatusCzujnika(BaseModel):
    sensor_id: str
    status: int # 0, 1 lub 2

@app.get("/")
def read_root():
    return {"status": "Parking API działa!"}


# === Endpoint dla BRAMKI LILYGO (Zabezpieczony Kluczem API) ===
@app.put("/api/v1/miejsce_parkingowe/aktualizuj", dependencies=[Depends(check_api_key)])
def aktualizuj_miejsce(dane_z_bramki: StatusCzujnika, db: Session = Depends(get_db)):
    """
    Ten endpoint jest wywoływany przez Bramkę 4G, aby zaktualizować status miejsca.
    """
    teraz = datetime.datetime.utcnow()
    sensor_id = dane_z_bramki.sensor_id
    status = dane_z_bramki.status

    # 1. Zapisz do Danych Historycznych (Tabela 2)
    nowy_rekord_historyczny = DaneHistoryczne(
        czas_pomiaru=teraz,
        sensor_id=sensor_id,
        status=status
    )
    db.add(nowy_rekord_historyczny)

    # 2. Zaktualizuj Aktualny Stan (Tabela 1)
    miejsce_db = db.query(AktualnyStan).filter(AktualnyStan.sensor_id == sensor_id).first()
    if miejsce_db:
        miejsce_db.status = status
        miejsce_db.ostatnia_aktualizacja = teraz
    else:
        # Jeśli to pierwszy raz, stwórz nowy wpis
        nowe_miejsce = AktualnyStan(
            sensor_id=sensor_id,
            status=status,
            ostatnia_aktualizacja=teraz
        )
        db.add(nowe_miejsce)
    
    # 3. Zaktualizuj czas ostatniego kontaktu z bramką (Tabela 3)
    # (Zakładamy, że wszystkie czujniki z jednej bramki mają podobne ID)
    bramka_id_z_czujnika = sensor_id.split('_')[0] # np. "parking_A_01" -> "parking_A"
    bramka_db = db.query(OstatniStanBramki).filter(OstatniStanBramki.bramka_id == bramka_id_z_czujnika).first()
    if bramka_db:
        bramka_db.ostatni_kontakt = teraz
    else:
        nowa_bramka = OstatniStanBramki(bramka_id=bramka_id_z_czujnika, ostatni_kontakt=teraz)
        db.add(nowa_bramka)

    # Zapisz wszystkie zmiany w bazie
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

    # Zaktualizuj status miejsc, których bramki są offline
    for bramka in bramki_offline:
        db.query(AktualnyStan).filter(
            AktualnyStan.sensor_id.startswith(bramka.bramka_id)
        ).update({"status": 2}) # 2 = Nieznany
    
    db.commit()
    
    # Zwróć gotową listę wszystkich miejsc
    wszystkie_miejsca = db.query(AktualnyStan).all()
    return wszystkie_miejsca