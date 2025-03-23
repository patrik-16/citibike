import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL-Verbindungsdetails
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

# CSV-Datei definieren
csv_file = "202501-citibike-tripdata_1.csv"

# Verbindung zur PostgreSQL-Datenbank herstellen; formatierter String
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

try:
    print("Verbindung zu PostgreSQL erfolgreich!")

    # CSV-Datei mit Pandas einlesen; dataframe
    df = pd.read_csv(csv_file)

    # Daten in PostgreSQL schreiben (Bulk-Insert)
    df.to_sql("stg_citibike_trips", engine, if_exists="append", index=False)

    print("Daten erfolgreich importiert!")

except Exception as e:
    print(f"Fehler beim Import: {e}")