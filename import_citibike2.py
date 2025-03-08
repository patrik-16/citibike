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

    # Koordinaten konvertieren (von Ganzzahl zu Dezimalgrad)
    #df['start_lat'] = df['start_lat'] / 1000000.0
    #df['start_lng'] = df['start_lng'] / 1000000.0
    #df['end_lat'] = df['end_lat'] / 1000000.0
    #df['end_lng'] = df['end_lng'] / 1000000.0

    # Daten in PostgreSQL schreiben (Bulk-Insert)
    df.to_sql("citibike_trips", engine, if_exists="append", index=False)

    print("Daten erfolgreich importiert!")

except Exception as e:
    print(f"Fehler beim Import: {e}")
