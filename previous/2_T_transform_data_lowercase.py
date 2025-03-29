import pandas as pd
from sqlalchemy import create_engine

# Verbindungsdetails
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

# Verbindung zur PostgreSQL-Datenbank
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Staging-Daten laden
df = pd.read_sql("SELECT * FROM stg_citibike_trips", engine)
print("Staging-Daten geladen:")
print(df.head())

# 1. Normierung: lowercase
df["member_casual"] = df["member_casual"].str.lower()
df["rideable_type"] = df["rideable_type"].str.lower()

print("Einzigartige Kundentypen (nach Normalisierung):", df["member_casual"].unique())
print("Einzigartige Fahrradtypen:", df["rideable_type"].unique())


# 2. Station-IDs in Integer umwandeln
#    a) Dezimalpunkt entfernen
df["start_station_id"] = df["start_station_id"].astype(str).str.replace(".", "", regex=False)
df["end_station_id"]   = df["end_station_id"].astype(str).str.replace(".", "", regex=False)

#    b) Als Zahlen parsen (Fehler -> NaN)
df["start_station_id"] = pd.to_numeric(df["start_station_id"], errors="coerce")
df["end_station_id"]   = pd.to_numeric(df["end_station_id"], errors="coerce")

#    c) NaN durch 0 ersetzen
df["start_station_id"] = df["start_station_id"].fillna(0).astype(int)
df["end_station_id"]   = df["end_station_id"].fillna(0).astype(int)


# 3. Zeitstempel in Zeitdimension zerlegen
for col in ["started_at", "ended_at"]:
    df[col] = pd.to_datetime(df[col], errors='coerce')

print("Datentypen nach Umwandlung:", df.dtypes)

df_dates = pd.concat([
    df[["started_at"]].rename(columns={"started_at": "volles_datum_uhrzeit"}),
    df[["ended_at"]].rename(columns={"ended_at": "volles_datum_uhrzeit"})
]).drop_duplicates()

df_dates["tag"] = df_dates["volles_datum_uhrzeit"].dt.day
df_dates["woche"] = df_dates["volles_datum_uhrzeit"].dt.isocalendar().week
df_dates["monat"] = df_dates["volles_datum_uhrzeit"].dt.month
df_dates["jahr"] = df_dates["volles_datum_uhrzeit"].dt.year
df_dates["stunde"] = df_dates["volles_datum_uhrzeit"].dt.hour
df_dates["minute"] = df_dates["volles_datum_uhrzeit"].dt.minute
df_dates["sekunde"] = df_dates["volles_datum_uhrzeit"].dt.second

print("Beispielhafte Zeitdimension:")
print(df_dates.head())

df_dates.to_sql("dim_datum", engine, if_exists="append", index=False)

# 4. Deduplizieren und Laden: dim_kunde
df_kunde = df[["member_casual"]].drop_duplicates().rename(columns={"member_casual": "status"})
print("dim_kunde Vorschau:")
print(df_kunde)
df_kunde.to_sql("dim_kunde", engine, if_exists="append", index=False)

# 5. Deduplizieren und Laden: dim_fahrrad
df_fahrrad = df[["rideable_type"]].drop_duplicates().rename(columns={"rideable_type": "typ"})
print("dim_fahrrad Vorschau:")
print(df_fahrrad)
df_fahrrad.to_sql("dim_fahrrad", engine, if_exists="append", index=False)

# 6. Deduplizieren und Laden: dim_station
start_stations = df[["start_station_id", "start_station_name", "start_lng", "start_lat"]].dropna()
start_stations.columns = ["station_code", "name", "laengengrad", "breitengrad"]

end_stations = df[["end_station_id", "end_station_name", "end_lng", "end_lat"]].dropna()
end_stations.columns = ["station_code", "name", "laengengrad", "breitengrad"]

df_stationen = pd.concat([start_stations, end_stations])

# Schritt A: Duplikate innerhalb dieses DataFrame entfernen
df_stationen.drop_duplicates(subset=["station_code"], inplace=True)

# Schritt B: Existierende station_codes aus DB abfragen
try:
    existing_codes_df = pd.read_sql("SELECT station_code FROM dim_station", engine)
    existing_station_codes = set(existing_codes_df["station_code"])
except:
    # Falls Tabelle noch nicht existiert
    existing_station_codes = set()

# Schritt C: Nur Zeilen behalten, deren station_code NICHT schon in dim_station steht
df_stationen = df_stationen[~df_stationen["station_code"].isin(existing_station_codes)]

print("Anzahl eindeutig neuer Stationen:", len(df_stationen))

# Falls noch neue Stationen übrig sind -> Append
if len(df_stationen) > 0:
    df_stationen.to_sql("dim_station", engine, if_exists="append", index=False)
    print("Neue Stationen in dim_station geladen.")
else:
    print("Keine neuen Stationen hinzuzufügen.")

print("Transformation abgeschlossen.")
