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

# 2. Zeitstempel in Zeitdimension zerlegen
for col in ["started_at", "ended_at"]:
    df[col] = pd.to_datetime(df[col], errors='coerce')

print("Datentypen nach Umwandlung:", df.dtypes)

df_dates = pd.concat([
    df[["started_at"]].rename(columns={"started_at": "VollesDatumUhrzeit"}),
    df[["ended_at"]].rename(columns={"ended_at": "VollesDatumUhrzeit"})
]).drop_duplicates()

df_dates["Tag"] = df_dates["VollesDatumUhrzeit"].dt.day
df_dates["Woche"] = df_dates["VollesDatumUhrzeit"].dt.isocalendar().week
df_dates["Monat"] = df_dates["VollesDatumUhrzeit"].dt.month
df_dates["Jahr"] = df_dates["VollesDatumUhrzeit"].dt.year
df_dates["Stunde"] = df_dates["VollesDatumUhrzeit"].dt.hour
df_dates["Minute"] = df_dates["VollesDatumUhrzeit"].dt.minute
df_dates["Sekunde"] = df_dates["VollesDatumUhrzeit"].dt.second

print("Beispielhafte Zeitdimension:")
print(df_dates.head())

df_dates.to_sql("DIM_DATUM", engine, if_exists="append", index=False)

# 3. Deduplizieren und Laden: DIM_KUNDE
df_kunde = df[["member_casual"]].drop_duplicates().rename(columns={"member_casual": "Status"})
print("DIM_KUNDE Vorschau:")
print(df_kunde)

df_kunde.to_sql("DIM_KUNDE", engine, if_exists="append", index=False)

# 4. Deduplizieren und Laden: DIM_FAHRRAD
df_fahrrad = df[["rideable_type"]].drop_duplicates().rename(columns={"rideable_type": "Typ"})
print("DIM_FAHRRAD Vorschau:")
print(df_fahrrad)

df_fahrrad.to_sql("DIM_FAHRRAD", engine, if_exists="append", index=False)

# 5. Deduplizieren und Laden: DIM_STATION
start_stations = df[["start_station_id", "start_station_name", "start_lng", "start_lat"]].dropna()
start_stations.columns = ["Station_Code", "Name", "Längengrad", "Breitengrad"]

end_stations = df[["end_station_id", "end_station_name", "end_lng", "end_lat"]].dropna()
end_stations.columns = ["Station_Code", "Name", "Längengrad", "Breitengrad"]

df_stationen = pd.concat([start_stations, end_stations]).drop_duplicates()
print("Anzahl eindeutiger Stationen:", len(df_stationen))

df_stationen.to_sql("DIM_STATION", engine, if_exists="append", index=False)

print("Transformation abgeschlossen.")
