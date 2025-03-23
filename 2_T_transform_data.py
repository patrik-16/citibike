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

# 1. Normierung: lowercase
df["member_casual"] = df["member_casual"].str.lower()
df["rideable_type"] = df["rideable_type"].str.lower()

# 2. Zeitstempel in Zeitdimension zerlegen
for col in ["started_at", "ended_at"]:
    df[col] = pd.to_datetime(df[col])

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

df_dates.to_sql("dim_datum", engine, if_exists="append", index=False)

# 3. Deduplizieren und Laden: DIM_KUNDE
df_kunde = df[["member_casual"]].drop_duplicates().rename(columns={"member_casual": "Status"})
df_kunde.to_sql("dim_kunde", engine, if_exists="append", index=False)

# 4. Deduplizieren und Laden: DIM_FAHRRAD
df_fahrrad = df[["rideable_type"]].drop_duplicates().rename(columns={"rideable_type": "Typ"})
df_fahrrad.to_sql("dim_fahrrad", engine, if_exists="append", index=False)

# 5. Deduplizieren und Laden: DIM_STATION
start_stations = df[["start_station_id", "start_station_name", "start_lng", "start_lat"]].dropna()
start_stations.columns = ["Station_Code", "Name", "Längengrad", "Breitengrad"]

end_stations = df[["end_station_id", "end_station_name", "end_lng", "end_lat"]].dropna()
end_stations.columns = ["Station_Code", "Name", "Längengrad", "Breitengrad"]

df_stationen = pd.concat([start_stations, end_stations]).drop_duplicates()
df_stationen.to_sql("dim_station", engine, if_exists="append", index=False)

print("Transformation abgeschlossen.")
