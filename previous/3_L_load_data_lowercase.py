import pandas as pd
from sqlalchemy import create_engine

# Verbindung
engine = create_engine("postgresql://myuser:mypassword@localhost:5432/mydatabase")

# Staging-Daten laden
df = pd.read_sql("SELECT * FROM stg_citibike_trips", engine)

# Alle DIM-Tabellen laden
kunden = pd.read_sql("SELECT * FROM dim_kunde", engine)
fahrrad = pd.read_sql("SELECT * FROM dim_fahrrad", engine)
datum = pd.read_sql("SELECT * FROM dim_datum", engine)
stationen = pd.read_sql("SELECT * FROM dim_station", engine)

# Timestamps konvertieren
df["started_at"] = pd.to_datetime(df["started_at"])
df["ended_at"] = pd.to_datetime(df["ended_at"])

# Merge mit dim_kunde
fact = df.merge(kunden, left_on="member_casual", right_on="status", how="inner")

# Merge mit dim_fahrrad
fact = fact.merge(fahrrad, left_on="rideable_type", right_on="typ", how="inner")

# Merge mit dim_datum (Start)
fact = fact.merge(datum, left_on="started_at", right_on="volles_datum_uhrzeit", how="inner")
fact = fact.rename(columns={"datum_id": "startdatum_id"})

# Merge mit dim_datum (Ende)
fact = fact.merge(datum, left_on="ended_at", right_on="volles_datum_uhrzeit", how="inner")
fact = fact.rename(columns={"datum_id": "enddatum_id"})

# Merge mit dim_station (Start)
fact = fact.merge(stationen, left_on="start_station_id", right_on="station_code", how="inner")
fact = fact.rename(columns={"station_id": "startstation_id"})

# Merge mit dim_station (Ende)
fact = fact.merge(stationen, left_on="end_station_id", right_on="station_code", how="inner")
fact = fact.rename(columns={"station_id": "endstation_id"})

# Ergebnis zusammenfassen und in fact_fahrt schreiben
fact_fahrt = fact[[
    "ride_id", "startdatum_id", "enddatum_id", "kunde_id",
    "startstation_id", "endstation_id", "fahrrad_id"
]].drop_duplicates()

fact_fahrt.to_sql("fact_fahrt", engine, if_exists="append", index=False)

print("Faktentabelle erfolgreich geladen.")
