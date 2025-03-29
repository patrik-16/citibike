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

# Merge mit DIM_KUNDE
fact = df.merge(kunden, left_on="member_casual", right_on="Status", how="inner")

# Merge mit DIM_FAHRRAD
fact = fact.merge(fahrrad, left_on="rideable_type", right_on="Typ", how="inner")

# Merge mit DIM_DATUM (Start und Ende jeweils separat)
fact = fact.merge(datum, left_on="started_at", right_on="VollesDatumUhrzeit", how="inner")
fact = fact.rename(columns={"Datum_ID": "Startdatum_ID"})
fact = fact.merge(datum, left_on="ended_at", right_on="VollesDatumUhrzeit", how="inner")
fact = fact.rename(columns={"Datum_ID": "Enddatum_ID"})

# Merge mit DIM_STATION (Start und Ziel)
fact = fact.merge(stationen, left_on="start_station_id", right_on="Station_Code", how="inner")
fact = fact.rename(columns={"Station_ID": "Startstation_ID"})
fact = fact.merge(stationen, left_on="end_station_id", right_on="Station_Code", how="inner")
fact = fact.rename(columns={"Station_ID": "Endstation_ID"})

# Ergebnis zusammenfassen und in FACT_FAHRT schreiben
fact_fahrt = fact[[
    "ride_id", "Startdatum_ID", "Enddatum_ID", "Kunde_ID",
    "Startstation_ID", "Endstation_ID", "Fahrrad_ID"
]].drop_duplicates()

fact_fahrt.to_sql("fact_fahrt", engine, if_exists="append", index=False)

print("Faktentabelle erfolgreich geladen.")