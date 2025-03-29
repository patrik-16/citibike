import pandas as pd
from sqlalchemy import create_engine

# Verbindung
engine = create_engine("postgresql://myuser:mypassword@localhost:5432/mydatabase")

# --------------------------------------------------------
# 1) STAGING-DATEN LADEN
#    -> Station-ID und Timestamps als String/Datetime beibehalten
# --------------------------------------------------------
df = pd.read_sql("SELECT * FROM stg_citibike_trips", engine)

# Station-ID: als String beibehalten
df["start_station_id"] = df["start_station_id"].astype(str)
df["end_station_id"]   = df["end_station_id"].astype(str)

# Zeitspalten in datetime konvertieren (falls noch nicht geschehen)
df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
df["ended_at"]   = pd.to_datetime(df["ended_at"],   errors="coerce")

# --------------------------------------------------------
# 2) DIMENSIONSTABELLEN LADEN
#    -> station_code auch als String
# --------------------------------------------------------
kunden    = pd.read_sql("SELECT * FROM dim_kunde",    engine)
fahrrad   = pd.read_sql("SELECT * FROM dim_fahrrad",  engine)
datum     = pd.read_sql("SELECT * FROM dim_datum",    engine)

# Nur die notwendigen Spalten laden
stationen = pd.read_sql("SELECT station_id, station_code FROM dim_station", engine)

# station_code als String
stationen["station_code"] = stationen["station_code"].astype(str)

# --------------------------------------------------------
# 3) MERGES (Lookups) => FACT
# --------------------------------------------------------

# A) Merge mit dim_kunde (member_casual -> status)
fact = df.merge(
    kunden,
    left_on="member_casual",
    right_on="status",
    how="inner"
)

# B) Merge mit dim_fahrrad (rideable_type -> typ)
fact = fact.merge(
    fahrrad,
    left_on="rideable_type",
    right_on="typ",
    how="inner"
)

# C) Merge mit dim_datum (Start)
fact = fact.merge(
    datum,
    left_on="started_at",
    right_on="volles_datum_uhrzeit",
    how="inner"
)
fact.rename(columns={"datum_id": "startdatum_id"}, inplace=True)

# D) Merge mit dim_datum (Ende)
fact = fact.merge(
    datum,
    left_on="ended_at",
    right_on="volles_datum_uhrzeit",
    how="inner"
)
fact.rename(columns={"datum_id": "enddatum_id"}, inplace=True)

# E) Merge mit dim_station (Start)
#    Achtung: jetzt sind beide Seiten Strings
fact = fact.merge(
    stationen,
    how="inner",
    left_on="start_station_id",
    right_on="station_code"
)
fact.rename(columns={"station_id": "startstation_id"}, inplace=True)

# F) Merge mit dim_station (Ende)
fact = fact.merge(
    stationen,
    how="inner",
    left_on="end_station_id",
    right_on="station_code"
)
fact.rename(columns={"station_id": "endstation_id"}, inplace=True)

# --------------------------------------------------------
# 4) FINALES FACT-DATAFRAME => fact_fahrt
# --------------------------------------------------------
fact_fahrt = fact[[
    "ride_id",
    "startdatum_id",
    "enddatum_id",
    "kunde_id",
    "startstation_id",
    "endstation_id",
    "fahrrad_id"
]].drop_duplicates()

fact_fahrt.to_sql("fact_fahrt", engine, if_exists="append", index=False)
print("Faktentabelle erfolgreich geladen.")
