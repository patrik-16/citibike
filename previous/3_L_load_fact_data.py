import pandas as pd
from sqlalchemy import create_engine

# ---(1) VERBINDUNGSDATEN ANLEGEN---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

# ---(2) ENGINE ERZEUGEN---
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ------------------------------------------------------------------------------
# ---(3) STAGING-DATEN LADEN UND MINIMALE TRANSFORMS)---
#    Du kannst hier denselben Code nutzen wie in 2_T_transform_data.py,
#    wenn du dein DataFrame nicht gesondert gespeichert hast.
#    Die Idee: Wir stellen sicher, dass Station-IDs und Datumsangaben
#    wieder im passenden Format sind, um korrekt zu joinen.
# ------------------------------------------------------------------------------

df = pd.read_sql("SELECT * FROM stg_citibike_trips", engine)
print("Staging-Daten (Roh) geladen:\n", df.head())

# Beispiel: lowercase bei member_casual + rideable_type
df["member_casual"] = df["member_casual"].str.lower()
df["rideable_type"] = df["rideable_type"].str.lower()

# Station-ID in Integer umwandeln
df["start_station_id"] = (df["start_station_id"].astype(str)
                          .str.replace(".", "", regex=False))
df["start_station_id"] = pd.to_numeric(df["start_station_id"], errors="coerce").fillna(0).astype(int)

df["end_station_id"] = (df["end_station_id"].astype(str)
                        .str.replace(".", "", regex=False))
df["end_station_id"] = pd.to_numeric(df["end_station_id"], errors="coerce").fillna(0).astype(int)

# Zeitspalten parsen
df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
df["ended_at"]   = pd.to_datetime(df["ended_at"],   errors="coerce")

print("Nach Minimal-Transform:\n", df.head())

# ------------------------------------------------------------------------------
# ---(4) DIMENSIONSTABELLEN LADEN---
#    Wir benötigen jeweils Surrogate Key (z. B. kunde_id) und Business Key
#    (z. B. status).
# ------------------------------------------------------------------------------

df_dim_kunde = pd.read_sql("SELECT kunde_id, status FROM dim_kunde", engine)
df_dim_fahrrad = pd.read_sql("SELECT fahrrad_id, typ FROM dim_fahrrad", engine)
df_dim_station = pd.read_sql("SELECT station_id, station_code FROM dim_station", engine)
df_dim_datum  = pd.read_sql("SELECT datum_id, volles_datum_uhrzeit FROM dim_datum", engine)

# ------------------------------------------------------------------------------
# ---(5) LOOKUPS VIA JOINS / MERGES)---
#    A) Kunde -> Verknüpfung über member_casual <-> status
# ------------------------------------------------------------------------------

df_fact = df.merge(df_dim_kunde,
                   how="left",
                   left_on="member_casual",
                   right_on="status")

#    B) Fahrrad -> Verknüpfung über rideable_type <-> typ
df_fact = df_fact.merge(df_dim_fahrrad,
                        how="left",
                        left_on="rideable_type",
                        right_on="typ")

#    C) Stationen -> start_station_id / end_station_id
#       wir joinen 2x gegen df_dim_station:
#       1. Start-Station
df_fact = df_fact.merge(df_dim_station,
                        how="left",
                        left_on="start_station_id",
                        right_on="station_code")
#       Die gemergte station_id nennen wir startstation_id
df_fact.rename(columns={"station_id": "startstation_id"}, inplace=True)
df_fact.drop(columns=["station_code"], inplace=True)  # station_code (join-Spalte) kannst du löschen oder behalten

#       2. End-Station
df_fact = df_fact.merge(df_dim_station,
                        how="left",
                        left_on="end_station_id",
                        right_on="station_code")
df_fact.rename(columns={"station_id": "endstation_id"}, inplace=True)
df_fact.drop(columns=["station_code"], inplace=True)

#    D) Datum -> startdatum_id, enddatum_id
#       Da wir in dim_datum volles_datum_uhrzeit haben, mappen wir:
#         started_at -> volles_datum_uhrzeit -> datum_id
#         ended_at   -> volles_datum_uhrzeit -> datum_id
df_fact = df_fact.merge(df_dim_datum,
                        how="left",
                        left_on="started_at",
                        right_on="volles_datum_uhrzeit")
df_fact.rename(columns={"datum_id": "startdatum_id"}, inplace=True)
df_fact.drop(columns=["volles_datum_uhrzeit"], inplace=True)

df_fact = df_fact.merge(df_dim_datum,
                        how="left",
                        left_on="ended_at",
                        right_on="volles_datum_uhrzeit")
df_fact.rename(columns={"datum_id": "enddatum_id"}, inplace=True)
df_fact.drop(columns=["volles_datum_uhrzeit"], inplace=True)

print("\nPreview df_fact nach allen Joins:\n", df_fact.head())

# ------------------------------------------------------------------------------
# ---(6) FINALE SPALTEN AUSWÄHLEN UND INS DWH LADEN (fact_fahrt)---
# ------------------------------------------------------------------------------

# Zu diesem Zeitpunkt hast du in df_fact alle Surrogate Keys und die ride_id.
# Du kannst optional noch Measures (z. B. Fahrtdauer in Minuten) berechnen,
# oder nur die Minimalspalten übernehmen, die du in fact_fahrt brauchst.

fact_cols = [
    # "fahrt_id"  # Falls du 'fahrt_id' als SERIAL PK in der DB haben willst, hier NICHT angeben
    "ride_id",
    "startdatum_id",
    "enddatum_id",
    "kunde_id",
    "startstation_id",
    "endstation_id",
    "fahrrad_id"
]

# Falls du versehentlich doppelte ride_id hast, kann es Sinn ergeben:
df_fact_final = df_fact[fact_cols].drop_duplicates()

# Laden in die Faktentabelle
df_fact_final.to_sql("fact_fahrt", engine, if_exists="append", index=False)

print("\n--- FACT_FAHRT wurde befüllt! ---")
print(f"Anzahl Zeilen für fact_fahrt: {len(df_fact_final)}")
