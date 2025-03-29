import pandas as pd
import psycopg2

# ==========================
# 1️⃣ PostgreSQL Verbindung
# ==========================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    print("✅ Verbindung zur Datenbank erfolgreich!")
except Exception as e:
    print(f"❌ Fehler bei der Verbindung zur Datenbank: {e}")
    exit()

cur = conn.cursor()

# ==========================
# 2️⃣ CSV-Datei einlesen (Extract)
# ==========================
csv_file = "202501-citibike-tripdata_1.csv"
df = pd.read_csv(csv_file, dtype=str, low_memory=False)

# ==========================
# 3️⃣ Daten transformieren & bereinigen (Transform & Clean)
# ==========================
df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')

df['Tag'] = df['started_at'].dt.day
df['Woche'] = df['started_at'].dt.isocalendar().week
df['Monat'] = df['started_at'].dt.month
df['Jahr'] = df['started_at'].dt.year
df['Stunde'] = df['started_at'].dt.hour
df['Minute'] = df['started_at'].dt.minute
df['Sekunde'] = df['started_at'].dt.second

df['start_station_name'] = df['start_station_name'].fillna('Unknown')
df['end_station_name'] = df['end_station_name'].fillna('Unknown')

# ==========================
# 4️⃣ Daten in das Data Warehouse laden (Load)
# ==========================

# 🚲 Fahrräder (DIM_FAHRRAD)
bike_types = df['rideable_type'].dropna().unique()
for bike in bike_types:
    cur.execute("INSERT INTO DIM_FAHRRAD (Typ) VALUES (%s) ON CONFLICT (Typ) DO NOTHING;", (bike,))

# 👥 Kunden (DIM_KUNDE)
customer_types = df['member_casual'].dropna().unique()
for customer in customer_types:
    cur.execute("INSERT INTO DIM_KUNDE (Status) VALUES (%s) ON CONFLICT (Status) DO NOTHING;", (customer,))

# 📍 Stationen (DIM_STATION)
stations = df[['start_station_name', 'start_lat', 'start_lng']].drop_duplicates()
for _, row in stations.iterrows():
    cur.execute("""
        INSERT INTO DIM_STATION (Name, Laengengrad, Breitengrad) 
        VALUES (%s, %s, %s) 
        ON CONFLICT (Name) DO NOTHING;
    """, (row['start_station_name'], row['start_lat'], row['start_lng']))

# ==========================
# 5️⃣ Zeitdimension (DIM_DATUM) KORREKT befüllen
# ==========================
unique_timestamps = pd.concat([df['started_at'], df['ended_at']]).drop_duplicates().dropna()

for timestamp in unique_timestamps:
    cur.execute("""
        INSERT INTO DIM_DATUM (VollesDatumUhrzeit, Tag, Woche, Monat, Jahr, Stunde, Minute, Sekunde)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (VollesDatumUhrzeit) DO NOTHING;
    """, (timestamp, timestamp.day, timestamp.isocalendar()[1], timestamp.month, timestamp.year, timestamp.hour, timestamp.minute, timestamp.second))

# ==========================
# 6️⃣ Faktentabelle (FACT_FAHRT) befüllen
# ==========================
for _, row in df.iterrows():
    # 🔹 Startdatum_ID abrufen
    cur.execute("SELECT Datum_ID FROM DIM_DATUM WHERE VollesDatumUhrzeit = %s;", (row['started_at'],))
    startdatum = cur.fetchone()
    
    # 🔹 Enddatum_ID abrufen
    cur.execute("SELECT Datum_ID FROM DIM_DATUM WHERE VollesDatumUhrzeit = %s;", (row['ended_at'],))
    enddatum = cur.fetchone()
    
    # 🔹 Kunde_ID abrufen
    cur.execute("SELECT Kunde_ID FROM DIM_KUNDE WHERE Status = %s;", (row['member_casual'],))
    kunde = cur.fetchone()
    
    # 🔹 Startstation_ID abrufen
    cur.execute("SELECT Station_ID FROM DIM_STATION WHERE Name = %s;", (row['start_station_name'],))
    startstation = cur.fetchone()
    
    # 🔹 Endstation_ID abrufen
    cur.execute("SELECT Station_ID FROM DIM_STATION WHERE Name = %s;", (row['end_station_name'],))
    endstation = cur.fetchone()
    
    # 🔹 Fahrrad_ID abrufen
    cur.execute("SELECT Fahrrad_ID FROM DIM_FAHRRAD WHERE Typ = %s;", (row['rideable_type'],))
    fahrrad = cur.fetchone()
    
    # ✅ Falls alle IDs vorhanden sind → in FACT_FAHRT einfügen
    if startdatum and enddatum and kunde and startstation and endstation and fahrrad:
        cur.execute("""
            INSERT INTO FACT_FAHRT (Startdatum_ID, Enddatum_ID, Kunde_ID, Startstation_ID, Endstation_ID, Fahrrad_ID)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (startdatum[0], enddatum[0], kunde[0], startstation[0], endstation[0], fahrrad[0]))

print("✅ Daten erfolgreich geladen!")
cur.close()
conn.close()
