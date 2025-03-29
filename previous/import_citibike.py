import pandas as pd
import psycopg2

# PostgreSQL-Verbindungsdetails (anpassen!)
DB_HOST = "localhost"  # Falls Docker: localhost, weil der Container den Port mapped
DB_PORT = "5432"       # Standard-PostgreSQL-Port
DB_NAME = "mydatabase" # Name der Datenbank
DB_USER = "myuser"     # Dein Benutzername
DB_PASSWORD = "mypassword" # Dein Passwort

# CSV-Datei einlesen (Pfad anpassen!)
csv_file = "citibike.csv"

# Verbindung zur PostgreSQL-Datenbank herstellen
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    print("✅ Verbindung zu PostgreSQL erfolgreich!")

    # CSV-Datei mit Pandas einlesen
    df = pd.read_csv(csv_file)

    # Koordinaten konvertieren (von Ganzzahl zu Dezimalgrad)
    df['start_lat'] = df['start_lat'] / 1000000.0
    df['start_lng'] = df['start_lng'] / 1000000.0
    df['end_lat'] = df['end_lat'] / 1000000.0
    df['end_lng'] = df['end_lng'] / 1000000.0

    # SQL-Query für den Import
    insert_query = """
    INSERT INTO citibike_trips (ride_id, rideable_type, started_at, ended_at, start_station_name,
                                start_station_id, end_station_name, end_station_id, start_lat, start_lng, 
                                end_lat, end_lng, member_casual)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Datenbank-Import Zeile für Zeile
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["ride_id"], row["rideable_type"], row["started_at"], row["ended_at"],
            row["start_station_name"], row["start_station_id"], row["end_station_name"],
            row["end_station_id"], row["start_lat"], row["start_lng"], row["end_lat"],
            row["end_lng"], row["member_casual"]
        ))

    # Änderungen speichern und Verbindung schließen
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Daten erfolgreich importiert!")

except Exception as e:
    print(f"❌ Fehler beim Import: {e}")
