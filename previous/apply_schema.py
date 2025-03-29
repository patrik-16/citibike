import psycopg2
import sys

def apply_schema(sql_file, db_host, db_port, db_name, db_user, db_password):
    """
    Liest das SQL-Skript aus sql_file ein und führt es gegen die angegebene PostgreSQL-Datenbank aus.
    """
    # Verbindung aufbauen
    connection = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    connection.autocommit = True  # Damit CREATE/ALTER Befehle direkt ausgeführt werden
    cursor = connection.cursor()

    try:
        # SQL-Datei lesen
        with open(sql_file, 'r', encoding='utf-8') as file:
            schema_sql = file.read()

        # SQL-Befehle ausführen
        cursor.execute(schema_sql)
        print(f"Schema aus {sql_file} wurde erfolgreich angewendet!")
    except Exception as e:
        print("Fehler beim Anwenden des Schemas:", e)
    finally:
        cursor.close()
        connection.close()

# if __name__ == "__main__":
#     # Kurzes Beispiel für Aufrufparameter:
#     # python apply_schema.py 01_create_schema.sql localhost 5432 mydatabase myuser mypassword

#     if len(sys.argv) < 7:
#         print("Aufruf: python apply_schema.py <sql_file> <host> <port> <dbname> <dbuser> <dbpassword>")
#         sys.exit(1)

#     sql_file = sys.argv[1]
#     host = sys.argv[2]
#     port = sys.argv[3]
#     db_name = sys.argv[4]
#     db_user = sys.argv[5]
#     db_password = sys.argv[6]

#     apply_schema(sql_file, host, port, db_name, db_user, db_password)
