CREATE TABLE IF NOT EXISTS DIM_KUNDE (
    Kunde_ID SERIAL PRIMARY KEY,
    Status VARCHAR(20) UNIQUE,
    Gültig_von TIMESTAMP DEFAULT now(), --?
    Gültig_bis TIMESTAMP DEFAULT '9999-12-31' --?
);

CREATE TABLE IF NOT EXISTS DIM_FAHRRAD (
    Fahrrad_ID SERIAL PRIMARY KEY,
    Typ VARCHAR(50) UNIQUE,
    Gültig_von TIMESTAMP DEFAULT now(), --?
    Gültig_bis TIMESTAMP DEFAULT '9999-12-31' --?
);

CREATE TABLE IF NOT EXISTS DIM_DATUM (
    Datum_ID SERIAL PRIMARY KEY,
    VollesDatumUhrzeit TIMESTAMP UNIQUE,
    Tag INT,
    Woche INT,
    Monat INT,
    Jahr INT,
    Stunde INT,
    Minute INT,
    Sekunde INT
);

CREATE TABLE IF NOT EXISTS DIM_STATION (
    Station_ID SERIAL PRIMARY KEY,
    Name VARCHAR(168) UNIQUE,
    Laengengrad DOUBLE PRECISION,
    Breitengrad DOUBLE PRECISION
);

-- ===============================
-- 2️⃣ ERSTELLE DIE FAKTENTABELLE (FACT_FAHRT)
-- ===============================
CREATE TABLE IF NOT EXISTS FACT_FAHRT (
    Enddatum_ID INT,
    Startdatum_ID INT NOT NULL,
    Kunde_ID INT NOT NULL,
    Startstation_ID INT NOT NULL,
    Endstation_ID INT NOT NULL,
    Fahrrad_ID INT NOT NULL,

    -- 🔹 Fremdschlüssel
    PRIMARY KEY (Kunde_ID, Fahrrad_ID, Startdatum_ID, Enddatum_ID, Startstation_ID, Endstation_ID)
    CONSTRAINT fk_fact_fahrt_startdatum FOREIGN KEY (Startdatum_ID) REFERENCES DIM_DATUM(Datum_ID),
    CONSTRAINT fk_fact_fahrt_enddatum FOREIGN KEY (Enddatum_ID) REFERENCES DIM_DATUM(Datum_ID),
    CONSTRAINT fk_fact_fahrt_kunde FOREIGN KEY (Kunde_ID) REFERENCES DIM_KUNDE(Kunde_ID),
    CONSTRAINT fk_fact_fahrt_startstation FOREIGN KEY (Startstation_ID) REFERENCES DIM_STATION(Station_ID),
    CONSTRAINT fk_fact_fahrt_endstation FOREIGN KEY (Endstation_ID) REFERENCES DIM_STATION(Station_ID),
    CONSTRAINT fk_fact_fahrt_fahrrad FOREIGN KEY (Fahrrad_ID) REFERENCES DIM_FAHRRAD(Fahrrad_ID)
);


-- ===============================
-- 3️⃣ ANALYSEFRAGEN (Business Queries)
-- ===============================
""
-- 📌 1️⃣ Wann und wo gibt es Stoßzeiten?
SELECT d.Stunde, s.Name AS Start_Station, COUNT(*) AS Fahrten
FROM FACT_FAHRT f
JOIN DIM_DATUM d ON f.Startdatum_ID = d.Datum_ID
JOIN DIM_STATION s ON f.Startstation_ID = s.Station_ID
GROUP BY d.Stunde, s.Name
ORDER BY Fahrten DESC
LIMIT 10;

-- 📌 2️⃣ Welche Kundentypen nutzen Citi Bike am häufigsten und wann?
SELECT d.Stunde, k.Status, COUNT(*) AS Fahrten
FROM FACT_FAHRT f
JOIN DIM_DATUM d ON f.Startdatum_ID = d.Datum_ID
JOIN DIM_KUNDE k ON f.Kunde_ID = k.Kunde_ID
GROUP BY d.Stunde, k.Status
ORDER BY Fahrten DESC
LIMIT 10;

-- 📌 3️⃣ Welcher Fahrradtyp wird am häufigsten genutzt?
SELECT f.Typ, COUNT(*) AS Fahrten
FROM FACT_FAHRT fa
JOIN DIM_FAHRRAD f ON fa.Fahrrad_ID = f.Fahrrad_ID
GROUP BY f.Typ
ORDER BY Fahrten DESC;
