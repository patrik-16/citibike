CREATE TABLE DIM_KUNDE (
    Kunde_ID SERIAL PRIMARY KEY,  -- K_ID
    Status VARCHAR(20)
);

CREATE TABLE DIM_FAHRRAD (
    Fahrrad_ID SERIAL PRIMARY KEY, -- B_ID
    Typ VARCHAR(50)
);

CREATE TABLE DIM_DATUM (
    Datum_ID SERIAL PRIMARY KEY, -- D_ID
    VollesDatumUhrzeit TIMESTAMP, -- Das genaue Datum abspeichern
    Tag INT,
    Woche INT,
    Monat INT,
    Jahr INT,
    Stunde INT,
    Minute INT,
    Sekunde INT
);

CREATE TABLE DIM_STATION (
    Station_ID SERIAL PRIMARY KEY, -- S_ID
    Name VARCHAR(100),
    Längengrad DOUBLE PRECISION,
    Breitengrad DOUBLE PRECISION
);

CREATE TABLE FACT_FAHRT (
    Fahrt_ID SERIAL PRIMARY KEY, -- R_ID
    Kunde_ID INT REFERENCES DIM_KUNDE(Kunde_ID),
    Fahrrad_ID INT REFERENCES DIM_FAHRRAD(Fahrrad_ID)
    Startdatum_ID INT REFERENCES DIM_DATUM(Datum_ID),  -- Startdatum als FK in FACT
    Enddatum_ID INT REFERENCES DIM_DATUM(Datum_ID),  -- Enddatum als FK in FACT
    Startstation_ID INT REFERENCES DIM_STATION(Station_ID), -- Startstation als FK in FACT
    Endstation_ID INT REFERENCES DIM_STATION(Station_ID), -- Endstation als FK in FACT
);
