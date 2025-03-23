CREATE TABLE dim_kunde (
    Kunde_ID SERIAL PRIMARY KEY,  -- SK (PK)
    Status VARCHAR(20) -- BK
);

CREATE TABLE dim_fahrrad (
    Fahrrad_ID SERIAL PRIMARY KEY, -- SK (PK)
    Typ VARCHAR(50) -- BK
);

CREATE TABLE dim_datum (
    Datum_ID SERIAL PRIMARY KEY, -- SK (PK)
    VollesDatumUhrzeit TIMESTAMP, -- BK
    Tag INT,
    Woche INT,
    Monat INT,
    Jahr INT,
    Stunde INT,
    Minute INT,
    Sekunde INT
);

CREATE TABLE dim_station (
    Station_ID SERIAL PRIMARY KEY, -- SK (PK)
    Station_Code VARCHAR(50) UNIQUE, -- BK
    Name VARCHAR(100),
    Längengrad DOUBLE PRECISION,
    Breitengrad DOUBLE PRECISION
);

CREATE TABLE fact_fahrt (
    Fahrt_ID SERIAL PRIMARY KEY, -- SK (PK)
    ride_id VARCHAR(50) UNIQUE,   -- BK
    Startdatum_ID INT REFERENCES DIM_DATUM(Datum_ID),
    Enddatum_ID INT REFERENCES DIM_DATUM(Datum_ID),
    Kunde_ID INT REFERENCES DIM_KUNDE(Kunde_ID),
    Startstation_ID INT REFERENCES DIM_STATION(Station_ID),
    Endstation_ID INT REFERENCES DIM_STATION(Station_ID),
    Fahrrad_ID INT REFERENCES DIM_FAHRRAD(Fahrrad_ID)
);