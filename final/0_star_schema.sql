CREATE TABLE dim_kunde (
    kunde_id SERIAL PRIMARY KEY,  -- SK (PK)
    status VARCHAR(20) -- BK
);

CREATE TABLE dim_fahrrad (
    fahrrad_id SERIAL PRIMARY KEY, -- SK (PK)
    typ VARCHAR(50) -- BK
);

CREATE TABLE dim_datum (
    datum_id SERIAL PRIMARY KEY, -- SK (PK)
    volles_datum_uhrzeit TIMESTAMP, -- BK
    tag INT,
    woche INT,
    monat INT,
    jahr INT,
    stunde INT,
    minute INT,
    sekunde INT
);

CREATE TABLE dim_station (
    station_id SERIAL PRIMARY KEY, -- SK (PK)
    station_code VARCHAR(50) UNIQUE, -- BK
    name VARCHAR(100),
    laengengrad DOUBLE PRECISION,
    breitengrad DOUBLE PRECISION
);

CREATE TABLE fact_fahrt (
    PRIMARY KEY (startdatum_id, enddatum_id, kunde_id, startstation_id, endstation_id, fahrrad_id),
    ride_id VARCHAR(50) UNIQUE,   -- BK
    startdatum_id INT REFERENCES dim_datum(datum_id),
    enddatum_id INT REFERENCES dim_datum(datum_id),
    kunde_id INT REFERENCES dim_kunde(kunde_id),
    startstation_id INT REFERENCES dim_station(station_id),
    endstation_id INT REFERENCES dim_station(station_id),
    fahrrad_id INT REFERENCES dim_fahrrad(fahrrad_id)
);
