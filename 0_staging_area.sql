CREATE TABLE stg_citibike_trips (
    ride_id VARCHAR(50),                   -- Beispiel: '56BD148A05E26915' (alphanumerisch)
    rideable_type VARCHAR(20),             -- Beispiel: 'electric_bike' (auch 'docked_bike' etc.)
    started_at TIMESTAMP,                  -- Beispiel: '01.01.2025 22:19' Startzeit der Fahrt
    ended_at TIMESTAMP,                    -- Beispiel: '01.01.2025 22:23' Endzeit der Fahrt
    start_station_name VARCHAR(255),       -- z.B. 'W 36 St & 7 Ave' Name der Startstation
    start_station_id VARCHAR(50),          -- z.B. '6483.06' Stationen-ID (FLOAT-Werte → VARCHAR!)
    end_station_name VARCHAR(255),         -- z.B. 'W 24 St & 7 Ave' Name der Endstation
    end_station_id VARCHAR(50),            -- z.B. '6257.03' Stationen-ID (FLOAT-Werte → VARCHAR!)
    start_lat DOUBLE PRECISION,            -- Latitude Start z.B.  40752149 -> ggf. zu 40.752149 konvertieren
    start_lng DOUBLE PRECISION,            -- Longitude Start z.B. -73989539 -> ggf. zu -73.989539 konvertieren
    end_lat DOUBLE PRECISION,              -- Latitude Ende z.B. 4074487634 -> ggf. 40.74487634
    end_lng DOUBLE PRECISION,              -- Longitude Ende z.B. -7399529885 -> ggf. -73.99529885
    member_casual VARCHAR(20)              -- z.B. 'member' oder 'casual'
);