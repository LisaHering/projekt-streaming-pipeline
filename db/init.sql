CREATE TABLE raw_trips ( 
    pickup_datetime     TIMESTAMP, 
    dropoff_datetime    TIMESTAMP, 
    pickup_zone         INTEGER, 
    dropoff_zone        INTEGER, 
    passenger_count     INTEGER, 
    total_amount        NUMERIC, 
    trip_distance       NUMERIC
); 

CREATE TABLE zones ( 
    location_id         INTEGER PRIMARY KEY, 
    borough             TEXT, 
    zone                TEXT, 
    service_zone        TEXT
);

CREATE TABLE invalid_trips (
    trip_id             INTEGER PRIMARY KEY,
    pickup_datetime     TIMESTAMP,
    dropoff_datetime    TIMESTAMP,
    pickup_zone         INTEGER,
    dropoff_zone        INTEGER,
    passenger_count     INTEGER,
    trip_distance       NUMERIC,
    total_amount        NUMERIC,
    duration_seconds    NUMERIC
);