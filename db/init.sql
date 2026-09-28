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

CREATE TABLE aggregates (
    id                  SERIAL PRIMARY KEY,
    metric_name         TEXT NOT NULL,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP NOT NULL,
    dimension           TEXT,
    value               NUMERIC NOT NULL,     
    UNIQUE (metric_name, window_start, window_end, dimension)
);