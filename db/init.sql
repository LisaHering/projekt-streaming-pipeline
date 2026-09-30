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
    trip_id             TEXT PRIMARY KEY,
    pickup_datetime     TIMESTAMP,
    dropoff_datetime    TIMESTAMP,
    pickup_zone         INTEGER,
    dropoff_zone        INTEGER,
    passenger_count     INTEGER,
    trip_distance       NUMERIC,
    total_amount        NUMERIC,
    duration_seconds    NUMERIC,
    invalid_reason      TEXT NOT NULL
);

CREATE TABLE aggregates (
    id                  SERIAL PRIMARY KEY,
    metric_name         TEXT NOT NULL,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    dimension           TEXT NOT NULL,
    value               NUMERIC NOT NULL,
    UNIQUE (metric_name, window_start, window_end, dimension)
);

CREATE TABLE rejected_events (
    id                  SERIAL PRIMARY KEY,
    topic               TEXT NOT NULL,
    reason              TEXT NOT NULL,
    raw_payload         TEXT NOT NULL,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX rejected_events_unique
    ON rejected_events (topic, md5(raw_payload));
