CREATE TABLE raw_trips (
    trip_id             INTEGER PRIMARY KEY, 
    pickup_datetime     TIMESTAMP, 
    dropoff_datetime    TIMESTAMP, 
    pickup_zone         INTEGER, 
    dropoff_zone        INTEGER, 
    passenger_count     INTEGER, 
    total_amount        NUMERIC, 
    trip_distance       NUMERIC
); 