"""Unit tests for validation and business rules of the Flink job."""

import json
from datetime import datetime

from flink_job import (JoinTripsFunction, TripStatsAggregate,
                       dropoff_time_to_time_of_day, dropoff_time_to_week,
                       validate_event, parse_event)


def make_pickup(**changes):
    """Return valid pickup event."""
    event = {"trip_id": "t1", "event_type": "pickup",
             "pickup_datetime": "2022-01-01T10:00:00",
             "pickup_zone": 1, "passenger_count": 2}
    event.update(changes)
    return event


def make_dropoff(**changes):
    """Return valid dropoff event."""
    event = {"trip_id": "t1", "event_type": "dropoff",
             "dropoff_datetime": "2022-01-01T10:20:00",
             "dropoff_zone": 2, "trip_distance": 2.5, "total_amount": 12.0}
    event.update(changes)
    return event


def test_valid_pickup_is_accepted():
    assert validate_event(make_pickup(), "pickup") is None


def test_pickup_with_nine_passengers_is_rejected():
    reason = validate_event(make_pickup(passenger_count=9), "pickup")
    assert reason == "invalid_passenger_count"


def test_pickup_undefined_event_type_is_rejected():
    reason = validate_event(make_pickup(event_type="trip"), "pickup")
    assert reason == "unexpected_event_type"


def test_valid_dropoff_is_accepted():
    assert validate_event(make_dropoff(), "dropoff") is None


def test_dropoff_with_negative_distance_is_rejected():
    reason = validate_event(make_dropoff(trip_distance=-2.3), "dropoff")
    assert reason == "invalid_trip_distance"


def test_dropoff_in_pickup_topic_is_rejected():
    reason = validate_event(make_dropoff(), "pickup")
    assert reason == "unexpected_event_type"


def test_trip_of_exactly_three_hours_is_valid():
    pickup = make_pickup(pickup_datetime="2022-01-01T09:00:00")
    dropoff = make_dropoff(dropoff_datetime="2022-01-01T12:00:00")
    trip = JoinTripsFunction()._build_trip(pickup, dropoff)
    assert trip["is_valid"] is True
    assert trip["duration_seconds"] == 60 * 60 * 3


def test_trip_one_second_over_three_hours_is_invalid():
    pickup = make_pickup(pickup_datetime="2022-01-01T09:00:00")
    dropoff = make_dropoff(dropoff_datetime="2022-01-01T12:00:01")
    trip = JoinTripsFunction()._build_trip(pickup, dropoff)
    assert trip["invalid_reason"] == "duration_over_3h"


def test_trip_with_zero_duration_is_invalid():
    pickup = make_pickup(pickup_datetime="2022-01-01T09:00:00")
    dropoff = make_dropoff(dropoff_datetime="2022-01-01T09:00:00")
    trip = JoinTripsFunction()._build_trip(pickup, dropoff)
    assert trip["invalid_reason"] == "non_positive_duration"


def test_invalid_json_becomes_rejected_record():
    record = parse_event("{not json", "pickup_events", "pickup")
    assert record["record_type"] == "rejected"
    assert record["reason"] == "invalid_json"
    assert record["raw_payload"] == "{not json"


def test_valid_json_becomes_event_record():
    raw = json.dumps(make_pickup())
    record = parse_event(raw, "pickup_events", "pickup")
    assert record["record_type"] == "event"


def test_time_of_day_switches_at_five():
    assert dropoff_time_to_time_of_day(datetime(2022, 1, 1, 4, 59)) == "night"
    assert dropoff_time_to_time_of_day(datetime(2022, 1, 1, 5, 0)) == "morning"


def test_first_of_january_2022_belongs_to_last_week_of_2021():
    assert dropoff_time_to_week(datetime(2022, 1, 1)) == "2021-W52"


def test_aggregate_sums_two_trips():
    aggregate = TripStatsAggregate()
    acc = aggregate.create_accumulator()
    acc = aggregate.add({"passenger_count": 2, "trip_distance": 2.5,
                         "total_amount": 12.0}, acc)
    acc = aggregate.add({"passenger_count": 4, "trip_distance": 7.3,
                         "total_amount": 65.0}, acc)
    assert aggregate.get_result(acc) == (2, 6, 9.8, 77.0)
