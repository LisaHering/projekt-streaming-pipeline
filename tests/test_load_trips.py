"""Unit tests for the validation rules of the trips load script."""

from datetime import datetime

from load_trips import rejection_reason

VALID_ZONES = {1, 2}
PICKUP = datetime(2022, 1, 1, 10, 0)
DROPOFF = datetime(2022, 1, 1, 10, 20)


def make_row(**changes):
    """Return valid row."""
    row = {"passenger_count": 1, "trip_distance": 2.5, "total_amount": 12.0,
           "pickup_location_id": 1, "dropoff_location_id": 2}
    row.update(changes)
    return row


def test_valid_row_is_accepted():
    assert rejection_reason(make_row(), PICKUP, DROPOFF, VALID_ZONES) is None


def test_dropoff_time_before_pickup_is_rejected():
    reason = rejection_reason(make_row(), DROPOFF, PICKUP, VALID_ZONES)
    assert reason == "time_order"


def test_total_amount_zero_is_rejected():
    row = make_row(total_amount=0)
    reason = rejection_reason(row, PICKUP, DROPOFF, VALID_ZONES)
    assert reason == "price"


def test_undefined_zone_is_rejected():
    row = make_row(pickup_location_id=200)
    reason = rejection_reason(row, PICKUP, DROPOFF, VALID_ZONES)
    assert reason == "invalid_zones"
