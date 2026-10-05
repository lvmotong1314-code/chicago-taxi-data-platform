import sys
from pathlib import Path
from unittest.mock import MagicMock

from psycopg.types.json import Jsonb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "db" / "scripts"))

from ingest_taxi_trips import (
    SOURCE_NAME,
    fetch_taxi_trips,
    get_date_window,
    prepare_record,
)


def test_prepare_record_adds_ingestion_metadata():
    record = {
        "trip_id": "test-trip-001",
        "taxi_id": "test-taxi-001",
        "pickup_centroid_location": {
            "type": "Point",
            "coordinates": [-87.6270, 41.8810],
        },
        "dropoff_centroid_location": None,
    }

    prepared = prepare_record(
        record,
        "test-batch-001",
    )

    assert prepared["trip_id"] == "test-trip-001"
    assert prepared["taxi_id"] == "test-taxi-001"

    assert prepared["_batch_id"] == "test-batch-001"
    assert prepared["_source_name"] == SOURCE_NAME

    assert isinstance(
        prepared["pickup_centroid_location"],
        Jsonb,
    )

    assert prepared["dropoff_centroid_location"] is None


def test_get_date_window_preserves_half_open_interval():
    start_timestamp, end_timestamp = get_date_window(
        "2026-08-01",
        "2026-09-01",
    )

    assert start_timestamp == "2026-08-01T00:00:00"
    assert end_timestamp == "2026-09-01T00:00:00"


def test_fetch_taxi_trips_builds_correct_query_and_pagination(monkeypatch):
    monkeypatch.setenv(
        "CHICAGO_APP_TOKEN",
        "test-token",
    )

    mock_session = MagicMock()
    mock_response = MagicMock()

    expected_records = [
        {
            "trip_id": "test-trip-001",
            "taxi_id": "test-taxi-001",
        },
        {
            "trip_id": "test-trip-002",
            "taxi_id": "test-taxi-002",
        },
    ]

    mock_response.json.return_value = expected_records
    mock_session.post.return_value = mock_response

    records = fetch_taxi_trips(
        session=mock_session,
        start_date="2026-08-01",
        end_date="2026-08-02",
        page_number=3,
        page_size=5000,
    )

    assert records == expected_records

    mock_session.post.assert_called_once()

    _, kwargs = mock_session.post.call_args

    assert kwargs["headers"]["X-App-Token"] == "test-token"

    payload = kwargs["json"]

    assert payload["page"]["pageNumber"] == 3
    assert payload["page"]["pageSize"] == 5000
    assert payload["includeSynthetic"] is False

    query = payload["query"]

    assert (
        "trip_start_timestamp >= "
        "'2026-08-01T00:00:00'"
        in query
    )

    assert (
        "trip_start_timestamp < "
        "'2026-08-02T00:00:00'"
        in query
    )

    assert "ORDER BY trip_start_timestamp ASC, trip_id ASC" in query

    mock_response.raise_for_status.assert_called_once()