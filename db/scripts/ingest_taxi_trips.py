
from uuid import uuid4
from datetime import datetime, timedelta
import argparse
import logging
import os

import requests
import psycopg
from psycopg.types.json import Jsonb

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_URL = "https://data.cityofchicago.org/api/v3/views/ajtu-isnz/query.json"
SOURCE_NAME = "chicago_taxi_trips_2024_plus"
SOURCE_COLUMNS = (
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_census_tract",
    "dropoff_census_tract",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
)

INSERT_SQL = """
    INSERT INTO raw.taxi_trips (
        trip_id,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        pickup_census_tract,
        dropoff_census_tract,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        payment_type,
        company,
        pickup_centroid_latitude,
        pickup_centroid_longitude,
        pickup_centroid_location,
        dropoff_centroid_latitude,
        dropoff_centroid_longitude,
        dropoff_centroid_location,
        _batch_id,
        _source_name
    )
    VALUES (
        %(trip_id)s,
        %(taxi_id)s,
        %(trip_start_timestamp)s,
        %(trip_end_timestamp)s,
        %(trip_seconds)s,
        %(trip_miles)s,
        %(pickup_census_tract)s,
        %(dropoff_census_tract)s,
        %(pickup_community_area)s,
        %(dropoff_community_area)s,
        %(fare)s,
        %(tips)s,
        %(tolls)s,
        %(extras)s,
        %(trip_total)s,
        %(payment_type)s,
        %(company)s,
        %(pickup_centroid_latitude)s,
        %(pickup_centroid_longitude)s,
        %(pickup_centroid_location)s,
        %(dropoff_centroid_latitude)s,
        %(dropoff_centroid_longitude)s,
        %(dropoff_centroid_location)s,
        %(_batch_id)s,
        %(_source_name)s
    )
    ON CONFLICT (trip_id) DO NOTHING
"""


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


def get_app_token():
    token = os.getenv("CHICAGO_APP_TOKEN")

    if not token:
        raise RuntimeError("CHICAGO_APP_TOKEN is not set")

    return token


def get_db_config():
    config = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    }

    missing = [
        key
        for key, value in config.items()
        if not value
    ]

    if missing:
        logger.error(
            "Missing PostgreSQL configuration: %s",
            ", ".join(missing),
        )
        raise RuntimeError(
            f"Missing PostgreSQL configuration: {', '.join(missing)}"
        )

    return config

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest Chicago Taxi Trips for a date interval"
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Interval start date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="Interval end date in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    try:
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            "--start-date and --end-date must use YYYY-MM-DD format"
        ) from exc

    if end <= start:
        raise ValueError("--end-date must be after --start-date")

    return args

def get_date_window(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    return (
        start.strftime("%Y-%m-%dT%H:%M:%S"),
        end.strftime("%Y-%m-%dT%H:%M:%S"),
    )


def create_http_session():
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
    )

    session = requests.Session()

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry),
    )

    return session

def fetch_taxi_trips(
    session,
    start_date,
    end_date, 
    page_number=1, 
    page_size=5000
):
    token = get_app_token()
    start_timestamp, end_timestamp = get_date_window(
        start_date,
        end_date,
    )

    headers = {
        "X-App-Token": token,
    }

    payload = {
        "query": (
        "SELECT * "
        f"WHERE trip_start_timestamp >= '{start_timestamp}' "
        f"AND trip_start_timestamp < '{end_timestamp}' "
        "ORDER BY trip_start_timestamp ASC, trip_id ASC"
        ),
        "page": {
            "pageNumber": page_number,
            "pageSize": page_size,
        },
        "includeSynthetic": False,
    }

    logger.info(
        "Requesting Chicago Taxi Trips start_date=%s end_date=%s page=%s page_size=%s",
        start_date,
        end_date,
        page_number,
        page_size,
    )

    response = session.post(
        API_URL,
        headers=headers,
        json=payload,
        timeout=(10, 120),
    )

    response.raise_for_status()

    records = response.json()

    if not isinstance(records, list):
        raise TypeError(
            f"Expected API response to be a list, got {type(records).__name__}"
        )

    logger.info("Received %s taxi trip records", len(records))

    return records

def prepare_record(record, batch_id):
    prepared = {
        column: record.get(column)
        for column in SOURCE_COLUMNS
    }

    if prepared["pickup_centroid_location"] is not None:
        prepared["pickup_centroid_location"] = Jsonb(
            prepared["pickup_centroid_location"]
        )

    if prepared["dropoff_centroid_location"] is not None:
        prepared["dropoff_centroid_location"] = Jsonb(
            prepared["dropoff_centroid_location"]
        )

    prepared["_batch_id"] = batch_id
    prepared["_source_name"] = SOURCE_NAME

    return prepared


def load_taxi_trips(records, batch_id):
    if not records:
        logger.info("No records to load")
        return None, 0

    db_config = get_db_config()

    prepared_records = [
        prepare_record(record, batch_id)
        for record in records
    ]

    logger.info(
        "Loading %s records into raw.taxi_trips batch_id=%s",
        len(prepared_records),
        batch_id,
    )

    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                INSERT_SQL,
                prepared_records,
            )

            inserted_count = cur.rowcount

    logger.info(
        "Page load complete batch_id=%s fetched=%s inserted=%s skipped=%s",
        batch_id,
        len(records),
        inserted_count,
        len(records) - inserted_count,
    )

    return batch_id, inserted_count


if __name__ == "__main__":
    args = parse_args()

    start_date = args.start_date
    end_date = args.end_date
    page_size = 5000
    page_number = 1
    batch_id = str(uuid4())

    total_fetched = 0
    total_inserted = 0

    logger.info(
        "Starting interval ingestion start_date=%s end_date=%s batch_id=%s",
        start_date,
        end_date,
        batch_id,
    )

    start = datetime.strptime(
        args.start_date,
        "%Y-%m-%d",
    )

    end = datetime.strptime(
        args.end_date,
        "%Y-%m-%d",
    )

    session = create_http_session()

    current_date = start

    while current_date < end:
        next_date = current_date + timedelta(days=1)

        daily_start = current_date.strftime("%Y-%m-%d")
        daily_end = next_date.strftime("%Y-%m-%d")

        page_number = 1

        while True:
            trips = fetch_taxi_trips(
                session=session,
                start_date=daily_start,
                end_date=daily_end,
                page_number=page_number,
                page_size=page_size,
            )

            if not trips:
                logger.info(
                    "No records returned start_date=%s end_date=%s page=%s, stopping",
                    start_date,
                    end_date,
                    page_number,
                )
                break

            _, inserted_count = load_taxi_trips(
                trips,
                batch_id=batch_id,
            )

            total_fetched += len(trips)
            total_inserted += inserted_count

            logger.info(
                "Page complete start_date=%s end_date=%s page=%s batch_id=%s fetched=%s inserted=%s skipped=%s",
                start_date,
                end_date,
                page_number,
                batch_id,
                len(trips),
                inserted_count,
                len(trips) - inserted_count,
            )

            page_number += 1
            
        current_date = next_date

    if total_fetched == 0:
        raise ValueError(
            f"No taxi trips fetched for interval "
            f"[{start_date}, {end_date})"
        )

    logger.info(
        "Interval ingestion complete start_date=%s end_date=%s batch_id=%s total_fetched=%s total_inserted=%s total_skipped=%s",
        start_date,
        end_date,
        batch_id,
        total_fetched,
        total_inserted,
        total_fetched - total_inserted,
    )