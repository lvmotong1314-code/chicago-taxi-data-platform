# Raw Data Contract

## Target

- Database: `taxi_warehouse`
- Schema: `raw`
- Table: `taxi_trips`

## Grain

One row per source taxi trip, identified by `trip_id`.

## Source

City of Chicago Taxi Trips (2024-) via the SODA 3 API.

## Source Fields

Scalar source fields are initially preserved as `TEXT` in the raw layer.

The following nested geographic fields are stored as `JSONB`:

- `pickup_centroid_location`
- `dropoff_centroid_location`

## Ingestion Metadata

Each raw record includes:

- `_ingested_at`: timestamp when the record enters the warehouse
- `_batch_id`: identifier for the ingestion batch
- `_source_name`: identifier for the upstream source

## Key

`trip_id` is the natural source key and primary key of `raw.taxi_trips`.

## Layer Responsibility

Raw preserves the source representation as closely as practical.

Type conversion and basic normalization belong in the dbt staging layer.

## Source Availability

The upstream Chicago Taxi Trips dataset is published with a delayed,
batch-oriented cadence rather than as a real-time daily feed.

Airflow therefore uses a monthly processing interval with a source
availability sensor before ingestion begins.