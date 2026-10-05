# Data Model

## Business Process

A taxi trip completed in the City of Chicago taxi system.

## Fact Grain

`fct_taxi_trips` has one row per unique taxi trip, identified by `trip_id`.

## Dimensional Model

### Fact

- `fct_taxi_trips`

Measures:
- `trip_seconds`
- `trip_miles`
- `fare`
- `tips`
- `tolls`
- `extras`
- `trip_total`

Identifiers / categorical attributes:
- `trip_id`
- `taxi_id`
- `payment_type`

### Dimensions

- `dim_date`
- `dim_location`
- `dim_company`

`dim_date` will be role-played for trip start and trip end dates.

`dim_location` will be role-played for pickup and dropoff locations.


## Key Strategy

### fct_taxi_trips
- Grain: one row per unique taxi trip.
- Primary key: `trip_id`.
- Foreign keys:
  - `start_date_key` -> `dim_date.date_key`
  - `end_date_key` -> `dim_date.date_key`
  - `pickup_location_key` -> `dim_location.location_key`
  - `dropoff_location_key` -> `dim_location.location_key`
  - `company_key` -> `dim_company.company_key`

### dim_date
- Grain: one row per calendar date.
- Primary key: deterministic integer `date_key` in `YYYYMMDD` form.
- Role-playing dimension for trip start date and trip end date.

#### Date Key Strategy

`dim_date` contains one row per calendar date.

- Primary key: deterministic integer `date_key` in `YYYYMMDD` format.
- `date_key = 0` is reserved for the Unknown Date member.
- `start_date_key` and `end_date_key` in `fct_taxi_trips` role-play the same date dimension.
- Initial attributes include full date, year, quarter, month, month name, day of month, day of week, day name, and weekend flag.

### dim_location
- Grain: one row per distinct canonical location represented by community area, census tract, latitude, and longitude.
- Primary key: deterministic surrogate `location_key`.
- Role-playing dimension for pickup and dropoff locations.

#### Location Canonicalization

Pickup and dropoff location attributes are normalized into the same canonical
location structure:

- `community_area`
- `census_tract`
- `centroid_latitude`
- `centroid_longitude`

`dim_location` contains one row per distinct combination of these attributes.

Partially missing location attributes do not make the location unknown. Missing
attributes remain SQL `NULL`, while an explicit sentinel is used only during
deterministic surrogate-key generation so that null positions remain
unambiguous.

If all canonical location attributes are `NULL`, the fact row maps to a single
Unknown Location dimension member rather than using a null foreign key.

`dim_location` is role-played by `pickup_location_key` and
`dropoff_location_key` in `fct_taxi_trips`.

### dim_company
- Grain: one row per distinct taxi company.
- Primary key: deterministic surrogate `company_key`.

#### Company Key Strategy

`dim_company` contains one row per distinct normalized company name.

- Company names are lightly normalized using trimming and empty-string-to-null handling.
- Non-null company names receive deterministic surrogate keys.
- Missing company values map to a single Unknown Company member rather than a null fact foreign key.
- No SCD history or company entity-resolution logic is introduced in v1.

### Scope Decisions
- `taxi_id` remains in the fact table because the source currently provides no descriptive taxi attributes.
- `payment_type` remains a categorical fact attribute rather than a separate dimension in v1.
- `fct_taxi_trips` does not introduce a separate surrogate trip key because `trip_id` already defines the fact grain.