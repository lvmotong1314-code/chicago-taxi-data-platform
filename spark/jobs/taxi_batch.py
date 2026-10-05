import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SOURCE_TABLE = "raw.taxi_trips"
OUTPUT_PATH = "/app/data/parquet/taxi_trips_by_date"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("chicago-taxi-batch")
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def parse_args():
    parser = argparse.ArgumentParser(
        description="Process Chicago Taxi Trips for a date interval with Spark"
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


def read_raw_taxi_data(
    spark: SparkSession,
    start_date: str,
    end_date: str,
) -> DataFrame:
    postgres_host = os.environ["POSTGRES_HOST"]
    postgres_port = os.environ["POSTGRES_PORT"]
    postgres_db = os.environ["POSTGRES_DB"]
    postgres_user = os.environ["POSTGRES_USER"]
    postgres_password = os.environ["POSTGRES_PASSWORD"]
    start_timestamp, end_timestamp = get_date_window(
        start_date,
        end_date,
    )

    interval_query = f"""
    (
        SELECT *
        FROM {SOURCE_TABLE}
        WHERE trip_start_timestamp >= '{start_timestamp}'
        AND trip_start_timestamp < '{end_timestamp}'
    ) AS interval_taxi_trips
    """

    jdbc_url = (
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    )

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", interval_query)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def transform_taxi_data(taxi_df: DataFrame) -> DataFrame:
    return (
        taxi_df
        .select(
            "trip_id",
            "trip_start_timestamp",
            "trip_end_timestamp",
            "trip_seconds",
            "trip_miles",
            "pickup_community_area",
            "dropoff_community_area",
            "fare",
            "tips",
            "tolls",
            "extras",
            "trip_total",
            "payment_type",
            "company",
        )
        .withColumn(
            "trip_date",
            F.to_date(F.col("trip_start_timestamp")),
        )
        .withColumn(
            "trip_seconds",
            F.col("trip_seconds").cast("double"),
        )
        .withColumn(
            "trip_miles",
            F.col("trip_miles").cast("double"),
        )
        .withColumn(
            "fare",
            F.col("fare").cast("double"),
        )
        .withColumn(
            "tips",
            F.col("tips").cast("double"),
        )
        .withColumn(
            "tolls",
            F.col("tolls").cast("double"),
        )
        .withColumn(
            "extras",
            F.col("extras").cast("double"),
        )
        .withColumn(
            "trip_total",
            F.col("trip_total").cast("double"),
        )
    )


def write_partitioned_parquet(taxi_df: DataFrame) -> None:
    (
        taxi_df
        .write
        .mode("overwrite")
        .partitionBy("trip_date")
        .parquet(OUTPUT_PATH)
    )


def validate_output(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    expected_count: int,
) -> None:
    parquet_df = (
        spark.read
        .parquet(OUTPUT_PATH)
        .filter(
            (F.col("trip_date") >= F.to_date(F.lit(start_date))) &
            (F.col("trip_date") < F.to_date(F.lit(end_date)))
        )
    )

    actual_count = parquet_df.count()

    if actual_count != expected_count:
        raise ValueError(
            "Parquet row count mismatch: "
            f"interval=[{start_date}, {end_date}), "
            f"expected={expected_count}, "
            f"actual={actual_count}"
        )

    print(
        "Validation passed: "
        f"interval=[{start_date}, {end_date}), "
        f"source_rows={expected_count}, "
        f"parquet_rows={actual_count}"
    )


def main() -> None:
    args = parse_args()
    start_date = args.start_date
    end_date = args.end_date

    spark = create_spark_session()

    try:
        print(
            f"Starting Spark batch "
            f"interval=[{start_date}, {end_date})"
        )

        raw_df = read_raw_taxi_data(
            spark=spark,
            start_date=start_date,
            end_date=end_date,
        )

        raw_count = raw_df.count()

        if raw_count == 0:
            raise ValueError(
                f"No raw taxi trips found for interval "
                f"[{start_date}, {end_date})"
            )


        transformed_df = transform_taxi_data(raw_df)

        write_partitioned_parquet(transformed_df)

        print(
            f"Partitioned Parquet written: "
            f"interval=[{start_date}, {end_date}), "
            f"path={OUTPUT_PATH}"
        )

        validate_output(
            spark=spark,
            start_date=start_date,
            end_date=end_date,
            expected_count=raw_count,
        )

    finally:
        spark.stop()

if __name__ == "__main__":
    main()