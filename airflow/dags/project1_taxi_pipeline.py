from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

import os
import requests
import pendulum

from airflow.sensors.python import PythonSensor


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def source_data_available(start_date: str, end_date: str) -> bool:
    interval_end = datetime.strptime(end_date, "%Y-%m-%d")

    target_date = (
        interval_end - timedelta(days=1)
    ).date()

    response = requests.post(
        "https://data.cityofchicago.org/api/v3/views/ajtu-isnz/query.json",
        headers={
            "X-App-Token": os.environ["CHICAGO_APP_TOKEN"],
        },
        json={
            "query": "SELECT max(trip_start_timestamp) AS max_ts",
            "page": {
                "pageNumber": 1,
                "pageSize": 1,
            },
            "includeSynthetic": False,
        },
        timeout=30,
    )

    response.raise_for_status()

    records = response.json()

    if not records or not records[0].get("max_ts"):
        print(
            f"Source returned no max timestamp; "
            f"required_date={target_date}"
        )
        return False

    max_timestamp = records[0]["max_ts"]

    max_date = datetime.strptime(
        max_timestamp[:10],
        "%Y-%m-%d",
    ).date()

    print(
        f"Source max date={max_date}, "
        f"required date={target_date}, "
        f"interval=[{start_date}, {end_date})"
    )

    return max_date >= target_date

with DAG(
    dag_id="project1_taxi_pipeline",
    description="Chicago Taxi Project 1 pipeline",
    start_date=pendulum.datetime(
        2024,
        1,
        1,
        tz="America/Chicago",
    ),
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["project1", "chicago-taxi"],
) as dag:


    check_environment = BashOperator(
        task_id="check_environment",
        bash_command=(
            "python --version && "
            "dbt --version && "
            "test -f /opt/db/scripts/ingest_taxi_trips.py && "
            "echo 'Project 1 Airflow environment OK'"

        ),
        execution_timeout=timedelta(minutes=2),
    )

    wait_for_source_data = PythonSensor(
        task_id="wait_for_source_data",
        python_callable=source_data_available,
        op_kwargs={
            "start_date": "{{ data_interval_start | ds }}",
            "end_date": "{{ data_interval_end | ds }}",
        },
        mode="reschedule",
        poke_interval=6 * 60 * 60,
        timeout=35 * 24 * 60 * 60,
        retries=0,
    )

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            "python /opt/db/scripts/ingest_taxi_trips.py "
            "--start-date {{ data_interval_start | ds }} "
            "--end-date {{ data_interval_end | ds }}"
        ),
        execution_timeout=timedelta(minutes=60),
    )

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=(
            "cd /opt/dbt/chicago_taxi && "
            "dbt run --profiles-dir /opt/dbt/chicago_taxi"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    spark_batch = DockerOperator(
        task_id="spark_batch",
        image="chicago-taxi-spark:1.0",
        command=(
            "spark-submit "
            "--jars /opt/jdbc/postgresql.jar "
            "/app/spark/jobs/taxi_batch.py "
            "--start-date {{ data_interval_start | ds }} "
            "--end-date {{ data_interval_end | ds }}"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="project1-network",
        environment={
            "POSTGRES_PASSWORD": "docker",
            "POSTGRES_USER": "docker",
            "POSTGRES_DB": "taxi_warehouse",
            "POSTGRES_HOST": "postgres_local",
            "POSTGRES_PORT": "5432",
        },
        mounts=[
            Mount(
                source="taxi-parquet-data",
                target="/app/data/parquet",
                type="volume",
            )
        ],
        mount_tmp_dir=False,
        auto_remove=True,
        execution_timeout=timedelta(minutes=30),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=(
            "cd /opt/dbt/chicago_taxi && "
            "dbt test --profiles-dir /opt/dbt/chicago_taxi"
        ),
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    publish = BashOperator(
        task_id="publish",
        bash_command="echo 'All quality gates passed. Chicago Taxi marts and partitioned Parquet outputs are ready for downstream consumption.'",
        retries=0,
    )

    check_environment >> wait_for_source_data >> ingest_raw
    ingest_raw >> [dbt_transform, spark_batch]
    dbt_transform >> quality_gate
    [quality_gate, spark_batch] >> publish