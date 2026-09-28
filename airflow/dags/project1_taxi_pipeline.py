from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="project1_taxi_pipeline",
    description="Chicago Taxi Project 1 pipeline",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
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

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="python /opt/db/scripts/ingest_taxi_trips.py",
        execution_timeout=timedelta(minutes=5),
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
        command="spark-submit --jars /opt/jdbc/postgresql.jar /app/spark/jobs/taxi_batch.py",
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
        execution_timeout=timedelta(minutes=10),
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

    check_environment >> ingest_raw
    ingest_raw >> [dbt_transform, spark_batch]
    dbt_transform >> quality_gate
    [quality_gate, spark_batch] >> publish