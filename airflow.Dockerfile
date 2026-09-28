FROM apache/airflow:2.2.2-python3.8

RUN pip install dbt-postgres==1.1.0
RUN pip install "requests==2.31.0" "psycopg[binary]>=3,<4"

RUN pip install "jsonschema==3.2.0" "openapi-schema-validator==0.1.5" "openapi-spec-validator==0.3.1"

RUN pip install --no-cache-dir "apache-airflow-providers-docker==2.6.0"
RUN pip install --no-cache-dir "requests==2.31.0"