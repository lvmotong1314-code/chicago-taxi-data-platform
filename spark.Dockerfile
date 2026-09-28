FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.5.5

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}

RUN mkdir -p /opt/jdbc && curl -L https://jdbc.postgresql.org/download/postgresql-42.7.5.jar -o /opt/jdbc/postgresql.jar

ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY spark/jobs/ /app/spark/jobs/