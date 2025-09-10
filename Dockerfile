# Base đúng phiên bản Airflow + Python
FROM apache/airflow:2.9.3-python3.12

# PySpark cần Java
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow

# Pinned theo constraints của Airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Cài Python deps (được “khóa” bởi constraints để tránh conflict)
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt --constraint "${CONSTRAINT_URL}"

# Copy code đúng layout dự án của bạn
COPY dags/ /opt/airflow/dags/
COPY etl/  /opt/airflow/etl/
COPY .env  /opt/airflow/.env

ENV PYTHONPATH=/opt/airflow/etl
