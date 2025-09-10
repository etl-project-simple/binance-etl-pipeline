# etl/load.py
import os, logging, psycopg2
from typing import Dict
from dotenv import load_dotenv
from pyspark.sql import DataFrame
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load ENV from ENV_FILE (set by DAG), fallback to /opt/airflow/.env
load_dotenv(os.getenv("ENV_FILE", "/opt/airflow/.env"))

# AWS & S3
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
S3_BUCKET  = os.getenv("AWS_S3_BUCKET")         
S3_PREFIX  = os.getenv("AWS_S3_PREFIX", "data_clean")   

# Redshift
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB   = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PWD  = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_IAM_ROLE_ARN = os.getenv("REDSHIFT_IAM_ROLE_ARN")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")


def _rs_conn():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PWD,
    )


def _copy_sql(table: str, s3_uri: str) -> str:
    # COPY from S3 (Parquet) into Redshift via IAM Role
    return f"""
        COPY {REDSHIFT_SCHEMA}.{table}
        FROM '{s3_uri}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        FORMAT AS PARQUET
        REGION '{AWS_REGION}';
    """


def load_to_s3_and_redshift(tables: Dict[str, DataFrame], *, coalesce_n: int = 8) -> None:
    """
    tables: dict of table_name -> transformed DataFrame
    - Write Parquet to S3 (s3a://) for Spark writer
    - COPY from S3 (s3://) into Redshift
    """
    if not S3_BUCKET:
        raise RuntimeError("Missing env AWS_S3_BUCKET")

    today = datetime.now()
    partition = f"year={today.year}/month={today.month:02d}/day={today.day:02d}"
    
    s3a_base = f"s3a://{S3_BUCKET}/{S3_PREFIX}"  # Spark writer
    s3_base  = f"s3://{S3_BUCKET}/{S3_PREFIX}"   # Redshift COPY

    # 1) for Spark writer
    for name, df in tables.items():
        s3a_uri = f"{s3a_base}/{name}/{partition}"
        df.coalesce(1).write.mode("overwrite").parquet(s3a_uri)
        log.info("Wrote %s → %s", name, s3a_uri)

    # 2) for Redshift COPY
    with _rs_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for name in tables.keys():
                s3_uri = f"{s3_base}/{name}/{partition}"
                cur.execute(_copy_sql(name, s3_uri))
                log.info("Loaded %s → %s.%s", name, REDSHIFT_SCHEMA, name)
