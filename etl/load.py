import os, psycopg2, sys, traceback
from dotenv import load_dotenv
from etl.logger import setup_logger
from pyspark.sql import SparkSession
from datetime import datetime, date

# 0. Load ENV
# Airflow/Docker: mount .env /opt/airflow/.env
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

# Local paths
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/airflow/output")
CLEAN_DIR = os.path.join(OUTPUT_PATH, "clean")

# Writer tuning
COALESCE_N = int(os.getenv("COALESCE_N", "8"))

TABLES = ["dim_time", "dim_symbol", "fact_trades" , "dim_exchange", "dim_currency"]
STATIC_TABLES = ["dim_exchange", "dim_currency"]
REFRESH_STATIC = os.getenv("REFRESH_STATIC", "False").lower() == "true"


# 1. Helpers
def validate_env(required_vars: dict):
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")


def _partition_for(d: date) -> str:
    return f"year={d.year}/month={d.month:02d}/day={d.day:02d}"

# Connect Redshift
def _rs_conn():
    required = {
        "REDSHIFT_HOST": REDSHIFT_HOST,
        "REDSHIFT_PORT": REDSHIFT_PORT,
        "REDSHIFT_DB": REDSHIFT_DB,
        "REDSHIFT_USER": REDSHIFT_USER,
        "REDSHIFT_PASSWORD": REDSHIFT_PWD,
        "REDSHIFT_SCHEMA": REDSHIFT_SCHEMA,
        "REDSHIFT_IAM_ROLE_ARN": REDSHIFT_IAM_ROLE_ARN,
    }
    validate_env(required)
    
    missing = {k: v for k, v in required.items() if not v}
    if missing:
        raise RuntimeError(f"Missing Redshift environment variables: {', '.join(missing)}")
    
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PWD,
    )

def _copy_sql(schema: str, table: str, s3_uri: str) -> str:
    # COPY from S3 (Parquet) into Redshift via IAM Role
    return f"""
        COPY {schema}.{table}
        FROM '{s3_uri}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        FORMAT AS PARQUET
        REGION '{AWS_REGION}';
    """.strip()

# 2. Main function
def run_load(target_date: date | None = None):
    #2.1 setup logging
    log = setup_logger("load_data")
    exec_date = target_date or datetime.now().date()
    log.info(f"Starting load step for date {exec_date}")
    
    try:
    #2.2 Validate env
        required_vars = {
            "REDSHIFT_HOST": REDSHIFT_HOST,
            "REDSHIFT_PORT": REDSHIFT_PORT,
            "REDSHIFT_DB": REDSHIFT_DB,
            "REDSHIFT_USER": REDSHIFT_USER,
            "REDSHIFT_PASSWORD": REDSHIFT_PWD,
            "REDSHIFT_SCHEMA": REDSHIFT_SCHEMA,
            "REDSHIFT_IAM_ROLE_ARN": REDSHIFT_IAM_ROLE_ARN,
        }
        validate_env(required_vars)
        
        partition = _partition_for(exec_date)
        s3a_base = f"s3a://{S3_BUCKET}/{S3_PREFIX}"
        s3_base = f"s3://{S3_BUCKET}/{S3_PREFIX}"
        
    #2.3 Spark DataFrame -> S3 (Parquet)
        spark = (
            SparkSession.builder
            .appName("LoadStep")
            .config(
                "spark.jars.packages",
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.697"
            )
            .getOrCreate())
    #2.4 Load each table
        for name in TABLES:
            local_path = os.path.join(CLEAN_DIR, f"{name}.parquet")
            if not os.path.exists(local_path):
               log.warning(f"Skipping {name}, file not found: {local_path}")
               continue
           
            if not REFRESH_STATIC and name in STATIC_TABLES:
               log.info(f"Skipping static table {name} as REFRESH_STATIC is False")
               continue
           
            df= spark.read.parquet(local_path)
            s3a_uri = f"{s3a_base}/{name}/{partition}"
           
            log.info(f"Writing {name} to S3 at {s3a_uri}")
            df.coalesce(COALESCE_N).write.mode("overwrite").parquet(s3a_uri)
            log.info("Uploaded %s to S3 (coalesce=%d)", name, COALESCE_N)
            
            with _rs_conn() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    for name in TABLES:
                        s3_uri = f"{s3_base}/{name}/{partition}"
                        sql = _copy_sql(REDSHIFT_SCHEMA, name, s3_uri)
                        log.info(f"Loading {name} into Redshift from {s3_uri}")
                        cur.execute(sql)
                        log.info("Loaded %s.%s Successfully ", REDSHIFT_SCHEMA, name)
                        
            log.info("Load step completed successfully for date %s", exec_date)
                        
            
    except Exception as e:
        log.error(f"❌ Transform failed: {e}")
        log.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    run_load() 