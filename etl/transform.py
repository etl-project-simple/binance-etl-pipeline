from pyspark.sql import SparkSession
from etl.logger import setup_logger
from etl.transform_logic import TRANSFORM_FORM_DF_FUNCTIONS, STATIC_DIMENSIONS
import os, sys, traceback
from dotenv import load_dotenv
from datetime import date   

# Airflow/Docker: mount .env /opt/airflow/.env
load_dotenv(os.getenv("ENV_FILE", "/opt/airflow/.env"))
REFRESH_STATIC = os.getenv("REFRESH_STATIC", "False").lower() == "true"

def run_transform(target_date: date= None):
    # 1.setup logging and spark session
    log = setup_logger("transform_data")
    log.info("Starting transform step")
    
    spark = (
        SparkSession.builder
        .appName("TransformStep")
        .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.6.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.697"
        )
        .getOrCreate())
    # 2.read raw data from parquet
    try:
        base_path = os.getenv("BASE_PATH", "/opt/airflow/output")
        raw_path = f"{base_path}/raw.parquet"
        df= spark.read.parquet(raw_path)
    # 3.apply transformations
        for name, func in TRANSFORM_FORM_DF_FUNCTIONS.items():
            log.info(f"Running transform {name}")
            result = func(df)
    # 4.write transformed data to parquet
            output_path = f"{base_path}/clean/{name}.parquet"
            result.write.mode("overwrite").parquet(output_path)
            log.info(f"Transformed data for {name} saved to {output_path}")
        
        for name, func in STATIC_DIMENSIONS.items():
            output_path = f"{base_path}/clean/{name}.parquet"
            if os.path.exists(output_path) and not REFRESH_STATIC:
                log.info(f"Skipping static dimension {name}, file already exists at {output_path}")
                continue
            log.info(f"Running static dimension {name}")
            result = func(spark)
            result.write.mode("overwrite").parquet(output_path)
            log.info(f"✅ Saved {name} → {output_path}")
            
        log.info("Transform step completed successfully")
    # 5.handle exceptions
    except Exception as e:
        log.error(f"❌ Transform failed: {e}")
        log.error(traceback.format_exc())
        sys.exit(1)
        
if __name__ == "__main__":
    run_transform()
