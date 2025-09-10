from pyspark.sql import SparkSession
from etl.extract import extract_data
from etl.transform import TRANSFORM_FUNCTIONS  
from etl.load import load_to_s3_and_redshift
from etl.utils import get_postgres_jdbc
import os, logging
from datetime import date, timedelta

def run_etl():
    # Create Spark session
    spark = SparkSession.builder \
    .appName("Binance ETL") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
        "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory") \
    .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "false") \
    .getOrCreate()
        
    target_date = date.today() 
    
    # Extract data from PostgreSQL
    jdbc_postgres = get_postgres_jdbc()
    raw_df = extract_data(spark, "trades", jdbc_postgres, target_date=target_date)
    
    table = {}
    # Transform data
    for name, transform_func in TRANSFORM_FUNCTIONS.items():
        logging.info(f"Running transform {name}")
        table[name] = transform_func(raw_df)
        
    # Load data into Redshift
    load_to_s3_and_redshift(table, coalesce_n=8)

if __name__ == "__main__":
    run_etl()
    