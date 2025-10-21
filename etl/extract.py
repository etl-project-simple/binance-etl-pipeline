from pyspark.sql import SparkSession
from datetime import date, timedelta
from etl.utils import get_postgres_jdbc
from etl.logger import setup_logger
import sys, traceback

def run_extract():
    # 1.setup logging and spark session
    log= setup_logger("extract_data")
    log.info("Starting extract step")
    
    spark = (
        SparkSession.builder
        .appName("ExtractStep")
        .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.6.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.697"
        )   
        .getOrCreate())
     
    # 2.extract data from postgres and save to parquet
    try:  
        jdbc= get_postgres_jdbc()
        target_date = date.today()  
        
        sql = f"(SELECT * FROM trades WHERE trade_time::date = '{target_date}') tmp"
        df= (
        spark.read.format("jdbc")
            .option("url", jdbc["url"])
            .option("dbtable", sql)
            .option("user", jdbc["user"])
            .option("password", jdbc["password"])
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", 10000)
            .load()
            )
    # 3.write raw data to parquet
        output_path = "/opt/airflow/output/raw.parquet"
        df.write.mode("overwrite").parquet(output_path)
        log.info(f"Data extracted {target_date} and saved to {output_path}")
    # 4.handle exceptions
    except Exception as e:
        log.error(f"❌ Extract failed: {e}")
        log.error(traceback.format_exc())
        sys.exit(1)
    
if __name__ == "__main__":
    run_extract()