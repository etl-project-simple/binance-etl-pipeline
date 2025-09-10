from pyspark.sql import SparkSession
from datetime import date, timedelta

def extract_data(spark: SparkSession, table: str, jdbc_db: dict, target_date):
    """
    Extract data from a PostgreSQL table into a Spark DataFrame.
    """
    
    sql = f"(SELECT * FROM trades WHERE trade_time::date = '{target_date}') tmp"
    return (
    spark.read.format("jdbc")
    .option("url", jdbc_db["url"])
    .option("dbtable", table)
    .option("user", jdbc_db["user"])
    .option("password", jdbc_db["password"])
    .option("driver", "org.postgresql.Driver") 
    .load()
    )

