from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, date_trunc, avg, min as _min, max as _max, sum as _sum

def create_dim_time(df: DataFrame) -> DataFrame:
    """
    Create a dimension table for time from the timestamp column.
    """
    return(
        df.withColumn("datetime", col("trade_time"))
            .withColumn("year", year(col("datetime")))
            .withColumn("month", month(col("datetime")))
            .withColumn("day", dayofmonth(col("datetime")))
            .withColumn("hour", hour(col("datetime")))
            .withColumn("minute", minute(col("datetime")))
            .select("datetime", "year", "month", "day", "hour", "minute")
            .dropDuplicates()  
    )

def create_fact_trades(df: DataFrame) -> DataFrame:
    df_minute = df.withColumn("minute_trunc", date_trunc("minute", col("trade_time")))
    return (
        df_minute.groupBy("symbol", "minute_trunc")
            .agg(
                _min("price").alias("min_price"),
                _max("price").alias("max_price"),
                avg("price").alias("avg_price"),
                _sum("quantity").alias("total_quantity"),
                _sum(col("price") * col("quantity")).alias("total_trade_value")
            )
            .withColumnRenamed("minute_trunc", "trade_minute")
    )
    
def create_dim_symbol(df: DataFrame) -> DataFrame:
    return df.select("symbol").dropDuplicates()

TRANSFORM_FUNCTIONS = {
    "dim_time": create_dim_time,
    "fact_trades": create_fact_trades,
    "dim_symbol": create_dim_symbol
}
