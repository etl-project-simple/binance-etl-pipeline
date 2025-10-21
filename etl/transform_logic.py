from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, date_trunc, avg, min as _min, max as _max, sum as _sum, regexp_extract, lit
from pyspark.sql.types import LongType, StringType


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

def create_dim_symbol(df: DataFrame) -> DataFrame:
    return(
        df.select("symbol").dropDuplicates()
            .withColumn("symbol_id", monotonically_increasing_id())  # Auto-generate symbol_id
            .select(col("symbol_id").cast(LongType()), col("symbol").cast(StringType()))
    )

def create_dim_currency(spark: SparkSession) -> DataFrame:
    """Tạo danh mục các đồng coin (base/quote)."""
    data = [
        ("BTC", "Bitcoin", "Base", "Bitcoin", False),
        ("ETH", "Ethereum", "Base", "Ethereum", False),
        ("BNB", "Binance Coin", "Base", "BNB Chain", False),
        ("USDT", "Tether USD", "Stablecoin", "Tron", True),
        ("USDC", "USD Coin", "Stablecoin", "Ethereum", True),
    ]
    cols = ["currency_id", "currency_name", "category", "blockchain", "is_stablecoin"]
    return spark.createDataFrame(data, cols)

def create_dim_exchange(spark: SparkSession) -> DataFrame:
    """Danh mục sàn giao dịch (hiện tại chỉ có Binance)."""
    data = [("binance", "Binance Exchange", "Global", "https://api.binance.com")]
    cols = ["exchange_id", "exchange_name", "region", "api_endpoint"]
    return spark.createDataFrame(data, cols)    

STABLE_QUOTES = ["USDT", "USDC", "BTC", "BNB", "ETH"]

def create_fact_trades(df: DataFrame) -> DataFrame:
    df_minute = df.withColumn("trade_minute", date_trunc("minute", col("trade_time")))

    #    Ex: BTCUSDT → base=BTC, quote=USDT
    df_with_quote = df_minute.withColumn(
        "quote_currency_id",
        F.when(
            F.expr(
                " OR ".join([f"symbol LIKE '%{q}'" for q in STABLE_QUOTES])
            ),
            F.regexp_extract(F.col("symbol"), "(" + "|".join(STABLE_QUOTES) + ")$", 1)
        ).otherwise(F.lit(None))
    )

    # Tách base = phần còn lại sau khi bỏ quote
    df_with_base = df_with_quote.withColumn(
        "base_currency_id",
        F.regexp_replace(F.col("symbol"), "(" + "|".join(STABLE_QUOTES) + ")$", "")
    )


    # 3️⃣ Tổng hợp các metrics theo symbol + phút
    df_fact = (
        df_with_base.groupBy("symbol", "trade_minute", "base_currency_id", "quote_currency_id")
        .agg(
            _min("price").alias("min_price"),
            _max("price").alias("max_price"),
            avg("price").alias("avg_price"),
            _sum("quantity").alias("total_quantity"),
            _sum(col("price") * col("quantity")).alias("total_trade_value")
        )
    )

    # 4️⃣ Gắn exchange_id (vì chỉ có Binance)
    df_final = df_fact.withColumn("exchange_id", lit("binance"))

    # 5️⃣ Sắp xếp cột theo đúng schema Redshift
    df_final = df_final.select(
        "symbol",
        "trade_minute",
        "base_currency_id",
        "quote_currency_id",
        "exchange_id",
        "min_price",
        "max_price",
        "avg_price",
        "total_quantity",
        "total_trade_value"
    )

    return df_final
    

# Group 1: transforms from df
TRANSFORM_FORM_DF_FUNCTIONS = {
    "dim_time": create_dim_time,
    "fact_trades": create_fact_trades,
    "dim_symbol": create_dim_symbol
}

STATIC_DIMENSIONS = {
    "dim_exchange": create_dim_exchange,
    "dim_currency": create_dim_currency,
}