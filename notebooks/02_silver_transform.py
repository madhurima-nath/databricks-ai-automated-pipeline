# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Clean, Join, and Align
# MAGIC
# MAGIC Reads raw Bronze Delta tables, applies data quality checks, aligns the monthly Fed Rate
# MAGIC to the daily S&P 500 timeline using a forward-fill window function, and joins all three sources.
# MAGIC
# MAGIC **Input tables:** `bronze_sp500`, `bronze_vix`, `bronze_fed_rate`
# MAGIC
# MAGIC **Output table:** `silver_market`
# MAGIC
# MAGIC **Key columns added:**
# MAGIC - `daily_return` — percentage change in S&P 500 close price day-over-day
# MAGIC - `log_return` — natural log return (used for volatility calculations in Gold)
# MAGIC - `fed_rate` — forward-filled from monthly to daily

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze tables

# COMMAND ----------

sp500 = spark.table("bronze_sp500").select("date", "open", "high", "low", "close", "volume")
vix   = spark.table("bronze_vix").select("date", "vix_close")
fed   = spark.table("bronze_fed_rate").select("date", "fed_rate")

print(f"sp500:    {sp500.count():,} rows")
print(f"vix:      {vix.count():,} rows")
print(f"fed rate: {fed.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks

# COMMAND ----------

null_counts = sp500.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in sp500.columns])
print("Null counts in sp500:")
null_counts.show()

null_counts_vix = vix.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in vix.columns])
print("Null counts in vix:")
null_counts_vix.show()

# COMMAND ----------

# Drop rows with null close price or null VIX (these would corrupt downstream metrics)
sp500_clean = sp500.dropna(subset=["close"])
vix_clean   = vix.dropna(subset=["vix_close"])

print(f"sp500 after drop: {sp500_clean.count():,} rows")
print(f"vix after drop:   {vix_clean.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forward-fill Fed Rate to daily frequency
# MAGIC
# MAGIC Fed Rate is monthly. Each trading day gets the most recent rate on or before that date.
# MAGIC Uses a SQL window function ordered by date with an unbounded preceding frame.

# COMMAND ----------

# Join Fed Rate onto the full S&P 500 date spine (left join — most days have no Fed Rate row)
sp500_with_fed = sp500_clean.join(fed, on="date", how="left")

# Forward-fill: carry last known rate forward over nulls
fill_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

sp500_filled = sp500_with_fed.withColumn(
    "fed_rate",
    F.last(F.col("fed_rate"), ignorenulls=True).over(fill_window)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join S&P 500 + VIX

# COMMAND ----------

market = sp500_filled.join(vix_clean, on="date", how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived columns: daily return and log return

# COMMAND ----------

date_window = Window.orderBy("date")

market = (
    market
    .withColumn("prev_close", F.lag("close", 1).over(date_window))
    .withColumn(
        "daily_return",
        F.round((F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100, 6)
    )
    .withColumn(
        "log_return",
        F.round(F.log(F.col("close") / F.col("prev_close")), 6)
    )
    .drop("prev_close")
    # Drop the first row — no previous close available
    .filter(F.col("daily_return").isNotNull())
    .withColumn("updated_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver table

# COMMAND ----------

market.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_market")

result = spark.table("silver_market")
print(f"Saved silver_market: {result.count():,} rows")
result.orderBy("date").show(5)

# COMMAND ----------

print("Schema:")
spark.table("silver_market").printSchema()
