# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Clean, Join, and Align
# MAGIC
# MAGIC Reads Bronze Delta tables, runs data quality checks, aligns all sources to the same
# MAGIC daily date spine using forward-fill window functions, and produces a single joined table.
# MAGIC
# MAGIC **Input tables:** `bronze_sp500`, `bronze_eurostoxx`, `bronze_vix`, `bronze_fed_rate`, `bronze_ecb_rate`
# MAGIC
# MAGIC **Output table:** `silver_market`
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `sp500_close` | S&P 500 daily close |
# MAGIC | `eurostoxx_close` | Euro Stoxx 50 daily close |
# MAGIC | `vix_close` | VIX index close |
# MAGIC | `fed_rate` | US Federal Funds Rate, forward-filled to daily |
# MAGIC | `ecb_rate` | ECB Deposit Facility Rate, forward-filled to daily |
# MAGIC | `sp500_log_return` | Natural log return of S&P 500 |
# MAGIC | `eurostoxx_log_return` | Natural log return of Euro Stoxx 50 |
# MAGIC | `policy_rate_diff` | Fed rate minus ECB rate (US premium) |

# COMMAND ----------

# MAGIC %run ./00_quality_checks

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore", message=".*No Partition Defined for Window.*")

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and validate Bronze tables

# COMMAND ----------

sp500      = spark.table("bronze_sp500").select("date", "open", "high", "low", "close", "volume")
eurostoxx  = spark.table("bronze_eurostoxx").select("date", "eurostoxx_close")
vix        = spark.table("bronze_vix").select("date", "vix_close")
fed        = spark.table("bronze_fed_rate").select("date", "fed_rate")
ecb        = spark.table("bronze_ecb_rate").select("date", "ecb_rate")

# Quality checks — fail fast if Bronze data is not usable
run_quality_checks(sp500,     "bronze_sp500",    min_rows=3000, null_cols=["close"],        range_checks={"close": (100, 10000)})
run_quality_checks(eurostoxx, "bronze_eurostoxx",min_rows=3000, null_cols=["eurostoxx_close"], range_checks={"eurostoxx_close": (500, 10000)})
run_quality_checks(vix,       "bronze_vix",      min_rows=3000, null_cols=["vix_close"],    range_checks={"vix_close": (5, 100)})
run_quality_checks(fed,       "bronze_fed_rate", min_rows=150,  null_cols=["fed_rate"],     range_checks={"fed_rate": (0, 25)})
run_quality_checks(ecb,       "bronze_ecb_rate", min_rows=100,  null_cols=["ecb_rate"],     range_checks={"ecb_rate": (-2, 10)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forward-fill monthly rates to daily frequency
# MAGIC
# MAGIC Both central bank rates are monthly. Each trading day receives the most recent known rate
# MAGIC on or before that date — a point-in-time join using a SQL unbounded-preceding window.

# COMMAND ----------

fill_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
back_window = Window.orderBy("date").rowsBetween(Window.currentRow, Window.unboundedFollowing)

def forward_fill_rate(daily_df, monthly_df, rate_col):
    """Left-join monthly rate onto daily date spine, forward-fill, then backward-fill.
    Backward-fill handles leading nulls when the first rate date falls on a non-trading day."""
    joined = daily_df.join(monthly_df, on="date", how="left")
    return (
        joined
        .withColumn(rate_col, F.last(F.col(rate_col),  ignorenulls=True).over(fill_window))
        .withColumn(rate_col, F.first(F.col(rate_col), ignorenulls=True).over(back_window))
    )

sp500_with_rates = forward_fill_rate(sp500, fed, "fed_rate")
sp500_with_rates = forward_fill_rate(sp500_with_rates, ecb, "ecb_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join all sources on date

# COMMAND ----------

market = (
    sp500_with_rates
    .join(eurostoxx, on="date", how="inner")
    .join(vix,       on="date", how="inner")
)

# Rename S&P 500 price columns for clarity alongside European equivalents
market = (
    market
    .withColumnRenamed("open",   "sp500_open")
    .withColumnRenamed("high",   "sp500_high")
    .withColumnRenamed("low",    "sp500_low")
    .withColumnRenamed("close",  "sp500_close")
    .withColumnRenamed("volume", "sp500_volume")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived columns

# COMMAND ----------

date_window = Window.orderBy("date")

market = (
    market
    # Log returns
    .withColumn("sp500_prev_close",      F.lag("sp500_close", 1).over(date_window))
    .withColumn("eurostoxx_prev_close",  F.lag("eurostoxx_close", 1).over(date_window))
    .withColumn(
        "sp500_log_return",
        F.round(F.log(F.col("sp500_close") / F.col("sp500_prev_close")), 6)
    )
    .withColumn(
        "eurostoxx_log_return",
        F.round(F.log(F.col("eurostoxx_close") / F.col("eurostoxx_prev_close")), 6)
    )
    # US–EU policy rate differential (positive = US rates higher than ECB)
    .withColumn(
        "policy_rate_diff",
        F.round(F.col("fed_rate") - F.col("ecb_rate"), 4)
    )
    .drop("sp500_prev_close", "eurostoxx_prev_close")
    # Drop first row — no previous close for lag
    .filter(F.col("sp500_log_return").isNotNull())
    .withColumn("updated_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver quality checks

# COMMAND ----------

run_quality_checks(
    market, "silver_market",
    min_rows=3000,
    null_cols=["sp500_close", "eurostoxx_close", "fed_rate", "ecb_rate"],
    range_checks={"sp500_log_return": (-0.15, 0.15), "eurostoxx_log_return": (-0.15, 0.15)},
    check_duplicates_on=["date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver table

# COMMAND ----------

market.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_market")

result = spark.table("silver_market")
print(f"Saved silver_market: {result.count():,} rows")
result.orderBy("date").show(5, truncate=False)
result.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline run log

# COMMAND ----------

log_pipeline_run(spark, "silver", "silver_market", result)

print("\nRun log (most recent entries):")
spark.table("pipeline_run_log").orderBy("run_timestamp", ascending=False).show(10, truncate=False)
