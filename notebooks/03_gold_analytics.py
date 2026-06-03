# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Analytics Aggregates
# MAGIC
# MAGIC Reads `silver_market` and produces analysis-ready metrics for the Streamlit dashboard.
# MAGIC
# MAGIC **Metrics computed:**
# MAGIC - `vol_20d` — 20-day rolling realised volatility (annualised std of log returns)
# MAGIC - `vol_60d` — 60-day rolling realised volatility (annualised)
# MAGIC - `corr_60d` — 60-day rolling correlation between S&P 500 log return and VIX close
# MAGIC - `rate_regime` — interest rate environment: `low` / `medium` / `high`
# MAGIC
# MAGIC **Output table:** `gold_analytics`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

silver = spark.table("silver_market")
print(f"silver_market: {silver.count():,} rows")
silver.orderBy("date").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rolling volatility (annualised)
# MAGIC
# MAGIC Realised volatility = std(log_return) × √252
# MAGIC Uses a trailing window (current row and N-1 preceding rows).

# COMMAND ----------

TRADING_DAYS = 252

w20 = Window.orderBy("date").rowsBetween(-19, 0)
w60 = Window.orderBy("date").rowsBetween(-59, 0)

gold = (
    silver
    .withColumn("vol_20d", F.round(F.stddev("log_return").over(w20) * (TRADING_DAYS ** 0.5), 6))
    .withColumn("vol_60d", F.round(F.stddev("log_return").over(w60) * (TRADING_DAYS ** 0.5), 6))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rolling correlation: S&P 500 return vs VIX
# MAGIC
# MAGIC 60-day rolling Pearson correlation between daily log return and VIX close.
# MAGIC High negative correlation is typical (market falls → VIX spikes).

# COMMAND ----------

gold = gold.withColumn(
    "corr_60d",
    F.round(F.corr("log_return", "vix_close").over(w60), 6)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rate regime classification
# MAGIC
# MAGIC Thresholds based on the FEDFUNDS historical range (2010–present):
# MAGIC - `low`    : fed_rate < 1.0
# MAGIC - `medium` : 1.0 ≤ fed_rate < 4.0
# MAGIC - `high`   : fed_rate ≥ 4.0

# COMMAND ----------

gold = gold.withColumn(
    "rate_regime",
    F.when(F.col("fed_rate") < 1.0, "low")
     .when(F.col("fed_rate") < 4.0, "medium")
     .otherwise("high")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop rows with insufficient history for rolling windows
# MAGIC
# MAGIC The first 59 rows cannot produce a valid 60-day window — exclude them.

# COMMAND ----------

gold = (
    gold
    .filter(F.col("vol_60d").isNotNull())
    .withColumn("updated_at", F.current_timestamp())
    .orderBy("date")
)

print(f"Gold rows (after windowing): {gold.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold table

# COMMAND ----------

gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_analytics")

result = spark.table("gold_analytics")
print(f"Saved gold_analytics: {result.count():,} rows")
result.orderBy("date").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rate regime distribution

# COMMAND ----------

spark.sql("""
    SELECT rate_regime, COUNT(*) AS days, ROUND(AVG(vol_20d), 4) AS avg_vol_20d, ROUND(AVG(vix_close), 2) AS avg_vix
    FROM gold_analytics
    GROUP BY rate_regime
    ORDER BY avg_vix DESC
""").show()

# COMMAND ----------

print("Schema:")
spark.table("gold_analytics").printSchema()
