# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Analytics Aggregates
# MAGIC
# MAGIC Produces analysis-ready metrics from `silver_market` for the Streamlit dashboard
# MAGIC and downstream reporting.
# MAGIC
# MAGIC **Output table:** `gold_analytics`
# MAGIC
# MAGIC | Metric | Description |
# MAGIC |--------|-------------|
# MAGIC | `sp500_vol_20d` | 20-day rolling realised volatility, annualised |
# MAGIC | `sp500_vol_60d` | 60-day rolling realised volatility, annualised |
# MAGIC | `eurostoxx_vol_20d` | 20-day rolling realised volatility for Euro Stoxx 50 |
# MAGIC | `eurostoxx_vol_60d` | 60-day rolling realised volatility for Euro Stoxx 50 |
# MAGIC | `us_eu_equity_corr_60d` | 60-day rolling correlation between S&P 500 and Euro Stoxx 50 log returns |
# MAGIC | `sp500_vix_corr_60d` | 60-day rolling correlation between S&P 500 return and VIX |
# MAGIC | `sp500_drawdown_52w` | S&P 500 drawdown from 52-week rolling high |
# MAGIC | `us_rate_regime` | Fed rate environment: `low` / `medium` / `high` |
# MAGIC | `eu_rate_regime` | ECB rate environment: `negative` / `low` / `medium` / `high` |
# MAGIC | `policy_divergence` | US vs EU rate differential regime |
# MAGIC | `vix_regime` | Market stress level: `calm` / `elevated` / `stress` |

# COMMAND ----------

# MAGIC %run ./00_quality_checks

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore", message=".*No Partition Defined for Window.*")

from pyspark.sql import functions as F
from pyspark.sql import Window

TRADING_DAYS = 252

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver

# COMMAND ----------

silver = spark.table("silver_market")
run_quality_checks(silver, "silver_market", min_rows=3000, null_cols=["sp500_close", "eurostoxx_close", "fed_rate", "ecb_rate"])
print(f"silver_market: {silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rolling windows

# COMMAND ----------

w20  = Window.orderBy("date").rowsBetween(-19, 0)
w60  = Window.orderBy("date").rowsBetween(-59, 0)
w252 = Window.orderBy("date").rowsBetween(-251, 0)   # 52-week rolling high

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volatility — annualised rolling std of log returns

# COMMAND ----------

gold = (
    silver
    .withColumn("sp500_vol_20d",      F.round(F.stddev("sp500_log_return").over(w20)  * (TRADING_DAYS ** 0.5), 6))
    .withColumn("sp500_vol_60d",      F.round(F.stddev("sp500_log_return").over(w60)  * (TRADING_DAYS ** 0.5), 6))
    .withColumn("eurostoxx_vol_20d",  F.round(F.stddev("eurostoxx_log_return").over(w20) * (TRADING_DAYS ** 0.5), 6))
    .withColumn("eurostoxx_vol_60d",  F.round(F.stddev("eurostoxx_log_return").over(w60) * (TRADING_DAYS ** 0.5), 6))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rolling correlations

# COMMAND ----------

gold = (
    gold
    # S&P 500 vs Euro Stoxx 50 return correlation (US–EU equity co-movement)
    .withColumn("us_eu_equity_corr_60d", F.round(F.corr("sp500_log_return", "eurostoxx_log_return").over(w60), 6))
    # S&P 500 return vs VIX (typically strongly negative — falling markets spike VIX)
    .withColumn("sp500_vix_corr_60d",    F.round(F.corr("sp500_log_return", "vix_close").over(w60), 6))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S&P 500 drawdown from 52-week rolling high

# COMMAND ----------

gold = gold.withColumn(
    "sp500_52w_high",
    F.max("sp500_close").over(w252)
).withColumn(
    "sp500_drawdown_52w",
    F.round((F.col("sp500_close") - F.col("sp500_52w_high")) / F.col("sp500_52w_high") * 100, 4)
).drop("sp500_52w_high")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regime classifications

# COMMAND ----------

gold = (
    gold
    # US rate regime — Fed Funds Rate thresholds based on 2010–present range
    .withColumn(
        "us_rate_regime",
        F.when(F.col("fed_rate") < 1.0, "low")
         .when(F.col("fed_rate") < 4.0, "medium")
         .otherwise("high")
    )
    # EU rate regime — ECB rate was negative 2014–2022
    .withColumn(
        "eu_rate_regime",
        F.when(F.col("ecb_rate") < 0.0, "negative")
         .when(F.col("ecb_rate") < 1.0, "low")
         .when(F.col("ecb_rate") < 3.5, "medium")
         .otherwise("high")
    )
    # Policy divergence — US rate premium over ECB
    .withColumn(
        "policy_divergence",
        F.when(F.col("policy_rate_diff") >  2.0, "us_significantly_higher")
         .when(F.col("policy_rate_diff") >  0.5, "us_higher")
         .when(F.col("policy_rate_diff") < -0.5, "eu_higher")
         .otherwise("aligned")
    )
    # VIX regime — standard market stress thresholds
    .withColumn(
        "vix_regime",
        F.when(F.col("vix_close") >= 30, "stress")
         .when(F.col("vix_close") >= 20, "elevated")
         .otherwise("calm")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop rows with insufficient rolling window history and write

# COMMAND ----------

gold = (
    gold
    .filter(F.col("sp500_vol_60d").isNotNull())
    .filter(F.col("us_eu_equity_corr_60d").isNotNull())
    .withColumn("updated_at", F.current_timestamp())
    .orderBy("date")
)

run_quality_checks(gold, "gold_analytics", min_rows=2500, null_cols=["sp500_vol_60d", "eurostoxx_vol_60d"])

gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary statistics

# COMMAND ----------

result = spark.table("gold_analytics")
print(f"Saved gold_analytics: {result.count():,} rows")

print("\nRate regime distribution:")
result.groupBy("us_rate_regime", "eu_rate_regime", "policy_divergence") \
      .agg(F.count("*").alias("days")) \
      .orderBy("days", ascending=False).show()

print("\nVIX regime distribution:")
result.groupBy("vix_regime") \
      .agg(F.count("*").alias("days"), F.round(F.avg("sp500_vol_20d"), 4).alias("avg_vol_20d")) \
      .orderBy("days", ascending=False).show()

print("\nMost recent 5 rows:")
result.orderBy(F.col("date").desc()).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline run log

# COMMAND ----------

log_pipeline_run(spark, "gold", "gold_analytics", result)

print("\nFull pipeline run log:")
spark.table("pipeline_run_log").orderBy("run_timestamp", ascending=False).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta table version history
# MAGIC
# MAGIC Delta Lake records every write to gold_analytics as a new version.
# MAGIC Use `VERSION AS OF` or `TIMESTAMP AS OF` to query any prior state.

# COMMAND ----------

print("gold_analytics version history:")
spark.sql("DESCRIBE HISTORY gold_analytics").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(10, truncate=False)
