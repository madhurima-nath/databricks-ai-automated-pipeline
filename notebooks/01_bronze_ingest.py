# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Reads pre-downloaded CSV files from `data/raw/` (synced via Databricks Repos) and
# MAGIC writes to Delta tables. No transformations — data is stored exactly as received.
# MAGIC
# MAGIC | Source | Ticker / Series | Frequency | Table |
# MAGIC |--------|----------------|-----------|-------|
# MAGIC | yfinance | `^GSPC` S&P 500 | Daily OHLCV | `bronze_sp500` |
# MAGIC | yfinance | `^STOXX50E` Euro Stoxx 50 | Daily close | `bronze_eurostoxx` |
# MAGIC | yfinance | `^VIX` | Daily close | `bronze_vix` |
# MAGIC | FRED | `FEDFUNDS` US Fed Funds Rate | Monthly | `bronze_fed_rate` |
# MAGIC | FRED | `ECBDFR` ECB Deposit Facility Rate | Monthly | `bronze_ecb_rate` |
# MAGIC
# MAGIC **Data source:** CSV files in `data/raw/` — downloaded locally via `scripts/download_data.py`
# MAGIC and committed to the repo. Serverless compute has no outbound internet access, so
# MAGIC data is fetched on a laptop and synced here via Repos.

# COMMAND ----------

import glob
import os
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

# Locate data/raw — works on serverless where notebookPath() is unavailable
DATA_DIR = None

# Method 1: notebook context (classic compute)
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _root    = "/Workspace/" + "/".join(_nb_path.lstrip("/").split("/")[:-2])
    if os.path.exists(_root + "/data/raw"):
        DATA_DIR = _root + "/data/raw"
except Exception:
    pass

# Method 2: search Repos by repo name (serverless)
if not DATA_DIR:
    matches = glob.glob("/Workspace/Repos/*/databricks-ai-automated-pipeline/data/raw")
    if matches:
        DATA_DIR = matches[0]

if not DATA_DIR:
    raise RuntimeError("Cannot find data/raw. Pull the latest repo changes in Databricks Repos.")

print(f"Reading CSVs from: {DATA_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## S&P 500 — daily OHLCV

# COMMAND ----------

sp500_pd = pd.read_csv(f"{DATA_DIR}/sp500.csv")
print(f"S&P 500: {len(sp500_pd):,} rows | columns: {list(sp500_pd.columns)}")
display(sp500_pd.head(3))

# COMMAND ----------

(
    spark.createDataFrame(sp500_pd)
    .withColumn("date",   F.col("date").cast(DateType()))
    .withColumn("open",   F.col("open").cast(DoubleType()))
    .withColumn("high",   F.col("high").cast(DoubleType()))
    .withColumn("low",    F.col("low").cast(DoubleType()))
    .withColumn("close",  F.col("close").cast(DoubleType()))
    .withColumn("volume", F.col("volume").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_sp500")
)
print(f"Saved bronze_sp500: {spark.table('bronze_sp500').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Euro Stoxx 50 — daily close

# COMMAND ----------

eurostoxx_pd = pd.read_csv(f"{DATA_DIR}/eurostoxx.csv")
print(f"Euro Stoxx 50: {len(eurostoxx_pd):,} rows")
display(eurostoxx_pd.head(3))

# COMMAND ----------

(
    spark.createDataFrame(eurostoxx_pd)
    .withColumn("date",            F.col("date").cast(DateType()))
    .withColumn("eurostoxx_close", F.col("eurostoxx_close").cast(DoubleType()))
    .withColumn("ingested_at",     F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_eurostoxx")
)
print(f"Saved bronze_eurostoxx: {spark.table('bronze_eurostoxx').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## VIX — daily close

# COMMAND ----------

vix_pd = pd.read_csv(f"{DATA_DIR}/vix.csv")
print(f"VIX: {len(vix_pd):,} rows")
display(vix_pd.head(3))

# COMMAND ----------

(
    spark.createDataFrame(vix_pd)
    .withColumn("date",      F.col("date").cast(DateType()))
    .withColumn("vix_close", F.col("vix_close").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_vix")
)
print(f"Saved bronze_vix: {spark.table('bronze_vix').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## US Federal Funds Rate — monthly

# COMMAND ----------

fed_pd = pd.read_csv(f"{DATA_DIR}/fed_rate.csv")
print(f"Fed Funds Rate: {len(fed_pd):,} rows")
display(fed_pd.head(3))

# COMMAND ----------

(
    spark.createDataFrame(fed_pd)
    .withColumn("date",     F.col("date").cast(DateType()))
    .withColumn("fed_rate", F.col("fed_rate").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_fed_rate")
)
print(f"Saved bronze_fed_rate: {spark.table('bronze_fed_rate').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ECB Deposit Facility Rate — monthly

# COMMAND ----------

ecb_pd = pd.read_csv(f"{DATA_DIR}/ecb_rate.csv")
print(f"ECB Rate: {len(ecb_pd):,} rows")
display(ecb_pd.head(3))

# COMMAND ----------

(
    spark.createDataFrame(ecb_pd)
    .withColumn("date",     F.col("date").cast(DateType()))
    .withColumn("ecb_rate", F.col("ecb_rate").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_ecb_rate")
)
print(f"Saved bronze_ecb_rate: {spark.table('bronze_ecb_rate').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

tables = ["bronze_sp500", "bronze_eurostoxx", "bronze_vix", "bronze_fed_rate", "bronze_ecb_rate"]
print("Bronze ingestion complete.")
for t in tables:
    print(f"  {t}: {spark.table(t).count():,} rows")
