# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Fetches raw market and macroeconomic data from external APIs and writes to Delta tables.
# MAGIC
# MAGIC | Source | Ticker / Series | Frequency |
# MAGIC |--------|----------------|-----------|
# MAGIC | yfinance | `^GSPC` (S&P 500) | Daily OHLCV |
# MAGIC | yfinance | `^VIX` | Daily close |
# MAGIC | FRED | `FEDFUNDS` | Monthly |
# MAGIC
# MAGIC **Output tables:** `bronze_sp500`, `bronze_vix`, `bronze_fed_rate`

# COMMAND ----------

# MAGIC %md
# MAGIC ## One-time setup: save FRED API key to DBFS
# MAGIC
# MAGIC Run this cell once with your key, then comment it out. The key persists in DBFS across sessions and is never committed to git.

# COMMAND ----------

# Uncomment, fill in your key, run once, then re-comment.
# dbutils.fs.put("/config/fred_api_key.txt", "PASTE_YOUR_KEY_HERE", overwrite=True)
# print("Key saved to DBFS.")

# COMMAND ----------

# MAGIC %pip install yfinance==1.4.1 fredapi==0.5.2 --quiet

# COMMAND ----------

import datetime
import pandas as pd
import yfinance as yf
from fredapi import Fred
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

START_DATE = "2010-01-01"
END_DATE   = datetime.date.today().isoformat()

fred_api_key = dbutils.fs.head("/config/fred_api_key.txt").strip()
fred = Fred(api_key=fred_api_key)

print(f"Date range: {START_DATE} → {END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## S&P 500 — daily OHLCV

# COMMAND ----------

def _flatten_yf(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise yfinance output: flatten MultiIndex columns, lowercase names, string date."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [col[0] for col in raw.columns]
    df = raw.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    date_col = next(c for c in df.columns if c in ("date", "datetime", "price"))
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df

sp500_pd = _flatten_yf(
    yf.download("^GSPC", start=START_DATE, end=END_DATE, auto_adjust=True, progress=False)
)
print(f"S&P 500: {len(sp500_pd):,} rows | columns: {list(sp500_pd.columns)}")
display(sp500_pd.head(3))

# COMMAND ----------

sp500_df = (
    spark.createDataFrame(sp500_pd)
    .withColumn("date",   F.col("date").cast(DateType()))
    .withColumn("open",   F.col("open").cast(DoubleType()))
    .withColumn("high",   F.col("high").cast(DoubleType()))
    .withColumn("low",    F.col("low").cast(DoubleType()))
    .withColumn("close",  F.col("close").cast(DoubleType()))
    .withColumn("volume", F.col("volume").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
)

sp500_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_sp500")
print(f"Saved bronze_sp500: {spark.table('bronze_sp500').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## VIX — daily close

# COMMAND ----------

vix_raw = yf.download("^VIX", start=START_DATE, end=END_DATE, auto_adjust=True, progress=False)
if isinstance(vix_raw.columns, pd.MultiIndex):
    vix_raw.columns = [col[0] for col in vix_raw.columns]
vix_pd = vix_raw[["Close"]].reset_index()
vix_pd.columns = ["date", "vix_close"]
vix_pd["date"] = pd.to_datetime(vix_pd["date"]).dt.date.astype(str)

print(f"VIX: {len(vix_pd):,} rows")
display(vix_pd.head(3))

# COMMAND ----------

vix_df = (
    spark.createDataFrame(vix_pd)
    .withColumn("date",      F.col("date").cast(DateType()))
    .withColumn("vix_close", F.col("vix_close").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
)

vix_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_vix")
print(f"Saved bronze_vix: {spark.table('bronze_vix').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Federal Funds Rate — monthly

# COMMAND ----------

fed_series = fred.get_series("FEDFUNDS", observation_start=START_DATE, observation_end=END_DATE)
fed_pd = fed_series.reset_index()
fed_pd.columns = ["date", "fed_rate"]
fed_pd["date"] = pd.to_datetime(fed_pd["date"]).dt.date.astype(str)

print(f"Fed Rate: {len(fed_pd):,} rows")
display(fed_pd.head(3))

# COMMAND ----------

fed_df = (
    spark.createDataFrame(fed_pd)
    .withColumn("date",     F.col("date").cast(DateType()))
    .withColumn("fed_rate", F.col("fed_rate").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
)

fed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_fed_rate")
print(f"Saved bronze_fed_rate: {spark.table('bronze_fed_rate').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("Bronze ingestion complete.")
print(f"  bronze_sp500:    {spark.table('bronze_sp500').count():,} rows")
print(f"  bronze_vix:      {spark.table('bronze_vix').count():,} rows")
print(f"  bronze_fed_rate: {spark.table('bronze_fed_rate').count():,} rows")
