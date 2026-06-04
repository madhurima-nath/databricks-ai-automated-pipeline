# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Fetches raw market and macroeconomic data from external APIs and writes to Delta tables.
# MAGIC No transformations — data is stored exactly as received.
# MAGIC
# MAGIC | Source | Ticker / Series | Frequency | Table |
# MAGIC |--------|----------------|-----------|-------|
# MAGIC | yfinance | `^GSPC` S&P 500 | Daily OHLCV | `bronze_sp500` |
# MAGIC | yfinance | `^STOXX50E` Euro Stoxx 50 | Daily close | `bronze_eurostoxx` |
# MAGIC | yfinance | `^VIX` | Daily close | `bronze_vix` |
# MAGIC | FRED | `FEDFUNDS` US Fed Funds Rate | Monthly | `bronze_fed_rate` |
# MAGIC | FRED | `ECBDFR` ECB Deposit Facility Rate | Monthly | `bronze_ecb_rate` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: FRED API key
# MAGIC
# MAGIC A widget will appear at the top of this notebook after running the next cell.
# MAGIC Paste your FRED API key there before running the rest of the notebook.
# MAGIC The key is never saved to a file or committed to git.
# MAGIC
# MAGIC Get a free key at [fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html).
# MAGIC
# MAGIC **Note:** This project requires a full Databricks workspace (free trial or paid).
# MAGIC Serverless compute, SQL Warehouses, and Databricks Apps are not available on Community Edition.

# COMMAND ----------

dbutils.widgets.text("FRED_API_KEY", "", "FRED API Key")

# COMMAND ----------

# MAGIC %pip install yfinance==1.4.1 fredapi==0.5.2 --quiet

# COMMAND ----------

import datetime
import os
import pandas as pd
import yfinance as yf
from fredapi import Fred
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

START_DATE = "2010-01-01"
END_DATE   = datetime.date.today().isoformat()

fred_api_key = dbutils.widgets.get("FRED_API_KEY").strip()
if not fred_api_key:
    try:
        fred_api_key = dbutils.secrets.get(scope="project-secrets", key="fred_api_key")
    except Exception:
        pass
if not fred_api_key:
    raise ValueError(
        "FRED API key not found. Paste it in the FRED_API_KEY widget at the top of this notebook."
    )

fred = Fred(api_key=fred_api_key)

print(f"Date range: {START_DATE} → {END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared utilities

# COMMAND ----------

def flatten_yf(raw: pd.DataFrame) -> pd.DataFrame:
    """Flatten yfinance MultiIndex columns, lowercase names, return string date."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [col[0] for col in raw.columns]
    df = raw.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    date_col = next(c for c in df.columns if c in ("date", "datetime", "price"))
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df

def fetch_close_only(ticker: str, col_name: str) -> pd.DataFrame:
    """Download a single ticker and return date + one close column."""
    raw = yf.download(ticker, start=START_DATE, end=END_DATE, auto_adjust=True, progress=False)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [col[0] for col in raw.columns]
    df = raw[["Close"]].reset_index()
    df.columns = ["date", col_name]
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df

def fetch_fred_series(series_id: str, col_name: str) -> pd.DataFrame:
    """Fetch a FRED series and return date + value column."""
    s = fred.get_series(series_id, observation_start=START_DATE, observation_end=END_DATE)
    df = s.reset_index()
    df.columns = ["date", col_name]
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## S&P 500 — daily OHLCV

# COMMAND ----------

sp500_pd = flatten_yf(
    yf.download("^GSPC", start=START_DATE, end=END_DATE, auto_adjust=True, progress=False)
)
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

eurostoxx_pd = fetch_close_only("^STOXX50E", "eurostoxx_close")
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

vix_pd = fetch_close_only("^VIX", "vix_close")
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

fed_pd = fetch_fred_series("FEDFUNDS", "fed_rate")
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

ecb_pd = fetch_fred_series("ECBDFR", "ecb_rate")
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
