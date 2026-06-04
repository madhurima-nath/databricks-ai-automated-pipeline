# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Export Gold Table to Parquet (optional — not needed for the Streamlit dashboard)
# MAGIC
# MAGIC **This notebook is not part of the main pipeline.**
# MAGIC The Streamlit dashboard reads `gold_analytics` live via the Databricks SQL connector.
# MAGIC
# MAGIC Only run this if you want a local `.parquet` snapshot of the Gold table.
# MAGIC Requires DBFS to be enabled in your workspace — if you get a DBFS_DISABLED error, skip this notebook.

# COMMAND ----------

from pyspark.sql import functions as F

DBFS_STAGING  = "/FileStore/gold_analytics_staging"
DBFS_FINAL    = "/FileStore/gold_analytics.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold table

# COMMAND ----------

gold = spark.table("gold_analytics").orderBy("date")
row_count = gold.count()
print(f"gold_analytics: {row_count:,} rows")
gold.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to DBFS staging (multi-part), then coalesce to single file

# COMMAND ----------

# Coalesce to 1 partition for a single output file — acceptable for a few thousand rows
gold.coalesce(1).write.mode("overwrite").parquet(DBFS_STAGING)
print(f"Written to staging: {DBFS_STAGING}")

# COMMAND ----------

# Find the single part file and move it to the final path
files = dbutils.fs.ls(DBFS_STAGING)
part_file = next(f.path for f in files if f.name.startswith("part-") and f.name.endswith(".parquet"))

dbutils.fs.cp(part_file, DBFS_FINAL)
dbutils.fs.rm(DBFS_STAGING, recurse=True)

print(f"Exported to: {DBFS_FINAL}")
print(f"Download with: python scripts/download_gold.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: read back from DBFS parquet

# COMMAND ----------

verify = spark.read.parquet(DBFS_FINAL)
print(f"Verified: {verify.count():,} rows in {DBFS_FINAL}")
verify.orderBy(F.col("date").desc()).show(3, truncate=False)
