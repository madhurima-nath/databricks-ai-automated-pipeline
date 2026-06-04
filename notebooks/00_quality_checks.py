# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Checks — Shared Module
# MAGIC
# MAGIC Reusable quality check functions called via `%run ./00_quality_checks` from any notebook.
# MAGIC Domain-agnostic: works for financial, energy, or any other pipeline.
# MAGIC
# MAGIC Each check logs a PASS or FAIL with detail. A FAIL raises a `ValueError` to halt the pipeline.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger("quality_checks")

# COMMAND ----------

def _log(check: str, table: str, passed: bool, detail: str):
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"  [{status}] {table} | {check}: {detail}")
    if not passed:
        raise ValueError(f"Quality check failed — {table} | {check}: {detail}")


def check_row_count(df: DataFrame, table_name: str, min_rows: int):
    """Fail if row count is below the expected minimum."""
    count = df.count()
    _log("row_count", table_name, count >= min_rows, f"{count:,} rows (min {min_rows:,})")


def check_nulls(df: DataFrame, table_name: str, columns: List[str], max_null_pct: float = 0.001):
    """Fail if any specified column exceeds the null percentage threshold."""
    total = df.count()
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        pct = null_count / total if total > 0 else 0
        passed = pct <= max_null_pct
        _log("null_check", table_name, passed,
             f"{col}: {null_count} nulls ({pct:.4%}) — threshold {max_null_pct:.4%}")


def check_value_ranges(df: DataFrame, table_name: str, ranges: Dict[str, Tuple[float, float]]):
    """Fail if any value in a column falls outside the expected [min, max] range."""
    for col, (lo, hi) in ranges.items():
        out_of_range = df.filter(
            F.col(col).isNotNull() & ((F.col(col) < lo) | (F.col(col) > hi))
        ).count()
        passed = out_of_range == 0
        _log("range_check", table_name, passed,
             f"{col}: {out_of_range} values outside [{lo}, {hi}]")


def check_no_duplicates(df: DataFrame, table_name: str, key_columns: List[str]):
    """Fail if duplicate rows exist on the specified key columns."""
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    dupes = total - distinct
    passed = dupes == 0
    _log("duplicate_check", table_name, passed,
         f"{dupes} duplicate rows on {key_columns}")


def check_date_gaps(df: DataFrame, table_name: str, date_col: str = "date", max_gap_days: int = 5):
    """Warn if any gap between consecutive dates exceeds max_gap_days (accounts for weekends/holidays)."""
    from pyspark.sql import Window
    w = Window.orderBy(date_col)
    gaps = (
        df.withColumn("prev_date", F.lag(date_col, 1).over(w))
          .withColumn("gap_days", F.datediff(F.col(date_col), F.col("prev_date")))
          .filter(F.col("gap_days") > max_gap_days)
          .select(date_col, "prev_date", "gap_days")
    )
    gap_count = gaps.count()
    passed = gap_count == 0
    _log("date_gap_check", table_name, passed,
         f"{gap_count} gaps exceeding {max_gap_days} days" if gap_count else f"no gaps > {max_gap_days} days")
    if not passed:
        print("    Gaps found:")
        gaps.show(10)


def run_quality_checks(
    df: DataFrame,
    table_name: str,
    min_rows: int = 0,
    null_cols: Optional[List[str]] = None,
    range_checks: Optional[Dict[str, Tuple[float, float]]] = None,
    check_duplicates_on: Optional[List[str]] = None,
    max_gap_days: Optional[int] = None,
    max_null_pct: float = 0.001,
):
    """
    Run all configured quality checks on a DataFrame.

    Parameters
    ----------
    df                 : Spark DataFrame to check
    table_name         : Label used in output messages
    min_rows           : Minimum acceptable row count
    null_cols          : Columns that must not exceed max_null_pct nulls
    range_checks       : Dict of {column: (min_value, max_value)}
    check_duplicates_on: List of columns forming the unique key
    max_gap_days       : Flag date gaps larger than this many days
    max_null_pct       : Null fraction threshold (default 0.1%)
    """
    print(f"\nQuality checks: {table_name}")

    if min_rows:
        check_row_count(df, table_name, min_rows)

    if null_cols:
        check_nulls(df, table_name, null_cols, max_null_pct)

    if range_checks:
        check_value_ranges(df, table_name, range_checks)

    if check_duplicates_on:
        check_no_duplicates(df, table_name, check_duplicates_on)

    if max_gap_days:
        check_date_gaps(df, table_name, max_gap_days=max_gap_days)

    print(f"  All checks passed for {table_name}\n")


def log_pipeline_run(spark, stage: str, table_name: str, df: DataFrame, date_col: str = "date"):
    """
    Append one row to pipeline_run_log after a successful table write.

    pipeline_run_log is a Delta table in the Hive Metastore — it persists across
    cluster restarts and accumulates a full history of every pipeline execution.
    Works on Databricks Community Edition (standard Delta Lake, no premium features needed).
    """
    from datetime import datetime, timezone
    import pyspark.sql.types as T

    schema = T.StructType([
        T.StructField("run_timestamp", T.TimestampType(), False),
        T.StructField("stage",         T.StringType(),    False),
        T.StructField("table_name",    T.StringType(),    False),
        T.StructField("rows_written",  T.LongType(),      False),
        T.StructField("date_min",      T.StringType(),    True),
        T.StructField("date_max",      T.StringType(),    True),
        T.StructField("status",        T.StringType(),    False),
    ])

    row_count = df.count()
    date_min = date_max = None
    if date_col in df.columns:
        stats = df.agg(
            F.min(date_col).cast("string").alias("mn"),
            F.max(date_col).cast("string").alias("mx"),
        ).collect()[0]
        date_min, date_max = stats["mn"], stats["mx"]

    log_row = spark.createDataFrame(
        [(datetime.now(timezone.utc), stage, table_name, row_count, date_min, date_max, "SUCCESS")],
        schema=schema,
    )
    log_row.write.format("delta").mode("append").saveAsTable("pipeline_run_log")
    print(f"  [pipeline_run_log] {stage} | {table_name}: {row_count:,} rows | {date_min} → {date_max}")
