# Aligning Mixed-Frequency Central Bank Rates with Daily Equity Data on Databricks

*Draft for Medium — target publication: ~2000 words*

---

## Introduction

Central bank interest rates are published once a month. Equity prices change every trading day.
If you want to study how policy rates relate to market volatility or drawdowns, you need both
series on the same daily timeline — and a naive join leaves you with hundreds of nulls per year.

This post walks through how I solved this in a Databricks medallion pipeline using a SQL window
function, and why the ordering of that window matters for avoiding lookahead bias.

---

## The problem: two different clocks

The US Federal Reserve publishes the Fed Funds Rate monthly. The ECB's Deposit Facility Rate
follows the same cadence. Yahoo Finance gives us S&P 500 and Euro Stoxx 50 prices for every
trading day — around 250 per year.

A simple inner join on `date` returns only the ~12 trading days per year where a rate
observation happens to fall on an equity trading day. A left join gives you the full equity
timeline but leaves `fed_rate` null on the other 238 days.

```python
# This is what a naive left join produces:
# date         close   fed_rate
# 2024-01-02   4742    null
# 2024-01-03   4697    null
# ...
# 2024-02-01   4958    5.33     ← rate published
# 2024-02-02   4906    null
```

What we actually want: each trading day should carry the most recently *known* rate — not a
future rate that hasn't been published yet.

---

## The solution: forward-fill with an unbounded preceding window

PySpark's window functions let you look backward arbitrarily far using `Window.unboundedPreceding`.
The `F.last(..., ignorenulls=True)` aggregate skips nulls and returns the most recent non-null value.

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

fill_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

def forward_fill_rate(daily_df, monthly_df, rate_col):
    joined = daily_df.join(monthly_df, on="date", how="left")
    return joined.withColumn(
        rate_col,
        F.last(F.col(rate_col), ignorenulls=True).over(fill_window)
    )

market = forward_fill_rate(sp500, fed_rate, "fed_rate")
market = forward_fill_rate(market, ecb_rate, "ecb_rate")
```

After this, every row has a rate value — and critically, the value on 2024-01-02 is the rate
from December 2023, not the February 2024 rate published later.

---

## Why `rowsBetween` matters

`.rowsBetween(Window.unboundedPreceding, Window.currentRow)` defines the frame as "all rows up to
and including the current row". This is exactly what we need.

If you accidentally use `rangeBetween` or forget `Window.currentRow` as the upper bound, you can
introduce lookahead bias: the window might expand to include future rows, meaning a row in January
could see a rate that wasn't published until March. In a research or backtesting context, that
corrupts results silently.

The `.orderBy("date")` clause is mandatory. Without it, the window has no defined ordering and
PySpark will raise an `AnalysisException`. Always specify the ordering column explicitly.

---

## Applying both rates in one pass

The pipeline handles two central bank series: the US Fed Funds Rate and the ECB Deposit Facility Rate.
Both are monthly; both need to be aligned to the same daily equity spine.

The same `forward_fill_rate` function handles both by accepting the column name as a parameter:

```python
sp500_with_rates = forward_fill_rate(sp500, fed, "fed_rate")
sp500_with_rates = forward_fill_rate(sp500_with_rates, ecb, "ecb_rate")
```

Once both rates are on every daily row, we can compute the policy rate differential:

```python
market = market.withColumn(
    "policy_rate_diff",
    F.round(F.col("fed_rate") - F.col("ecb_rate"), 4)
)
```

This single column captures the entire US–EU monetary policy divergence story: from near-parity
in 2015, to extreme divergence in 2022–2023 when the Fed hiked aggressively while the ECB moved
more gradually, and back toward alignment in 2024.

---

## The data quality check

Before writing the Silver table, the pipeline verifies the forward-fill worked. A small quality
check confirms no nulls remain in the rate columns after the join:

```python
run_quality_checks(
    market, "silver_market",
    null_cols=["fed_rate", "ecb_rate"],
    range_checks={"sp500_log_return": (-0.15, 0.15)},
    check_duplicates_on=["date"],
)
```

The `null_cols` check would catch a broken forward-fill immediately — for example, if the FRED
series started later than the equity data, leaving the earliest rows with no rate to carry forward.
The pipeline fails fast and surfaces the exact column and count, rather than silently propagating
nulls into the Gold analytics layer.

---

## What this unlocks in the Gold layer

Once rates are aligned to the daily spine, the Gold notebook can compute:

- **Rolling US–EU rate differential** — a daily view of monetary policy divergence over time
- **Rate regime classifications** — bucketing the ECB rate into `negative / low / medium / high`
  (the ECB held negative rates from 2014 to 2022, an important structural feature for European
  financial analysis)
- **Equity volatility regimes** — comparing 20-day and 60-day realised volatility side by side
  with the VIX to see when implied and realised vol diverge
- **60-day rolling US–EU equity correlation** — how closely do S&P 500 and Euro Stoxx 50 returns
  move together, and does that change when central bank policy diverges?

All of this flows from one clean Silver table with no nulls, no lookahead, and a reproducible
transformation you can step through line by line in a Databricks notebook.

---

## Running it yourself

The full pipeline is on GitHub. To run it:

1. Clone the repo and connect it to Databricks Repos.
2. Run `notebooks/01_bronze_ingest.py` (needs a FRED API key — free at fred.stlouisfed.org).
3. Run `02_silver_transform.py` and `03_gold_analytics.py`.
4. Export the Gold table with `04_export_parquet.py`, download it locally with
   `python scripts/download_gold.py`, then launch the Streamlit dashboard:
   `streamlit run dashboard/app.py`.

---

## Key takeaways

- A left join + forward-fill window is the standard pattern for aligning mixed-frequency
  time series in Spark. It is simple, scalable, and explicit about what data each row sees.
- `rowsBetween(Window.unboundedPreceding, Window.currentRow)` with `.orderBy("date")` is
  the correct frame. Missing either causes wrong results or an error.
- Forward-fill does not interpolate — it carries the last known value forward. That is intentional:
  you should only use data that was available at the time, not data that came later.
- Quality checks on null rates catch broken joins before they corrupt downstream analytics.

---

*Code: github.com/madhurima-nath/databricks-ai-automated-pipeline*
