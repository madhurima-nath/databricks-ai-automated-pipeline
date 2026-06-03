# Cross-Market Macro Analysis Platform

An end-to-end data engineering project built on Databricks, demonstrating a production-grade
Medallion Architecture pipeline alongside an AI-powered legacy code migration tool.

## What this project does

**Pipeline:** Ingests US and European market data — S&P 500, Euro Stoxx 50, VIX, US Federal
Funds Rate, and ECB Deposit Facility Rate — through Bronze, Silver, and Gold Delta Lake layers.
The Gold layer surfaces volatility regimes, US–EU equity correlations, central bank policy
divergence, and drawdown analysis via a Streamlit dashboard.

**Migration tool:** A SAS → PySpark converter that translates legacy SAS code (PROC SORT,
PROC MEANS, PROC SQL, DATA steps) to PySpark DataFrame API or Databricks SQL. Common patterns
are handled by a deterministic rule engine; complex or ambiguous code is routed to Claude
(claude-haiku-4-5) via the Anthropic API.

---

## Architecture

```
External APIs
  yfinance  →  ^GSPC (S&P 500)   ^STOXX50E (Euro Stoxx 50)   ^VIX
  FRED      →  FEDFUNDS (US rate, monthly)   ECBDFR (ECB rate, monthly)
                      │
              ┌───────▼────────┐
              │  Bronze Layer  │  Raw Delta tables — no transforms
              │  bronze_sp500  │  bronze_eurostoxx  bronze_vix
              │  bronze_fed_rate  bronze_ecb_rate   │
              └───────┬────────┘
                      │  Quality checks · Schema validation · Range checks
              ┌───────▼────────┐
              │  Silver Layer  │  Cleaned · Joined · Forward-filled
              │  silver_market │  Daily spine · Log returns · Rate differential
              └───────┬────────┘
                      │  Quality checks · Duplicate detection · Return bounds
              ┌───────▼────────┐
              │   Gold Layer   │  Analytics-ready aggregates
              │ gold_analytics │  Rolling vol · Correlations · Regimes · Drawdown
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │   Dashboard    │  Streamlit — market analysis + SAS converter
              └────────────────┘
```

---

## Key technical details

### Mixed-frequency time-series join

The Fed Funds Rate and ECB rate are monthly; equity data is daily. A standard join leaves most
rows null. Silver resolves this with a forward-fill window function:

```python
fill_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn(
    "fed_rate",
    F.last(F.col("fed_rate"), ignorenulls=True).over(fill_window)
)
```

Each trading day carries the most recently observed rate — a point-in-time join that avoids
lookahead bias.

### Data quality layer

Quality checks run at Bronze → Silver and Silver → Gold transitions. The `00_quality_checks`
module is domain-agnostic and reusable across projects:

```python
run_quality_checks(
    df, "bronze_sp500",
    min_rows=3000,
    null_cols=["close"],
    range_checks={"close": (100, 10000)},
    check_duplicates_on=["date"],
)
```

Each check logs PASS or FAIL with detail. A FAIL raises immediately to halt the pipeline.

### Gold layer metrics

| Metric | Description |
|--------|-------------|
| `sp500_vol_20d / 60d` | Rolling realised volatility, annualised (std of log returns × √252) |
| `eurostoxx_vol_20d / 60d` | Same for Euro Stoxx 50 |
| `us_eu_equity_corr_60d` | 60-day rolling Pearson correlation between US and EU equity returns |
| `sp500_vix_corr_60d` | 60-day correlation between S&P 500 return and VIX (typically strongly negative) |
| `sp500_drawdown_52w` | % drawdown from 52-week rolling high |
| `us_rate_regime` | `low` / `medium` / `high` based on Fed Funds Rate |
| `eu_rate_regime` | `negative` / `low` / `medium` / `high` based on ECB rate |
| `policy_divergence` | `us_significantly_higher` / `us_higher` / `aligned` / `eu_higher` |
| `vix_regime` | `calm` (<20) / `elevated` (20–30) / `stress` (≥30) |

### SAS → PySpark converter

Handles: `PROC SORT`, `PROC MEANS`, `PROC SQL`, `DATA` steps with `KEEP`, `DROP`, `RENAME`,
`WHERE`, `IF-THEN-ELSE`, `RETAIN` (with warning), date functions (`TODAY()`, `MDY()`),
`INTCK` (with warning), and SAS macro variables.

```python
from src.converter.sas_to_pyspark import convert

result = convert("""
PROC SORT DATA=customers;
    BY region last_name;
RUN;
""", target="pyspark")

print(result.output)
# customers_df = customers_df.orderBy("region", "last_name")

print(result.notes)
# ['PROC SORT → .orderBy()']
```

Tested with 23 pytest cases covering all major SAS patterns.

---

## Community Edition constraints

This project runs on Databricks Community Edition. Compared to a full workspace:

| Feature | Community Edition | Premium |
|---------|-----------------|---------|
| Delta Lake | ✅ | ✅ |
| Hive Metastore | ✅ | ✅ |
| Unity Catalog | ❌ | ✅ |
| Delta Live Tables | ❌ | ✅ |
| Databricks Apps | ❌ | ✅ |
| Cluster auto-scaling | ❌ | ✅ |

Code is written to be Unity Catalog-ready — adding catalog prefixes (`trading.bronze.sp500`)
is the only change required when migrating to a full workspace.

---

## Repository structure

```
databricks-ai-automated-pipeline/
├── notebooks/
│   ├── 00_quality_checks.py     # Reusable quality check functions (%run from other notebooks)
│   ├── 01_bronze_ingest.py      # Raw ingestion — yfinance + FRED
│   ├── 02_silver_transform.py   # Clean, join, forward-fill, derive returns
│   └── 03_gold_analytics.py     # Rolling metrics, regimes, drawdown
├── src/
│   └── converter/
│       └── sas_to_pyspark.py    # SAS → PySpark / Databricks SQL converter
├── dashboard/
│   └── app.py                   # Streamlit dashboard (market analysis + converter UI)
├── tests/
│   └── test_sas_converter.py    # 23 pytest cases — runs locally, no Spark needed
├── config/                      # Reserved for schema DDLs and pipeline configs
├── docs/                        # Architecture diagrams, article drafts
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Setup

### Local

```bash
git clone https://github.com/madhurima-nath/databricks-ai-automated-pipeline
cd databricks-ai-automated-pipeline
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your API keys:

```
FRED_API_KEY=your_fred_key
ANTHROPIC_API_KEY=your_anthropic_key   # optional — needed for complex SAS patterns
```

Run tests:
```bash
pytest tests/ -v
```

Run dashboard locally (requires `data/gold_analytics.parquet` — export from Databricks first):
```bash
streamlit run dashboard/app.py
```

### Databricks

1. In your Databricks workspace, go to **Repos → Add Repo** and enter this repository URL.
2. Open `notebooks/01_bronze_ingest.py`, uncomment the DBFS key setup cell, paste your FRED key, run once, then re-comment.
3. Run notebooks in order: `01` → `02` → `03`.

---

## Data sources

| Source | Series | Provider | Notes |
|--------|--------|----------|-------|
| S&P 500 | `^GSPC` | Yahoo Finance | Daily OHLCV, 2010–present |
| Euro Stoxx 50 | `^STOXX50E` | Yahoo Finance | Daily close, 2010–present |
| VIX | `^VIX` | Yahoo Finance | Daily close |
| US Fed Funds Rate | `FEDFUNDS` | FRED (St. Louis Fed) | Monthly — free API key |
| ECB Deposit Facility Rate | `ECBDFR` | FRED | Monthly |

---

## Related

- Medium: *Aligning Mixed-Frequency Central Bank Rates with Daily Equity Data on Databricks* (forthcoming)
- Medium: *From SAS to PySpark: Migrating Legacy Financial Analytics Code to a Modern Lakehouse* (forthcoming)
