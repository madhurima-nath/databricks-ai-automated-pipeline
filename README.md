# Financial Analytics Pipeline on Databricks

[![Tests](https://github.com/madhurima-nath/databricks-ai-automated-pipeline/actions/workflows/test.yml/badge.svg)](https://github.com/madhurima-nath/databricks-ai-automated-pipeline/actions/workflows/test.yml)

An end-to-end data engineering project on Databricks: a medallion architecture pipeline on 15 years
of US and European market data, and a SAS → PySpark converter that automates legacy code migration.

**Pipeline:** Ingests S&P 500, Euro Stoxx 50, VIX, US Federal Funds Rate, and ECB Deposit Facility
Rate through Bronze, Silver, and Gold Delta Lake layers. The Gold layer is the consumption-ready
layer where final business rules and aggregations are applied: rolling volatility, US-EU equity
correlations, central bank policy divergence, drawdown, and regime classifications on 15 years of
real market data.

**Migration tool:** A SAS → PySpark converter that translates legacy SAS code (PROC SORT,
PROC MEANS, PROC SQL, DATA steps) to PySpark DataFrame API or Databricks SQL. Common patterns
are handled by a deterministic rule engine; anything outside those patterns falls back to Claude
(claude-haiku-4-5) via the Anthropic API.

**Built with Claude Code** as a development collaborator — for architecture decisions, the quality
checks module, and the converter's rule engine. The converter deploys the same AI capability as a
production feature: rule engine for recognised patterns, Claude AI fallback for the rest.

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
                      │  Quality checks · Null checks · Range checks · Duplicate detection
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
              │   Dashboard    │  Streamlit — Home · Analytics Dashboard · SAS → PySpark Converter
              └────────────────┘
```

---

## Key technical details

### The Silver layer join

In a SAS script, this join sits alongside ingestion, cleaning, and analytics in the same file.
Silver isolates it: solved once, in one place, with every downstream calculation reading from
the same aligned source.

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

In the Databricks medallion architecture, the Gold layer is the consumption-ready layer where
final business rules and aggregations are applied for specific use cases. A financial analyst
opening the dashboard gets rolling volatility, correlations, drawdown, and regime classifications
as direct answers, with no further calculation needed.

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

Tested with 30 pytest cases: 23 covering every supported SAS construct (sort, aggregation, SQL, DATA step variants, date functions, and warning cases for constructs like RETAIN and INTCK), and 7 covering the pipeline submission script (credential loading, exit codes, job status handling). The full suite runs automatically on every push to main via GitHub Actions. Pipeline notebook execution is validated on Databricks through quality checks at every layer transition.

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
| Multi-task Jobs API | ❌ | ✅ |
| Cluster auto-scaling | ❌ | ✅ |

Code is written to be Unity Catalog-ready — adding catalog prefixes (`trading.bronze.sp500`)
is the only change required when migrating to a full workspace.

---

## Repository structure

```
databricks-ai-automated-pipeline/
│
├── notebooks/                          Databricks notebooks (sync via Repos)
│   ├── 00_quality_checks.py            Reusable quality check module — imported by 02 and 03
│   ├── 01_bronze_ingest.py             Raw ingestion from yfinance + FRED; writes 5 Delta tables
│   ├── 02_silver_transform.py          Clean, join, forward-fill rates; derives log returns
│   └── 03_gold_analytics.py            Rolling vol, correlations, regimes, drawdown
│
├── src/
│   └── converter/
│       ├── __init__.py
│       └── sas_to_pyspark.py           SAS→PySpark/SQL converter — rule-based + LLM fallback
│
├── dashboard/
│   └── app.py                          Streamlit app — Home · Analytics Dashboard · SAS → PySpark Converter
│
├── tests/
│   ├── test_sas_converter.py           23 pytest cases for the SAS converter (all patterns)
│   └── test_scripts.py                 7 pytest cases for run_pipeline.py + download_gold.py
│
├── scripts/
│   └── run_pipeline.py                 Submit the Databricks job and poll for completion
│
├── jobs/
│   └── pipeline_job.json               Databricks Jobs API 2.1 job definition template
│
├── .github/
│   └── workflows/
│       └── test.yml                    GitHub Actions CI — runs pytest on push
│
├── config/                             Reserved for schema DDLs and pipeline configs
│
├── .env.example                        Template for API keys (copy to .env — never commit .env)
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
ANTHROPIC_API_KEY=your_anthropic_key      # optional — needed for complex SAS patterns
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your_personal_access_token
DATABRICKS_JOB_ID=12345                   # set after registering the job below
```

Run tests:
```bash
pytest tests/ -v
```

### Databricks

**First time:**

1. Go to **Repos → Add Repo** and enter this repository URL.
2. Open `notebooks/01_bronze_ingest.py`. A **FRED API Key** widget appears at the top —
   paste your key there before running. The key is never saved to a file or committed to git.
3. Register the pipeline job:
   ```bash
   databricks jobs create --json @jobs/pipeline_job.json
   ```
   Note the returned job ID and add it to `.env` as `DATABRICKS_JOB_ID`.

**Subsequent runs:**

```bash
python scripts/run_pipeline.py --job-id 12345
```

The pipeline fetches from Yahoo Finance and FRED on every run — all five series are updated through the current date automatically. Re-run at any time to pull the latest data.

The Streamlit dashboard is at [financial-analytics-databricks.streamlit.app](https://financial-analytics-databricks.streamlit.app).

---

## Running the pipeline

### Via CLI script

```bash
python scripts/run_pipeline.py --job-id 12345
```

Output:
```
[09:14:22] Submitting job 12345...
[09:14:23] Run ID  : 789
[09:14:23] Track at: https://your-workspace.azuredatabricks.net/#job/12345/run/789

[09:14:53] RUNNING  [⏳ bronze_ingest: RUNNING | 🔒 silver_transform: BLOCKED | 🔒 gold_analytics: BLOCKED]
[09:16:23] RUNNING  [✅ bronze_ingest: TERMINATED/SUCCESS | ⏳ silver_transform: RUNNING | 🔒 gold_analytics: BLOCKED]
[09:17:53] TERMINATED/SUCCESS  [✅ bronze_ingest: TERMINATED/SUCCESS | ✅ silver_transform: TERMINATED/SUCCESS | ✅ gold_analytics: TERMINATED/SUCCESS]

[09:17:53] ✅ Pipeline SUCCESS in 214s

Task results:
  ✅  bronze_ingest                  SUCCESS       110s
  ✅  silver_transform               SUCCESS       56s
  ✅  gold_analytics                 SUCCESS       48s
```

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

- Portfolio: [Financial Analytics Pipeline on Databricks](https://madhurima-nath.github.io/project_related_files/data_migration.html)
- Dashboard: [financial-analytics-databricks.streamlit.app](https://financial-analytics-databricks.streamlit.app)
- Medium: *Migrating Financial Analytics to a Lakehouse on Databricks: A Working Demo* (forthcoming)
- Medium: *The Join SAS Buries: Aligning Data in a Databricks Pipeline* (forthcoming)
- Medium: *Automating SAS-to-PySpark Code Migration with a Rule Engine and AI Fallback* (forthcoming)
