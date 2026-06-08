# Financial Analytics Pipeline on Databricks

A hands-on migration project: a Databricks medallion pipeline on 15 years of US and European
market data, and a SAS → PySpark converter that translates legacy analytics code automatically.

**Pipeline:** Bronze → Silver → Gold across five series — S&P 500, Euro Stoxx 50, VIX, US
Federal Funds Rate, and ECB Deposit Facility Rate. Gold computes rolling volatility, US–EU
equity correlations, US–EU interest rate divergence, and rate regime classifications. The ECB
rate went negative in 2014 and stayed there until 2022 — a regime the US Fed never entered —
which appears clearly in the regime classifications and the US–EU rate differential.

**Migration tool:** Translates legacy SAS code (PROC SORT, PROC MEANS, PROC SQL, DATA steps)
to PySpark. Community mode converts a single block with three preloaded examples (PROC SORT,
PROC MEANS, DATA step) and a fourth tab showing the rule engine limit alongside an LLM-produced
equivalent. Enterprise mode converts a full script using a config file that maps SAS library names
and variables to Databricks paths — the dashboard preloads a four-block example: three blocks
handled by the rule engine and one RETAIN block the rule engine cannot fully translate. Each block
gets a confidence score; blocks below 85% are flagged for review. Converted code and a per-block
review manifest are downloadable.

**Built with Claude Code** as a development collaborator — for architecture decisions, the quality
checks module, and the converter's rule engine.

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
              │  silver_market │  Aligned to daily · Log returns · US–EU rate differential
              └───────┬────────┘
                      │  Quality checks · Duplicate detection · Log return range checks
              ┌───────▼────────┐
              │   Gold Layer   │  Analytics-ready aggregates
              │ gold_analytics │  Rolling vol · Correlations · Regimes · % decline from peak
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │   Dashboard    │  Streamlit — Home · Analytics Dashboard · SAS → PySpark Converter
              └────────────────┘
```

---

## Key technical details

### What the medallion architecture makes explicit

In a SAS script, ingestion, cleaning, joining, and business logic typically share one file. A
change to any part reruns everything; there is no clean boundary between what was ingested and
what was computed.

The medallion architecture separates each step. Bronze preserves source data exactly as received.
Silver owns data preparation — including the forward-fill join that aligns monthly interest rates
onto the daily trading sequence. Gold applies business rules on top of the prepared data. Each
layer is independently testable; a change to Gold does not touch Silver.

### Bronze layer

Five Delta tables written without any transforms — source data preserved exactly as received:
`bronze_sp500`, `bronze_eurostoxx`, `bronze_vix` from Yahoo Finance (daily OHLCV), and
`bronze_fed_rate`, `bronze_ecb_rate` from FRED (monthly rates). Because Bronze never transforms,
the full history can be re-derived by rerunning Silver against Bronze if any cleaning rule changes —
without calling the original APIs again.

### Data quality layer

Quality checks run at Bronze → Silver and Silver → Gold transitions. The `00_quality_checks`
module works across any project, not just this one:

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

Gold is where business logic runs on top of the prepared Silver data. Rolling volatility,
correlations, rate regimes, and price decline from peak are computed here and read directly
by the dashboard — no further calculation needed.

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

**Enterprise mode** converts a full SAS script using a config file that maps SAS library names
and variables to Databricks table paths. The dashboard preloads a four-block example — PROC SORT,
PROC MEANS, a DATA step filter, and a RETAIN cumulative-return block. Blocks 1–3 are handled by
the rule engine; Block 4 (`RETAIN`) cannot be fully translated by the rule engine and is flagged
for review. The dashboard shows the LLM-produced window function equivalent alongside the flag.
Output appears per block; confidence score below 85% triggers a "Needs review" label. Converted
code and manifest are downloadable.

```python
from src.converter import convert_script, load_config_from_dict, generate_manifest

config = load_config_from_dict({
    "source": {
        "library_mappings": {"risklib": "trading.bronze"},
        "macro_vars": {"start_date": "2010-01-01"},  # SAS &start_date → "2010-01-01"
    },
    "target": {"platform": "enterprise", "unity_catalog": False},  # set True for a full workspace
})

results = convert_script(sas_script, config=config)
manifest_yaml = generate_manifest(results, source_label="risk_models.sas")
```

`generate_manifest()` produces a YAML summary of each block — how it was converted, its
confidence score, and anything that needs a manual check.

Tested with 46 pytest cases: 39 covering the SAS converter (all supported patterns, Community and Enterprise modes) and 7 covering the pipeline script (credential loading, exit codes, job status handling). Pipeline notebook execution is validated on Databricks through quality checks at every layer transition.

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
│   └── 03_gold_analytics.py            Rolling vol, correlations, rate regimes, % decline from 52-week high
│
├── src/
│   └── converter/
│       ├── __init__.py
│       ├── sas_to_pyspark.py           SAS→PySpark/SQL converter — rule engine + LLM fallback
│       ├── migration_config.py         Config loader — maps SAS library names and variables to Databricks paths
│       └── manifest.py                 Generates a conversion summary per block
│
├── dashboard/
│   └── app.py                          Streamlit app — Home · Analytics Dashboard · SAS → PySpark Converter
│
├── tests/
│   ├── test_sas_converter.py           39 pytest cases — all supported SAS patterns, Community and Enterprise modes
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
├── config/
│   └── migration_config_example.yaml   Example config — library name mappings, variable substitutions, platform settings
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

Live dashboard: [financial-analytics-databricks.streamlit.app](https://financial-analytics-databricks.streamlit.app)

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
| US Fed Funds Rate | `FEDFUNDS` | FRED (Federal Reserve Economic Data) | Monthly — free API key |
| ECB Deposit Facility Rate | `ECBDFR` | FRED | Monthly |

---

## Related

- Portfolio: [Financial Analytics Pipeline on Databricks](https://madhurima-nath.github.io/project_related_files/data_migration.html)
- Dashboard: [Live Streamlit dashboard](https://financial-analytics-databricks.streamlit.app)
- Medium: [Migrating Financial Analytics to a Lakehouse on Databricks: A Working Demo](https://medium.com/@m.nath/migrating-financial-analytics-to-a-lakehouse-on-databricks-a-working-demo-38a3eb9f16d5)
- Medium: [A SAS Migration on Databricks: A Hands-On Project](https://medium.com/@m.nath/a-sas-migration-on-databricks-a-hands-on-project-13582e8f7004)
- Medium: *Automating SAS-to-PySpark Code Migration with a Rule Engine and AI Fallback* (forthcoming)
