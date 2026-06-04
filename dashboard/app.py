"""
Streamlit Dashboard — Financial Analytics Pipeline on Databricks
=====================================================
Three pages:
  1. Analytics Dashboard     — equity indices, rates, volatility, regimes (reads live from Databricks)
  2. Pipeline Control        — trigger and monitor Bronze→Silver→Gold jobs on Databricks
  3. SAS → PySpark Converter — convert legacy SAS code to PySpark or Databricks SQL

Run locally:
    streamlit run dashboard/app.py

Deploy to Streamlit Community Cloud:
    Connect your GitHub repo at share.streamlit.io.
    Add secrets under Settings → Secrets (see .streamlit/secrets.toml.example).
"""

import os
import sys
import datetime

import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def _secret(key: str, default: str = "") -> str:
    """Read from Streamlit secrets (Cloud) with fallback to environment variables (local)."""
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)


st.set_page_config(
    page_title="Financial Analytics Pipeline on Databricks",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

page = st.sidebar.radio(
    "Navigation",
    ["Analytics Dashboard", "SAS → PySpark Converter"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**About this app**\n\n"
    "This is the output of a data engineering pipeline built on Databricks. "
    "Market data from Yahoo Finance and FRED (the US Federal Reserve's public data service) is processed through three layers — "
    "Bronze (raw), Silver (cleaned and joined), Gold (analytics-ready) — "
    "and served here for exploration.\n\n"
    "**Data:** S&P 500 (US stocks) · Euro Stoxx 50 (European stocks) · VIX (market uncertainty index) · US Fed Funds Rate · ECB Deposit Facility Rate\n\n"
    "**Period:** 2010 to present\n\n"
    "[View project on GitHub ↗](https://github.com/madhurima-nath/databricks-ai-automated-pipeline)"
)


# ---------------------------------------------------------------------------
# Shared Databricks REST API helper (used by Pipeline Control)
# ---------------------------------------------------------------------------

def _db_api(method: str, path: str, host: str, token: str, **kwargs):
    """Call the Databricks REST API. Returns (data_dict, error_str)."""
    url = f"{host.rstrip('/')}/api/{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = getattr(requests, method)(url, headers=headers, timeout=15, **kwargs)
        if resp.status_code == 401:
            return None, "Authentication failed — check your personal access token."
        if resp.status_code == 404:
            return None, f"Not found: {url}"
        resp.raise_for_status()
        return resp.json(), None
    except requests.exceptions.ConnectionError:
        return None, f"Cannot reach {host} — check DATABRICKS_HOST."
    except Exception as exc:
        return None, str(exc)


TASK_EMOJI = {
    "SUCCESS":           "✅",
    "FAILED":            "❌",
    "TIMEDOUT":          "⏱",
    "CANCELED":          "🚫",
    "INTERNAL_ERROR":    "💥",
    "SKIPPED":           "⏭",
    "RUNNING":           "⏳",
    "PENDING":           "🔵",
    "BLOCKED":           "🔒",
    "WAITING_FOR_RETRY": "🔁",
    "TERMINATING":       "🔶",
    "QUEUED":            "🔵",
}


def _run_status_emoji(life_cycle: str, result: str = "") -> str:
    if result:
        return TASK_EMOJI.get(result, "❓")
    return TASK_EMOJI.get(life_cycle, "⏳")


# ---------------------------------------------------------------------------
# Page 1: Analytics Dashboard
# ---------------------------------------------------------------------------

if page == "Analytics Dashboard":
    st.title("Analytics Dashboard")
    st.info(
        "**How to use this dashboard** \n\n"
        "The charts below show how US and European stock markets and interest rates moved over 15 years — "
        "and how they influenced each other.\n\n"
        "1. Set the **date range** below to focus on a period of interest.\n"
        "2. Read each chart top to bottom — they build on each other: market prices → interest rates → how much markets swung → how connected they were.\n"
        "3. Suggested periods to explore: **March 2020** (COVID crash) · **2022–2023** (fastest rate rises in decades) · **2014–2022** (ECB negative rates)."
    )

    host      = _secret("DATABRICKS_HOST")
    token     = _secret("DATABRICKS_TOKEN")
    http_path = _secret("DATABRICKS_HTTP_PATH")

    @st.cache_data(ttl=3600, show_spinner=False)
    def load_gold(host: str, token: str, http_path: str):
        """
        Query gold_analytics directly from Databricks via the SQL connector.
        Results are cached for 1 hour (ttl=3600).
        """
        from databricks import sql as dbsql
        import pandas as pd

        conn_args = dict(
            server_hostname=host.replace("https://", ""),
            http_path=http_path,
            access_token=token,
        )
        with dbsql.connect(**conn_args) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM gold_analytics ORDER BY date")
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)

    if not (host and token and http_path):
        st.info(
            "Databricks connection not configured. "
            "Add **DATABRICKS_HOST**, **DATABRICKS_TOKEN**, and **DATABRICKS_HTTP_PATH** "
            "to your Streamlit secrets (or `.env` locally).\n\n"
            "Find HTTP_PATH in Databricks → Compute → your cluster → "
            "Advanced Options → JDBC/ODBC."
        )
        st.stop()

    with st.spinner("Loading data from Databricks..."):
        try:
            df = load_gold(host, token, http_path)
        except Exception as e:
            err_msg = str(e)
            if "cluster" in err_msg.lower() or "terminated" in err_msg.lower():
                st.warning(
                    "The Databricks cluster appears to be stopped. "
                    "Start it from the **Pipeline Control** page or from Databricks directly, "
                    "then reload this page."
                )
            else:
                st.error(f"Could not load data from Databricks: {err_msg}")
            st.stop()

    if df is None or df.empty:
        st.warning(
            "The gold_analytics table is empty. "
            "Run the pipeline first using the **Pipeline Control** page."
        )
        st.stop()

    import pandas as pd
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # --- Date filter ---
    col1, col2 = st.columns(2)
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    with col1:
        start = st.date_input(
            "From", value=pd.to_datetime("2018-01-01").date(),
            min_value=min_date, max_value=max_date,
        )
    with col2:
        end = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)

    mask = (df["date"].dt.date >= start) & (df["date"].dt.date <= end)
    d = df[mask].copy()

    if d.empty:
        st.warning("No data in selected range.")
        st.stop()

    # --- Metrics row ---
    latest = d.iloc[-1]
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("S&P 500",       f"{latest['sp500_close']:,.0f}")
    m2.metric("Euro Stoxx 50", f"{latest['eurostoxx_close']:,.0f}")
    m3.metric("VIX",           f"{latest['vix_close']:.1f}",
              help="Market stress: <20 calm, 20–30 elevated, >30 stress")
    m4.metric("US Fed Rate",   f"{latest['fed_rate']:.2f}%")
    m5.metric("ECB Rate",      f"{latest['ecb_rate']:.2f}%")

    st.markdown("---")

    # --- Chart 1: Equity indices ---
    st.subheader("Equity Indices")
    st.caption(
        "The S&P 500 tracks the 500 largest US companies; the Euro Stoxx 50 tracks the 50 largest in the Eurozone. "
        "Both fell sharply in March 2020 (COVID) and again in 2022 (rate hikes) — notice how closely they moved together."
    )
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=d["date"], y=d["sp500_close"],     name="S&P 500",      line=dict(color="#1f77b4")), secondary_y=False)
    fig.add_trace(go.Scatter(x=d["date"], y=d["eurostoxx_close"], name="Euro Stoxx 50", line=dict(color="#ff7f0e")), secondary_y=True)
    fig.update_layout(height=350, margin=dict(t=10, b=10), legend=dict(orientation="h", y=1.05))
    fig.update_yaxes(title_text="S&P 500",      secondary_y=False)
    fig.update_yaxes(title_text="Euro Stoxx 50", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # --- Chart 2: Central bank rates + differential ---
    st.subheader("Central Bank Policy Rates")
    st.caption(
        "Central banks raise rates to slow inflation and cut them to stimulate growth. "
        "The ECB rate was below zero from 2014 to 2022 — an unusual policy meaning banks were charged to hold cash. "
        "Both the US and EU raised rates sharply from 2022; compare this timing with the market drops in the chart above."
    )
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=d["date"], y=d["fed_rate"],         name="US Fed Funds Rate",       line=dict(color="#1f77b4")))
    fig2.add_trace(go.Scatter(x=d["date"], y=d["ecb_rate"],         name="ECB Deposit Rate",        line=dict(color="#ff7f0e")))
    fig2.add_trace(go.Scatter(x=d["date"], y=d["policy_rate_diff"], name="Differential (US − ECB)", line=dict(color="#2ca02c", dash="dot")))
    fig2.update_layout(height=300, margin=dict(t=10, b=10),
                       yaxis_title="Rate (%)", legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig2, use_container_width=True)

    # --- Chart 3: Volatility ---
    st.subheader("Realised Volatility (annualised)")
    st.caption(
        "How much prices swung up and down each day — larger swings mean more uncertainty. "
        "Spikes here line up with the drops in the equity chart: COVID (2020) and the rate hike period (2022). "
        "The VIX (dotted line) is a widely watched measure of expected market uncertainty — above 30 typically signals a crisis."
    )
    col_a, col_b = st.columns(2)
    with col_a:
        fig3a = go.Figure()
        fig3a.add_trace(go.Scatter(x=d["date"], y=d["sp500_vol_20d"],   name="20-day vol", line=dict(color="#1f77b4")))
        fig3a.add_trace(go.Scatter(x=d["date"], y=d["sp500_vol_60d"],   name="60-day vol", line=dict(color="#aec7e8")))
        fig3a.add_trace(go.Scatter(x=d["date"], y=d["vix_close"] / 100, name="VIX / 100",  line=dict(color="#d62728", dash="dot")))
        fig3a.update_layout(title="S&P 500", height=280, margin=dict(t=30, b=10))
        st.plotly_chart(fig3a, use_container_width=True)
    with col_b:
        fig3b = go.Figure()
        fig3b.add_trace(go.Scatter(x=d["date"], y=d["eurostoxx_vol_20d"], name="20-day vol", line=dict(color="#ff7f0e")))
        fig3b.add_trace(go.Scatter(x=d["date"], y=d["eurostoxx_vol_60d"], name="60-day vol", line=dict(color="#ffbb78")))
        fig3b.update_layout(title="Euro Stoxx 50", height=280, margin=dict(t=30, b=10))
        st.plotly_chart(fig3b, use_container_width=True)

    # --- Chart 4: US–EU correlation ---
    st.subheader("60-Day Rolling Correlation: S&P 500 vs Euro Stoxx 50")
    st.caption(
        "Shows whether the US and European markets moved in the same direction on any given day. "
        "A value near 1 means they moved together; near 0 means independently. "
        "During major global events, correlation tends to spike — both markets respond to the same news at the same time."
    )
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=d["date"], y=d["us_eu_equity_corr_60d"], name="Return correlation",
                              line=dict(color="#9467bd"), fill="tozeroy", fillcolor="rgba(148,103,189,0.1)"))
    fig4.add_hline(y=0, line_dash="dash", line_color="grey")
    fig4.update_layout(height=260, margin=dict(t=10, b=10), yaxis_title="Correlation", yaxis_range=[-1, 1])
    st.plotly_chart(fig4, use_container_width=True)

    # --- Chart 5: S&P 500 drawdown ---
    st.subheader("S&P 500 Drawdown from 52-Week High")
    st.caption(
        "How far the US market has fallen from its highest point in the past year. "
        "A drop of 20% or more is commonly called a market downturn. "
        "A value near 0% means the market is close to its recent high."
    )
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=d["date"], y=d["sp500_drawdown_52w"], name="Drawdown %",
                              line=dict(color="#d62728"), fill="tozeroy", fillcolor="rgba(214,39,40,0.15)"))
    fig5.update_layout(height=250, margin=dict(t=10, b=10), yaxis_title="Drawdown (%)")
    st.plotly_chart(fig5, use_container_width=True)

    # --- Regime summary ---
    st.subheader("Current Regimes")
    st.caption(
        "A summary of the current environment based on the last day in the selected date range — "
        "whether interest rates are historically high or low, how far US and EU rates have diverged, "
        "and whether markets are calm or stressed."
    )
    regime_cols = ["us_rate_regime", "eu_rate_regime", "policy_divergence", "vix_regime"]
    st.dataframe(
        d[regime_cols + ["date"]].tail(1).set_index("date").T.rename(columns=lambda c: str(c.date())),
        use_container_width=True,
    )


# ---------------------------------------------------------------------------
# Page 2: Pipeline Control
# ---------------------------------------------------------------------------

elif page == "Pipeline Control":
    st.title("Pipeline Control")
    st.caption(
        "Trigger and monitor the Bronze → Silver → Gold pipeline on Databricks. "
        "Requires a Databricks workspace with the pipeline job registered."
    )

    with st.expander("Databricks connection", expanded=True):
        host = st.text_input(
            "Workspace host",
            value=_secret("DATABRICKS_HOST"),
            placeholder="https://adb-1234567890.12.azuredatabricks.net",
            help="Your Databricks workspace URL.",
        )
        token = st.text_input(
            "Personal access token",
            type="password",
            value=_secret("DATABRICKS_TOKEN"),
            help="Generate from User Settings → Access tokens in Databricks.",
        )
        job_id_str = st.text_input(
            "Job ID",
            value=_secret("DATABRICKS_JOB_ID"),
            placeholder="12345",
            help="Register the job once with jobs/pipeline_job.json, then paste the ID here.",
        )

    connected = bool(host and token and job_id_str and job_id_str.isdigit())
    job_id    = int(job_id_str) if connected else None

    st.markdown("---")
    st.subheader("Pipeline stages")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Bronze — Raw ingestion**")
        st.caption("Fetches S&P 500, Euro Stoxx 50, VIX, US Fed Rate, ECB Rate "
                   "from yfinance and FRED. Writes 5 raw Delta tables.")
        st.code("01_bronze_ingest.py", language=None)
    with c2:
        st.markdown("**Silver — Clean & align**")
        st.caption("Quality checks, forward-fills monthly rates to daily spine, "
                   "joins all sources, computes log returns and rate differential.")
        st.code("02_silver_transform.py", language=None)
    with c3:
        st.markdown("**Gold — Analytics**")
        st.caption("Rolling volatility, US–EU equity correlations, S&P 500 drawdown, "
                   "VIX regime, rate regime classification, policy divergence.")
        st.code("03_gold_analytics.py", language=None)

    st.markdown("---")

    if not connected:
        st.info("Fill in the connection settings above to enable run controls.")
    else:
        run_col, refresh_col, _ = st.columns([2, 1, 3])
        with run_col:
            run_btn = st.button("▶ Run full pipeline", type="primary", use_container_width=True)
        with refresh_col:
            refresh_btn = st.button("🔄 Refresh", use_container_width=True)

        if run_btn:
            with st.spinner("Submitting pipeline run..."):
                data, err = _db_api("post", "2.1/jobs/run-now", host, token,
                                    json={"job_id": job_id})
            if err:
                st.error(f"Failed to start run: {err}")
            else:
                run_id = data["run_id"]
                st.session_state["latest_run_id"] = run_id
                st.success(f"Run submitted — Run ID: {run_id}")
                st.rerun()

        if refresh_btn:
            st.rerun()

        # --- Recent runs ---
        st.subheader("Recent runs")
        runs_data, runs_err = _db_api(
            "get", f"2.1/jobs/runs/list?job_id={job_id}&limit=10&active_only=false",
            host, token,
        )
        if runs_err:
            st.error(f"Could not fetch runs: {runs_err}")
        else:
            runs = runs_data.get("runs", [])
            if not runs:
                st.info("No runs yet. Click **Run full pipeline** to start.")
            else:
                import pandas as pd

                rows = []
                for r in runs:
                    s         = r["state"]
                    lc        = s["life_cycle_state"]
                    rs        = s.get("result_state", "")
                    emoji     = _run_status_emoji(lc, rs)
                    start_ms  = r.get("start_time", 0)
                    start_str = (
                        datetime.datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M")
                        if start_ms else "—"
                    )
                    dur_ms  = r.get("execution_duration", 0)
                    rows.append({
                        "Run ID":   r["run_id"],
                        "Started":  start_str,
                        "Status":   f"{emoji} {rs or lc}",
                        "Duration": f"{dur_ms // 1000}s" if dur_ms else "—",
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                # --- Task breakdown for most recent run (always shown) ---
                latest_run_id = runs[0]["run_id"]
                run_detail, detail_err = _db_api(
                    "get", f"2.1/jobs/runs/get?run_id={latest_run_id}", host, token
                )
                if detail_err:
                    st.error(f"Could not fetch run details: {detail_err}")
                elif run_detail:
                    tasks = run_detail.get("tasks", [])
                    if tasks:
                        st.markdown("**Latest run — task breakdown**")
                        task_rows = []
                        for t in tasks:
                            ts  = t["state"]
                            tlc = ts["life_cycle_state"]
                            trs = ts.get("result_state", "")
                            em  = _run_status_emoji(tlc, trs)
                            dur = t.get("execution_duration", 0)
                            task_rows.append({
                                "Task":     t["task_key"],
                                "Status":   f"{em} {trs or tlc}",
                                "Duration": f"{dur // 1000}s" if dur else "—",
                            })
                        st.dataframe(pd.DataFrame(task_rows), use_container_width=True, hide_index=True)

                    run_url = f"{host}/#job/{job_id}/run/{latest_run_id}"
                    st.markdown(f"[Open in Databricks UI ↗]({run_url})")

        st.markdown("---")
        st.caption(
            "After a successful run, the **Analytics Dashboard** page reads the updated "
            "Gold table automatically on next load."
        )


# ---------------------------------------------------------------------------
# Page 3: SAS → PySpark Converter
# ---------------------------------------------------------------------------

elif page == "SAS → PySpark Converter":
    st.title("SAS → PySpark Converter")
    st.caption(
        "Converts legacy SAS code to PySpark DataFrame API, Databricks SQL, or dbt YAML. "
        "Common patterns are handled by rules; complex code uses Claude (claude-haiku-4-5) "
        "via the Anthropic API."
    )

    from converter.sas_to_pyspark import convert

    EXAMPLES = {
        "(none)": "",
        "PROC SORT — sort customers by name": (
            "PROC SORT DATA=customers OUT=customers_sorted;\n"
            "    BY last_name first_name;\n"
            "RUN;"
        ),
        "PROC MEANS — regional sales summary": (
            "PROC MEANS DATA=sales MEAN STD MIN MAX;\n"
            "    CLASS region;\n"
            "    VAR revenue units;\n"
            "RUN;"
        ),
        "DATA step — tiering with IF-THEN-ELSE": (
            "DATA output;\n"
            "    SET input;\n"
            "    IF revenue > 1000000 THEN tier = 'platinum';\n"
            "    ELSE IF revenue > 500000 THEN tier = 'gold';\n"
            "    ELSE tier = 'standard';\n"
            "    WHERE year = YEAR(TODAY());\n"
            "    KEEP customer_id revenue tier;\n"
            "RUN;"
        ),
    }

    col_left, col_right = st.columns(2)

    with col_left:
        target = st.selectbox("Target format", ["pyspark", "databricks_sql", "yaml"])
        example_choice = st.selectbox("Load an example", list(EXAMPLES.keys()))
        api_key = st.text_input(
            "Anthropic API key (for complex patterns)",
            type="password",
            value=_secret("ANTHROPIC_API_KEY"),
            help="Required only for patterns not covered by built-in rules. "
                 "Set ANTHROPIC_API_KEY in Streamlit secrets or .env to avoid pasting it here.",
        )
        sas_input = st.text_area(
            "Paste SAS code here",
            value=EXAMPLES[example_choice],
            height=320,
            placeholder="PROC SORT DATA=customers;\n    BY last_name first_name;\nRUN;",
        )
        convert_btn = st.button("Convert", type="primary", use_container_width=True)

    with col_right:
        if convert_btn and sas_input.strip():
            with st.spinner("Converting..."):
                result = convert(sas_input, target=target, api_key=api_key or None)

            st.text_area("Converted code", value=result.output, height=320)

            if result.notes:
                st.markdown("**Conversion notes:**")
                for n in result.notes:
                    st.markdown(f"- {n}")

            if result.warnings:
                st.warning("**Review required:**")
                for w in result.warnings:
                    st.markdown(f"- {w}")

            st.caption(f"Method: `{result.method}` · Target: `{result.target}`")

        elif convert_btn:
            st.info("Paste some SAS code on the left to convert.")

    with st.expander("Example SAS patterns and their equivalents"):
        st.markdown("""
| SAS construct | PySpark equivalent |
|---|---|
| `PROC SORT DATA=x; BY col; RUN;` | `x_df.orderBy("col")` |
| `PROC MEANS DATA=x; CLASS g; VAR v; RUN;` | `x_df.groupBy("g").agg(F.mean("v"))` |
| `PROC SQL; SELECT ... QUIT;` | `spark.sql("SELECT ...")` |
| `DATA out; SET in; KEEP a b; RUN;` | `in_df.select("a", "b")` |
| `IF x > 0 THEN y = 1; ELSE y = 0;` | `.withColumn("y", F.when(x > 0, 1).otherwise(0))` |
| `RETAIN running_total;` | `F.last(..., True).over(Window.unboundedPreceding...)` |
| `WHERE date = TODAY();` | `.filter(F.col("date") == F.current_date())` |
| `RENAME old=new;` | `.withColumnRenamed("old", "new")` |
        """)
