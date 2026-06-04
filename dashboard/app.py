"""
Streamlit Dashboard — Cross-Market Macro Analysis Platform
===========================================================
Three pages:
  1. Market Analysis     — equity indices, rates, volatility, regimes
  2. Pipeline Control    — trigger and monitor Bronze→Silver→Gold on Databricks
  3. SAS → PySpark       — paste legacy SAS code, get PySpark or Databricks SQL

Run locally:
    streamlit run dashboard/app.py

On Databricks Apps:
    Point the app config to this file and set secrets in the Databricks secrets store.
"""

import os
import sys
import datetime

import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

st.set_page_config(
    page_title="Cross-Market Macro Analysis",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

page = st.sidebar.radio(
    "Navigation",
    ["Market Analysis", "Pipeline Control", "SAS → PySpark Converter"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Data sources:** S&P 500, Euro Stoxx 50, VIX (Yahoo Finance) · "
    "US Fed Funds Rate, ECB Deposit Facility Rate (FRED)"
)


# ---------------------------------------------------------------------------
# Shared Databricks API helper
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
    "SUCCESS":        "✅",
    "FAILED":         "❌",
    "TIMEDOUT":       "⏱",
    "CANCELED":       "🚫",
    "INTERNAL_ERROR": "💥",
    "SKIPPED":        "⏭",
    "RUNNING":        "⏳",
    "PENDING":        "🔵",
    "BLOCKED":        "🔒",
    "WAITING_FOR_RETRY": "🔁",
    "TERMINATING":    "🔶",
    "QUEUED":         "🔵",
}


def _run_status_emoji(life_cycle: str, result: str = "") -> str:
    if result:
        return TASK_EMOJI.get(result, "❓")
    return TASK_EMOJI.get(life_cycle, "⏳")


# ---------------------------------------------------------------------------
# Page 1: Market Analysis
# ---------------------------------------------------------------------------

if page == "Market Analysis":
    st.title("Cross-Market Macro Analysis")
    st.caption(
        "US and EU equity markets alongside central bank policy rates — "
        "2010 to present. Data served from the Gold Delta table."
    )

    @st.cache_data(ttl=3600)
    def load_gold():
        try:
            import pandas as pd
            parquet_path = os.path.join(
                os.path.dirname(__file__), "..", "data", "gold_analytics.parquet"
            )
            if os.path.exists(parquet_path):
                return pd.read_parquet(parquet_path)
            return None
        except Exception as e:
            st.error(f"Could not load data: {e}")
            return None

    df = load_gold()

    if df is None:
        st.info(
            "No data loaded. Run the pipeline on Databricks (use the **Pipeline Control** page), "
            "then export the Gold table:\n\n"
            "1. Run `notebooks/04_export_parquet.py` on Databricks\n"
            "2. Locally: `python scripts/download_gold.py`\n"
            "3. Reload this page"
        )
        st.stop()

    import pandas as pd
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    col1, col2 = st.columns(2)
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    with col1:
        start = st.date_input(
            "From", value=pd.to_datetime("2018-01-01").date(),
            min_value=min_date, max_value=max_date
        )
    with col2:
        end = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)

    mask = (df["date"].dt.date >= start) & (df["date"].dt.date <= end)
    d = df[mask].copy()

    if d.empty:
        st.warning("No data in selected range.")
        st.stop()

    latest = d.iloc[-1]
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("S&P 500",      f"{latest['sp500_close']:,.0f}")
    m2.metric("Euro Stoxx 50",f"{latest['eurostoxx_close']:,.0f}")
    m3.metric("VIX",          f"{latest['vix_close']:.1f}",
              help="Market stress: <20 calm, 20–30 elevated, >30 stress")
    m4.metric("US Fed Rate",  f"{latest['fed_rate']:.2f}%")
    m5.metric("ECB Rate",     f"{latest['ecb_rate']:.2f}%")

    st.markdown("---")

    st.subheader("Equity Indices")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=d["date"], y=d["sp500_close"],     name="S&P 500",      line=dict(color="#1f77b4")), secondary_y=False)
    fig.add_trace(go.Scatter(x=d["date"], y=d["eurostoxx_close"], name="Euro Stoxx 50", line=dict(color="#ff7f0e")), secondary_y=True)
    fig.update_layout(height=350, margin=dict(t=10, b=10), legend=dict(orientation="h", y=1.05))
    fig.update_yaxes(title_text="S&P 500",       secondary_y=False)
    fig.update_yaxes(title_text="Euro Stoxx 50",  secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Central Bank Policy Rates")
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=d["date"], y=d["fed_rate"],        name="US Fed Funds Rate",      line=dict(color="#1f77b4")))
    fig2.add_trace(go.Scatter(x=d["date"], y=d["ecb_rate"],        name="ECB Deposit Rate",       line=dict(color="#ff7f0e")))
    fig2.add_trace(go.Scatter(x=d["date"], y=d["policy_rate_diff"],name="Differential (US − ECB)",line=dict(color="#2ca02c", dash="dot")))
    fig2.update_layout(height=300, margin=dict(t=10, b=10),
                       yaxis_title="Rate (%)", legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Realised Volatility (annualised)")
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

    st.subheader("60-Day Rolling Correlation: S&P 500 vs Euro Stoxx 50")
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=d["date"], y=d["us_eu_equity_corr_60d"], name="Return correlation",
                              line=dict(color="#9467bd"), fill="tozeroy", fillcolor="rgba(148,103,189,0.1)"))
    fig4.add_hline(y=0, line_dash="dash", line_color="grey")
    fig4.update_layout(height=260, margin=dict(t=10, b=10), yaxis_title="Correlation", yaxis_range=[-1, 1])
    st.plotly_chart(fig4, use_container_width=True)

    st.subheader("S&P 500 Drawdown from 52-Week High")
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=d["date"], y=d["sp500_drawdown_52w"], name="Drawdown %",
                              line=dict(color="#d62728"), fill="tozeroy", fillcolor="rgba(214,39,40,0.15)"))
    fig5.update_layout(height=250, margin=dict(t=10, b=10), yaxis_title="Drawdown (%)")
    st.plotly_chart(fig5, use_container_width=True)

    st.subheader("Current Regimes")
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

    # --- Connection settings ---
    with st.expander("Databricks connection", expanded=True):
        host  = st.text_input(
            "Workspace host",
            value=os.getenv("DATABRICKS_HOST", ""),
            placeholder="https://adb-1234567890.12.azuredatabricks.net",
            help="Your Databricks workspace URL.",
        )
        token = st.text_input(
            "Personal access token",
            type="password",
            value=os.getenv("DATABRICKS_TOKEN", ""),
            help="Generate from User Settings → Access tokens in Databricks.",
        )
        job_id_str = st.text_input(
            "Job ID",
            value=os.getenv("DATABRICKS_JOB_ID", ""),
            placeholder="12345",
            help="Register the job once with jobs/pipeline_job.json, then paste the ID here.",
        )

    connected = bool(host and token and job_id_str and job_id_str.isdigit())
    job_id    = int(job_id_str) if connected else None

    # --- Pipeline stage overview ---
    st.markdown("---")
    st.subheader("Pipeline stages")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Bronze — Raw ingestion**")
        st.caption("Fetches S&P 500, Euro Stoxx 50, VIX, US Fed Rate, ECB Rate from yfinance and FRED. "
                   "Writes 5 raw Delta tables with no transformations.")
        st.code("01_bronze_ingest.py", language=None)
    with c2:
        st.markdown("**Silver — Clean & align**")
        st.caption("Runs quality checks, forward-fills monthly rates to the daily equity spine, "
                   "joins all sources, computes log returns and policy rate differential.")
        st.code("02_silver_transform.py", language=None)
    with c3:
        st.markdown("**Gold — Analytics**")
        st.caption("Rolling volatility (20d/60d), US–EU equity correlations, S&P 500 drawdown, "
                   "VIX regime, rate regime classification, and policy divergence.")
        st.code("03_gold_analytics.py", language=None)

    st.markdown("---")

    # --- Run controls ---
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

        # --- Recent runs ---
        st.subheader("Recent runs")
        runs_data, runs_err = _db_api(
            "get", f"2.1/jobs/runs/list?job_id={job_id}&limit=10&active_only=false",
            host, token
        )
        if runs_err:
            st.error(f"Could not fetch runs: {runs_err}")
        else:
            runs = runs_data.get("runs", [])
            if not runs:
                st.info("No runs yet for this job. Click **Run full pipeline** to start.")
            else:
                import pandas as pd

                rows = []
                for r in runs:
                    s          = r["state"]
                    lc         = s["life_cycle_state"]
                    rs         = s.get("result_state", "")
                    emoji      = _run_status_emoji(lc, rs)
                    start_ms   = r.get("start_time", 0)
                    start_str  = (
                        datetime.datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M")
                        if start_ms else "—"
                    )
                    dur_ms  = r.get("execution_duration", 0)
                    dur_str = f"{dur_ms // 1000}s" if dur_ms else "—"
                    rows.append({
                        "Run ID":  r["run_id"],
                        "Started": start_str,
                        "Status":  f"{emoji} {rs or lc}",
                        "Duration":dur_str,
                    })

                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                # --- Drill into latest/selected run ---
                latest_run_id = st.session_state.get("latest_run_id") or runs[0]["run_id"]
                select_run_id = st.selectbox(
                    "View task details for run",
                    [r["run_id"] for r in runs],
                    index=0,
                    format_func=lambda rid: str(rid),
                )

                run_detail, detail_err = _db_api(
                    "get", f"2.1/jobs/runs/get?run_id={select_run_id}", host, token
                )
                if detail_err:
                    st.error(f"Could not fetch run details: {detail_err}")
                elif run_detail:
                    tasks = run_detail.get("tasks", [])
                    if tasks:
                        st.markdown("**Task breakdown**")
                        task_rows = []
                        for t in tasks:
                            ts   = t["state"]
                            tlc  = ts["life_cycle_state"]
                            trs  = ts.get("result_state", "")
                            em   = _run_status_emoji(tlc, trs)
                            dur  = t.get("execution_duration", 0)
                            task_rows.append({
                                "Task":     t["task_key"],
                                "Status":   f"{em} {trs or tlc}",
                                "Duration": f"{dur // 1000}s" if dur else "—",
                            })
                        st.dataframe(pd.DataFrame(task_rows), use_container_width=True, hide_index=True)

                    # Link to run in Databricks UI
                    run_url = f"{host}/#job/{job_id}/run/{select_run_id}"
                    st.markdown(f"[Open in Databricks UI ↗]({run_url})")

        # --- Post-pipeline export reminder ---
        st.markdown("---")
        st.markdown("**After a successful run — get the data to the dashboard:**")
        st.code(
            "# 1. Run in Databricks:\n"
            "#    notebooks/04_export_parquet.py\n\n"
            "# 2. Locally:\n"
            "python scripts/download_gold.py\n\n"
            "# 3. Open the Market Analysis page",
            language="bash",
        )


# ---------------------------------------------------------------------------
# Page 3: SAS → PySpark Converter
# ---------------------------------------------------------------------------

elif page == "SAS → PySpark Converter":
    st.title("SAS → PySpark Converter")
    st.caption(
        "Converts legacy SAS code to PySpark DataFrame API, Databricks SQL, or dbt YAML. "
        "Common patterns are handled by rules; complex code uses Claude (claude-haiku-4-5) via the Anthropic API."
    )

    from converter.sas_to_pyspark import convert

    col_left, col_right = st.columns(2)

    with col_left:
        target = st.selectbox("Target format", ["pyspark", "databricks_sql", "yaml"])
        api_key = st.text_input(
            "Anthropic API key (for complex patterns)",
            type="password",
            value=os.getenv("ANTHROPIC_API_KEY", ""),
            help="Required only for patterns not covered by built-in rules. "
                 "Set ANTHROPIC_API_KEY in .env to avoid pasting it here.",
        )
        sas_input = st.text_area(
            "Paste SAS code here",
            height=320,
            placeholder="""PROC SORT DATA=customers;
    BY last_name first_name;
RUN;""",
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
