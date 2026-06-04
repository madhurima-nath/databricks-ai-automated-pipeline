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

st.markdown("""
<style>
[data-testid="metric-container"] {
    background: #f8fafc;
    border-radius: 10px;
    padding: 14px 18px;
    border: 1px solid #e2e8f0;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.insight-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 14px;
    margin-top: 12px;
}
.insight-card {
    border-radius: 8px;
    padding: 16px 18px;
    font-size: 0.93em;
    line-height: 1.5;
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

page = st.sidebar.radio(
    "Navigation",
    ["Home", "Analytics Dashboard", "SAS → PySpark Converter"],
    index=0,
    key="nav_page",
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Data** · 2010 – June 2026")
st.sidebar.caption(
    "S&P 500 · Euro Stoxx 50 · VIX\n"
    "US Fed Rate · ECB Rate"
)
st.sidebar.markdown(
    "[View on GitHub ↗](https://github.com/madhurima-nath/databricks-ai-automated-pipeline)"
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
# Home
# ---------------------------------------------------------------------------

if page == "Home":
    st.markdown(
        '<h1 style="margin-bottom:4px;">Financial Analytics Pipeline on Databricks</h1>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p style="font-size:1.1em;color:#555;margin-top:0;">How financial teams move from SAS to Databricks — '
        'a working pipeline on real market data, and a tool to migrate the legacy code.</p>',
        unsafe_allow_html=True,
    )

    st.markdown("---")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown(
            """
            <div style="background:linear-gradient(135deg,#EFF6FF,#DBEAFE);
                        border-top:4px solid #2563EB;border-radius:10px;padding:22px 24px;">
                <h3 style="color:#1D4ED8;margin-top:0;">The Pipeline</h3>
                <p style="color:#374151;margin-bottom:14px;">
                    15 years of US and European market data, processed on Databricks through three Delta Lake layers:
                </p>
                <p style="line-height:2.4;margin:0;">
                    <span style="background:#7C3D12;color:white;padding:3px 10px;border-radius:4px;font-size:0.85em;">Bronze</span>
                    &nbsp;5 raw tables — S&amp;P 500, Euro Stoxx 50, VIX, US Fed Rate, ECB Rate<br>
                    <span style="background:#475569;color:white;padding:3px 10px;border-radius:4px;font-size:0.85em;">Silver</span>
                    &nbsp;Cleaned, quality-checked, joined into a daily time series<br>
                    <span style="background:#B45309;color:white;padding:3px 10px;border-radius:4px;font-size:0.85em;">Gold</span>
                    &nbsp;Volatility, correlations, drawdown, regime classifications
                </p>
                <p style="color:#374151;margin-top:14px;margin-bottom:0;">
                    The Analytics Dashboard shows the Gold layer output on real data.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("Open Analytics Dashboard →", use_container_width=True, type="primary"):
            st.session_state["nav_page"] = "Analytics Dashboard"
            st.rerun()

    with col_b:
        st.markdown(
            """
            <div style="background:linear-gradient(135deg,#F0FDF4,#DCFCE7);
                        border-top:4px solid #16A34A;border-radius:10px;padding:22px 24px;">
                <h3 style="color:#15803D;margin-top:0;">The Migration Tool</h3>
                <p style="color:#374151;margin-bottom:14px;">
                    Financial teams have years of SAS analytics code that needs to move to Databricks.
                    This converter translates legacy SAS into PySpark or Databricks SQL automatically.
                </p>
                <p style="color:#374151;margin-bottom:14px;">
                    Paste SAS code, choose a target format, get working code back.
                    Common patterns are handled by a rule engine — complex code falls back to an AI model
                    that flags anything needing review.
                </p>
                <p style="color:#374151;margin:0;">
                    Tested with <strong>23 automated test cases</strong> covering every supported SAS pattern.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("Open SAS to PySpark Converter →", use_container_width=True, type="primary"):
            st.session_state["nav_page"] = "SAS → PySpark Converter"
            st.rerun()

    st.markdown("---")
    st.markdown(
        '<p style="color:#888;font-size:0.9em;">Stack: Databricks · PySpark · Delta Lake · Python · Streamlit · Anthropic API &nbsp;|&nbsp; '
        '<a href="https://github.com/madhurima-nath/databricks-ai-automated-pipeline" target="_blank">View on GitHub ↗</a></p>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Page 2: Analytics Dashboard
# ---------------------------------------------------------------------------

if page == "Analytics Dashboard":
    st.title("Analytics Dashboard")
    st.markdown(
        "How US and European markets and interest rates moved over 15 years — "
        "pulled from the Databricks pipeline and ready to explore. "
        "Use the date filters to zoom into any period."
    )
    st.info(
        "Try **March 2020** (COVID crash), **2022** (fastest rate rises in decades), "
        "or **2014–2022** (ECB held rates below zero for eight years)."
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

    st.warning(
        "This dashboard connects live to Databricks. "
        "The first load may take 20–30 seconds while the cluster wakes up. Thank you for your patience."
    )
    with st.spinner("Loading data from Databricks..."):
        try:
            df = load_gold(host, token, http_path)
        except Exception as e:
            err_msg = str(e)
            if "cluster" in err_msg.lower() or "terminated" in err_msg.lower():
                st.warning(
                    "The Databricks cluster appears to be stopped. "
                    "Start it from Databricks directly, then reload this page."
                )
            else:
                st.error(f"Could not load data from Databricks: {err_msg}")
            st.stop()

    if df is None or df.empty:
        st.warning("The gold_analytics table is empty. Run the pipeline notebooks on Databricks first.")
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
    st.caption(f"As of {latest['date'].strftime('%d %b %Y')} — last day in selected range")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("S&P 500",       f"{latest['sp500_close']:,.0f}")
    m2.metric("Euro Stoxx 50", f"{latest['eurostoxx_close']:,.0f}")
    m3.metric("VIX",           f"{latest['vix_close']:.1f}",
              help="Market stress: <20 calm, 20–30 elevated, >30 stress")
    m4.metric("US Fed Rate",   f"{latest['fed_rate']:.2f}%")
    m5.metric("ECB Rate",      f"{latest['ecb_rate']:.2f}%")

    st.markdown("---")

    # --- Chart 1: Equity indices ---
    st.markdown('<h3 style="color:#1f77b4;">Equity Indices</h3>', unsafe_allow_html=True)
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

    sp_ret = (d.iloc[-1]["sp500_close"] / d.iloc[0]["sp500_close"] - 1) * 100
    eu_ret = (d.iloc[-1]["eurostoxx_close"] / d.iloc[0]["eurostoxx_close"] - 1) * 100
    better = "S&P 500" if sp_ret > eu_ret else "Euro Stoxx 50"
    st.info(
        f"**In the selected period:** S&P 500 {sp_ret:+.1f}% · Euro Stoxx 50 {eu_ret:+.1f}%. "
        f"**{better} outperformed** over this window."
    )

    # --- Chart 2: Central bank rates + differential ---
    st.markdown('<h3 style="color:#ff7f0e;">Central Bank Policy Rates</h3>', unsafe_allow_html=True)
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

    d_rates = d.dropna(subset=["fed_rate", "ecb_rate"])
    if not d_rates.empty:
        peak_fed = d_rates["fed_rate"].max()
        peak_fed_date = d_rates.loc[d_rates["fed_rate"].idxmax(), "date"].strftime("%b %Y")
        neg_days = int((d_rates["ecb_rate"] < 0).sum())
        rate_msg = f"**US rates peaked at {peak_fed:.2f}% ({peak_fed_date}) in this period.**"
        if neg_days > 0:
            rate_msg += f" The ECB rate was negative on **{neg_days:,} trading days** — European banks were charged to hold cash rather than earning interest on it."
        st.info(rate_msg)

    # --- Chart 3: Volatility ---
    st.markdown('<h3 style="color:#2ca02c;">Realised Volatility</h3>', unsafe_allow_html=True)
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

    d_vol = d.dropna(subset=["sp500_vol_20d"])
    if not d_vol.empty:
        peak_row = d_vol.loc[d_vol["sp500_vol_20d"].idxmax()]
        st.info(f"**Most volatile period in this range:** {peak_row['date'].strftime('%b %Y')} — S&P 500 was swinging roughly **{peak_row['sp500_vol_20d']/16:.1%}** per day on average.")

    # --- Chart 4: US–EU correlation ---
    st.markdown('<h3 style="color:#9467bd;">US vs EU Equity Correlation</h3>', unsafe_allow_html=True)
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

    d_corr = d.dropna(subset=["us_eu_equity_corr_60d"])
    if not d_corr.empty:
        avg_corr = d_corr["us_eu_equity_corr_60d"].mean()
        st.info(
            f"**Average correlation in this period: {avg_corr:.2f}.** "
            + ("The two markets moved in the same direction on most days — holding both offered limited protection during downturns." if avg_corr > 0.6
               else "The two markets showed meaningful independence — holding both would have reduced risk compared to holding either alone.")
        )

    # --- Chart 5: S&P 500 drawdown ---
    st.markdown('<h3 style="color:#d62728;">S&amp;P 500 Drawdown from 52-Week High</h3>', unsafe_allow_html=True)
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

    worst_dd = d["sp500_drawdown_52w"].min()
    worst_dd_date = d.loc[d["sp500_drawdown_52w"].idxmin(), "date"].strftime("%b %Y")
    current_dd = d.iloc[-1]["sp500_drawdown_52w"]
    st.info(
        f"**Worst drop in this period: {worst_dd:.1f}% ({worst_dd_date}).** "
        f"Current position: **{current_dd:.1f}%** from its recent high."
    )

    # --- Regime summary ---
    st.subheader("Current Environment")
    st.caption(
        "A summary as of the last day in the selected date range — "
        "whether interest rates are historically high or low, how far apart US and EU rates are, "
        "and whether markets are calm or stressed."
    )
    latest_regime = d.iloc[-1]
    divergence_labels = {
        "us_significantly_higher": "US rates much higher than EU",
        "us_higher": "US rates higher than EU",
        "eu_higher": "EU rates higher than US",
        "aligned": "US and EU rates roughly equal",
    }
    vix_labels = {
        "calm": "Calm",
        "elevated": "Elevated",
        "stress": "Stress",
    }
    rc1, rc2, rc3, rc4 = st.columns(4)
    rc1.metric("US Interest Rate", latest_regime["us_rate_regime"].capitalize())
    rc2.metric("EU Interest Rate", latest_regime["eu_rate_regime"].capitalize())
    rc3.metric("US vs EU Rates", divergence_labels.get(latest_regime["policy_divergence"], latest_regime["policy_divergence"]))
    rc4.metric("Market Stress", vix_labels.get(latest_regime["vix_regime"], latest_regime["vix_regime"]))

    st.markdown("---")
    st.markdown("### Key findings from the data")
    st.markdown(
        '<p style="color:#555;margin-bottom:12px;">Patterns that emerge consistently from 15 years of daily market data.</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="insight-grid">
            <div class="insight-card" style="background:#FEF9C3;border-left:4px solid #CA8A04;">
                <strong style="color:#92400E;">Markets crash together</strong><br>
                During COVID (March 2020) and the 2022 rate shock, US and EU markets fell at the same time
                and by similar amounts. Correlation spiked toward 1 on the worst days — holding both gave little protection.
            </div>
            <div class="insight-card" style="background:#FEE2E2;border-left:4px solid #DC2626;">
                <strong style="color:#991B1B;">ECB's 8 years of negative rates</strong><br>
                2014 to 2022: the ECB held its deposit rate below zero. European banks were charged to hold cash
                rather than earning interest. The longest negative-rate experiment by a major central bank.
            </div>
            <div class="insight-card" style="background:#E0F2FE;border-left:4px solid #0284C7;">
                <strong style="color:#0C4A6E;">2022 rate shock: fastest in 4 decades</strong><br>
                Both the Fed and ECB raised rates from near zero to above 4% in roughly 18 months —
                the fastest pace since the early 1980s. Both stock markets fell over 20%.
            </div>
            <div class="insight-card" style="background:#F0FDF4;border-left:4px solid #16A34A;">
                <strong style="color:#14532D;">COVID recovery was unusually fast</strong><br>
                Both markets recovered their pre-COVID highs within 12 months of the March 2020 crash —
                faster than any comparable drop in the dataset.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    st.caption("Data covers 2010 – June 2026. Produced by the Bronze → Silver → Gold medallion pipeline on Databricks.")


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
    st.markdown(
        "Paste legacy SAS code and get the equivalent PySpark, Databricks SQL, or dbt YAML back automatically. "
        "Common patterns — PROC SORT, PROC MEANS, DATA steps — are handled by a built-in rule engine. "
        "Complex code falls back to an AI model that flags anything needing review."
    )
    st.markdown("---")

    from converter.sas_to_pyspark import convert

    EXAMPLES = {
        "(none — paste your own)": "",
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

    st.subheader("Step 1: Choose an example or paste your own SAS code")
    example_choice = st.selectbox(
        "Load a sample pattern",
        list(EXAMPLES.keys()),
        help="Select an example to see how the converter works, or choose '(none)' and paste your own code below.",
    )
    sas_input = st.text_area(
        "SAS code",
        value=EXAMPLES[example_choice],
        height=200,
        placeholder="PROC SORT DATA=customers;\n    BY last_name first_name;\nRUN;",
    )

    st.subheader("Step 2: Choose target format and convert")
    c1, c2 = st.columns([2, 1])
    with c1:
        target = st.selectbox(
            "Convert to",
            ["pyspark", "databricks_sql", "yaml"],
            help="PySpark: DataFrame API code · Databricks SQL: SQL statements · YAML: dbt model definition",
        )
    with c2:
        convert_btn = st.button("Convert →", type="primary", use_container_width=True)

    with st.expander("Advanced — Anthropic API key (for complex patterns only)"):
        api_key = st.text_input(
            "API key",
            type="password",
            value=_secret("ANTHROPIC_API_KEY"),
            help="Only needed for SAS code that the built-in rules cannot handle. Leave blank to use rules only.",
        )

    st.markdown("---")
    st.subheader("Step 3: Converted output")

    if convert_btn and sas_input.strip():
        with st.spinner("Converting..."):
            result = convert(sas_input, target=target, api_key=api_key or None)

        st.text_area("", value=result.output, height=250)

        col_n, col_m = st.columns(2)
        with col_n:
            if result.notes:
                st.markdown("**Notes:**")
                for n in result.notes:
                    st.markdown(f"- {n}")
        with col_m:
            if result.warnings:
                st.warning("**Needs review:**")
                for w in result.warnings:
                    st.markdown(f"- {w}")

        st.caption(f"Method: `{result.method}` · Target: `{result.target}`")

    elif convert_btn:
        st.info("Paste some SAS code above to convert.")
    else:
        st.info("Select an example above and click **Convert →** to see the output here.")

    with st.expander("What SAS patterns are supported?"):
        st.markdown("""
| SAS construct | Converts to |
|---|---|
| `PROC SORT DATA=x; BY col; RUN;` | `x_df.orderBy("col")` |
| `PROC MEANS DATA=x; CLASS g; VAR v; RUN;` | `x_df.groupBy("g").agg(F.mean("v"))` |
| `PROC SQL; SELECT ... QUIT;` | `spark.sql("SELECT ...")` |
| `DATA out; SET in; KEEP a b; RUN;` | `in_df.select("a", "b")` |
| `IF x > 0 THEN y = 1; ELSE y = 0;` | `.withColumn("y", F.when(x > 0, 1).otherwise(0))` |
| `WHERE date = TODAY();` | `.filter(F.col("date") == F.current_date())` |
| `RENAME old=new;` | `.withColumnRenamed("old", "new")` |

Patterns not in this list are handled by the AI fallback, which will flag anything requiring manual review.
        """)
