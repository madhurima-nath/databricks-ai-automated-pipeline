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

# Apply any pending navigation before the radio widget renders
if st.session_state.get("_nav_target"):
    st.session_state["nav_page"] = st.session_state.pop("_nav_target")

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
# Home
# ---------------------------------------------------------------------------

if page == "Home":
    st.title("Financial Analytics Pipeline on Databricks")
    st.markdown(
        "How financial teams move from SAS to Databricks: "
        "a working pipeline on real market data, and a tool to migrate the legacy code."
    )

    st.markdown("---")

    col_a, col_b = st.columns(2)

    CARD = (
        "background:#F8FAFC;border:1px solid #E2E8F0;"
        "border-top:3px solid #3B82F6;border-radius:10px;"
        "padding:22px 24px;min-height:400px;box-sizing:border-box;"
    )
    BADGE = (
        "color:white;padding:2px 10px;border-radius:4px;font-size:0.85em;"
        "white-space:nowrap;min-width:54px;text-align:center;display:inline-block;"
    )

    with col_a:
        st.markdown(
            f"""
            <div style="{CARD}">
                <h3 style="color:#1E3A5F;margin-top:0;">The Pipeline</h3>
                <p style="color:#374151;margin-bottom:14px;">
                    15 years of US and European market data processed on Databricks through three Delta Lake layers:
                </p>
                <div style="font-size:0.9em;">
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#7C3D12;{BADGE}">Bronze</span>
                        <span style="color:#374151;flex:1;">5 raw Delta tables: S&amp;P 500, Euro Stoxx 50, VIX, US Fed Rate, ECB Rate</span>
                    </div>
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#475569;{BADGE}">Silver</span>
                        <span style="color:#374151;flex:1;">Cleaned, quality-checked, joined into a single daily time series</span>
                    </div>
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#B45309;{BADGE}">Gold</span>
                        <span style="color:#374151;flex:1;">Volatility, correlations, drawdown, regime classifications</span>
                    </div>
                </div>
                <p style="color:#374151;margin-top:14px;margin-bottom:0;">
                    The Analytics Dashboard shows the Gold layer output on real data.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("Open Analytics Dashboard →", use_container_width=True):
            st.session_state["_nav_target"] = "Analytics Dashboard"
            st.rerun()

    with col_b:
        st.markdown(
            f"""
            <div style="{CARD}">
                <h3 style="color:#1E3A5F;margin-top:0;">The Migration Tool</h3>
                <p style="color:#374151;margin-bottom:12px;">
                    Financial teams have years of SAS analytics code that needs to move to Databricks.
                    This converter translates legacy SAS into PySpark, Databricks SQL, or dbt YAML automatically.
                </p>
                <ul style="color:#374151;margin:0;padding-left:18px;line-height:1.9;">
                    <li>Paste SAS code, choose a target format, get working code back</li>
                    <li>Common patterns handled by a rule engine — no API key needed</li>
                    <li>Complex code falls back to an AI model that flags anything needing review</li>
                    <li>Tested with <strong>23 automated test cases</strong></li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        if st.button("Open SAS to PySpark Converter →", use_container_width=True):
            st.session_state["_nav_target"] = "SAS → PySpark Converter"
            st.rerun()

    st.markdown("---")
    PILL = (
        "background:#EFF6FF;color:#1E40AF;padding:3px 12px;"
        "border-radius:12px;font-size:0.82em;font-weight:500;"
        "border:1px solid #BFDBFE;white-space:nowrap;"
    )
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:4px 0;'>"
        f"<span style='color:#4B5563;font-size:0.82em;font-weight:500;margin-right:2px;'>Built with</span>"
        f"<span style='{PILL}'>Databricks</span>"
        f"<span style='{PILL}'>PySpark</span>"
        f"<span style='{PILL}'>Delta Lake</span>"
        f"<span style='{PILL}'>Python</span>"
        f"<span style='{PILL}'>Streamlit</span>"
        f"<span style='{PILL}'>Claude AI</span>"
        f"&nbsp;&nbsp;<a href='https://github.com/madhurima-nath/databricks-ai-automated-pipeline' "
        f"target='_blank' rel='noopener' style='color:#3B82F6;font-size:0.85em;text-decoration:none;'>"
        f"View on GitHub ↗</a></div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Page 2: Analytics Dashboard
# ---------------------------------------------------------------------------

if page == "Analytics Dashboard":
    if st.button("← Home", key="home_from_analytics"):
        st.session_state["_nav_target"] = "Home"
        st.rerun()
    st.title("Analytics Dashboard")
    st.markdown(
        "**Bronze** ingests raw market data into Delta tables. "
        "**Silver** cleans it and joins daily equity prices with monthly interest rates. "
        "**Gold** computes the analytics metrics — rolling volatility, correlations, drawdown, and regime classifications. "
        "The patterns and charts below are the output of that pipeline."
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

    _load_note = st.empty()
    _load_note.warning(
        "This dashboard connects live to Databricks. "
        "The first load may take 20–30 seconds while the cluster wakes up. Thank you for your patience."
    )
    with st.spinner("Loading data from Databricks..."):
        try:
            df = load_gold(host, token, http_path)
            _load_note.empty()
        except Exception as e:
            _load_note.empty()
            err_msg = str(e)
            if "cluster" in err_msg.lower() or "terminated" in err_msg.lower():
                st.warning(
                    "The Databricks cluster appears to be stopped. "
                    "Start it from Databricks directly, then reload this page."
                )
            else:
                st.error(f"Could not load data from Databricks: {err_msg}")
            if st.button("← Back to Home"):
                st.session_state["_nav_target"] = "Home"
                st.rerun()
            st.stop()

    if df is None or df.empty:
        st.warning("The gold_analytics table is empty. Run the pipeline notebooks on Databricks first.")
        st.stop()

    import pandas as pd
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    latest = df.iloc[-1]

    # --- Key findings first: this is the point of the dashboard ---
    st.markdown("### Key patterns in the data")
    st.caption("These patterns are in the data — visible once it is loaded, cleaned, and structured through the Bronze, Silver, and Gold pipeline layers.")
    st.markdown(
        """
        <div class="insight-grid">
            <div class="insight-card" style="background:#EFF6FF;border-left:4px solid #3B82F6;">
                <strong style="color:#1E40AF;">Markets crash together</strong><br>
                During COVID (March 2020) and the 2022 rate shock, US and EU markets fell at the same time
                and by similar amounts. Correlation spiked toward 1 on the worst days.
            </div>
            <div class="insight-card" style="background:#FFFBEB;border-left:4px solid #D97706;">
                <strong style="color:#92400E;">ECB's 8 years of negative rates</strong><br>
                2014 to 2022: the ECB held its deposit rate below zero. European banks were charged to hold cash
                rather than earning interest — the longest negative-rate experiment by a major central bank.
            </div>
            <div class="insight-card" style="background:#EFF6FF;border-left:4px solid #3B82F6;">
                <strong style="color:#1E40AF;">2022 rate shock: fastest in 4 decades</strong><br>
                Both the Fed and ECB raised rates from near zero to above 4% in roughly 18 months —
                the fastest pace since the early 1980s. Both stock markets fell over 20%.
            </div>
            <div class="insight-card" style="background:#FFFBEB;border-left:4px solid #D97706;">
                <strong style="color:#92400E;">COVID recovery was unusually fast</strong><br>
                Both markets recovered their pre-COVID highs within 12 months of the March 2020 crash —
                faster than any comparable drop in the dataset.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    st.markdown("---")

    # --- Current state ---
    st.subheader("Current State")
    st.caption(f"As of {latest['date'].strftime('%d %b %Y')} · most recent pipeline run")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("S&P 500",       f"{latest['sp500_close']:,.0f}")
    m2.metric("Euro Stoxx 50", f"{latest['eurostoxx_close']:,.0f}")
    m3.metric("VIX",           f"{latest['vix_close']:.1f}",
              help="Market stress: <20 calm, 20–30 elevated, >30 stress")
    m4.metric("US Fed Rate",   f"{latest['fed_rate']:.2f}%")
    m5.metric("ECB Rate",      f"{latest['ecb_rate']:.2f}%")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    divergence_labels = {
        "us_significantly_higher": "US >> EU",
        "us_higher": "US > EU",
        "eu_higher": "EU > US",
        "aligned": "Aligned",
    }
    vix_labels = {"calm": "Calm", "elevated": "Elevated", "stress": "Stress"}
    rc1, rc2, rc3, rc4 = st.columns(4)
    rc1.metric("US Rate Regime",  latest["us_rate_regime"].capitalize())
    rc2.metric("EU Rate Regime",  latest["eu_rate_regime"].capitalize())
    rc3.metric("US vs EU Rates",  divergence_labels.get(latest["policy_divergence"], latest["policy_divergence"]))
    rc4.metric("Market Stress",   vix_labels.get(latest["vix_regime"], latest["vix_regime"]))

    st.markdown("---")
    st.subheader("Explore the data")
    st.info(
        "Try **March 2020** (COVID crash), **2022** (fastest rate rises in decades), "
        "or **2014–2022** (ECB held rates below zero for eight years)."
    )

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

    st.markdown("")

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

    st.caption("Data covers 2010 – June 2026. Produced by the Bronze → Silver → Gold medallion pipeline on Databricks.")


# ---------------------------------------------------------------------------
# SAS → PySpark Converter
# ---------------------------------------------------------------------------

elif page == "SAS → PySpark Converter":
    if st.button("← Home", key="home_from_converter"):
        st.session_state["_nav_target"] = "Home"
        st.rerun()
    st.title("SAS → PySpark Converter")
    st.markdown(
        "Paste legacy SAS code and get the equivalent PySpark, Databricks SQL, or dbt YAML back automatically. "
        "Common patterns — PROC SORT, PROC MEANS, DATA steps — are handled by a built-in rule engine: "
        "deterministic, no API key needed, same output every time. "
        "Complex code falls back to an AI model that flags anything needing review."
    )
    st.markdown("---")

    from converter.sas_to_pyspark import convert

    EXAMPLES = {
        "Paste your own SAS code": "",
        "PROC SORT: sort customers by name": (
            "PROC SORT DATA=customers OUT=customers_sorted;\n"
            "    BY last_name first_name;\n"
            "RUN;"
        ),
        "PROC MEANS: regional sales summary": (
            "PROC MEANS DATA=sales MEAN STD MIN MAX;\n"
            "    CLASS region;\n"
            "    VAR revenue units;\n"
            "RUN;"
        ),
        "DATA step: customer tiering with IF-THEN-ELSE": (
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

    st.info(
        "The three examples convert accurately without any API key — handled by a built-in rule engine. "
        "For custom SAS code outside the supported patterns, enter your own Anthropic API key in the Advanced section below. "
        "Without one, unrecognised patterns return a placeholder comment."
    )

    st.subheader("Step 1: Choose an example or paste your own SAS code")
    example_choice = st.selectbox(
        "Select an example or start from scratch",
        list(EXAMPLES.keys()),
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

    with st.expander("Advanced: Anthropic API key (for complex patterns outside the rule engine)"):
        api_key = st.text_input(
            "API key",
            type="password",
            value=_secret("ANTHROPIC_API_KEY"),
            help="Only needed for SAS code the built-in rules cannot handle. The three built-in examples do not need this.",
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
