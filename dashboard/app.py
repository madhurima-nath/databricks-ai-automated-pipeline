"""
Streamlit Dashboard — Financial Analytics Pipeline on Databricks
=====================================================
Three pages:
  0. Home                    — overview of the pipeline and the migration tool
  1. Analytics Dashboard     — equity indices, rates, volatility, regimes (reads live from Databricks)
  2. SAS → PySpark Converter — convert legacy SAS code to PySpark or Databricks SQL

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

_pages = ["Home", "Analytics Dashboard", "SAS → PySpark Converter"]

# On fresh load (session state cleared by refresh), restore page from URL
if "_url_loaded" not in st.session_state:
    st.session_state["_url_loaded"] = True
    _url_page = st.query_params.get("page", "Home")
    if _url_page in _pages:
        st.session_state["nav_page"] = _url_page

# Apply any pending navigation before the radio widget renders
if st.session_state.get("_nav_target"):
    st.session_state["nav_page"] = st.session_state.pop("_nav_target")

page = st.sidebar.radio(
    "Navigation",
    _pages,
    index=0,
    key="nav_page",
)

# Keep URL in sync so refresh returns to the same page
st.query_params["page"] = page

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
    st.markdown("---")

    col_a, col_b = st.columns(2)

    CARD = (
        "background:#F8FAFC;border:1px solid #E2E8F0;"
        "border-top:3px solid #3B82F6;border-radius:10px;"
        "padding:22px 24px;box-sizing:border-box;"
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
                <div style="font-size:0.9em;">
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#7C3D12;{BADGE}">Bronze</span>
                        <span style="color:#374151;flex:1;">S&amp;P 500 · Euro Stoxx 50 · VIX · Fed Rate · ECB Rate</span>
                    </div>
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#475569;{BADGE}">Silver</span>
                        <span style="color:#374151;flex:1;">Cleaned, quality-checked, joined into a single daily time series</span>
                    </div>
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#B45309;{BADGE}">Gold</span>
                        <span style="color:#374151;flex:1;">Volatility, correlations, and rate regime classifications</span>
                    </div>
                </div>
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
                <div style="font-size:0.9em;">
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#1E40AF;{BADGE}">Community</span>
                        <span style="color:#374151;flex:1;">Convert a single SAS block to PySpark</span>
                    </div>
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#6B21A8;{BADGE}">Enterprise</span>
                        <span style="color:#374151;flex:1;">Full script conversion using a config file</span>
                    </div>
                    <div style="display:flex;align-items:flex-start;padding:5px 0;gap:10px;">
                        <span style="background:#065F46;{BADGE}">Tested</span>
                        <span style="color:#374151;flex:1;">39 converter tests · 7 pipeline script tests</span>
                    </div>
                </div>
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
        "background:#DBEAFE;color:#1E40AF;padding:5px 14px;"
        "border-radius:12px;font-size:0.9em;font-weight:600;"
        "border:1px solid #93C5FD;white-space:nowrap;"
    )
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:6px 0;'>"
        f"<span style='color:#374151;font-size:0.9em;font-weight:600;margin-right:2px;'>Built with</span>"
        f"<span style='{PILL}'>Databricks</span>"
        f"<span style='{PILL}'>PySpark</span>"
        f"<span style='{PILL}'>Delta Lake</span>"
        f"<span style='{PILL}'>Python</span>"
        f"<span style='{PILL}'>Streamlit</span>"
        f"<span style='{PILL}'>Claude AI</span>"
        f"&nbsp;&nbsp;<a href='https://github.com/madhurima-nath/databricks-ai-automated-pipeline' "
        f"target='_blank' rel='noopener' style='color:#1D4ED8;font-size:0.9em;font-weight:600;text-decoration:none;'>"
        f"View on GitHub ↗</a></div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Page 2: Analytics Dashboard
# ---------------------------------------------------------------------------

if page == "Analytics Dashboard":
    _bk, _ = st.columns([1, 5])
    with _bk:
        if st.button("← Home", key="home_from_analytics", use_container_width=True):
            st.session_state["_nav_target"] = "Home"
            st.rerun()
    st.title("Analytics Dashboard")
    st.markdown(
        "In the Databricks medallion architecture, the Gold layer is where final business rules and "
        "aggregations are applied for specific use cases. This dashboard shows the Gold layer output: "
        "rolling volatility, cross-market correlations, drawdown, and rate regime classifications "
        "on 15 years of real US and European market data."
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
            if st.button("← Home"):
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
    st.markdown("These patterns are in the data — visible once it is loaded, cleaned, and structured through the Bronze, Silver, and Gold pipeline layers.")
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

    st.markdown("---")
    st.markdown(
        "The **SAS → PySpark Converter** shows how SAS analytics scripts can be migrated "
        "to build pipelines like this one."
    )
    _cv_col, _ = st.columns([2, 3])
    with _cv_col:
        if st.button("Open SAS → PySpark Converter →", key="converter_from_analytics", use_container_width=True):
            st.session_state["_nav_target"] = "SAS → PySpark Converter"
            st.rerun()


# ---------------------------------------------------------------------------
# SAS → PySpark Converter
# ---------------------------------------------------------------------------

elif page == "SAS → PySpark Converter":
    from converter.sas_to_pyspark import convert, convert_script
    from converter.manifest import generate_manifest
    from converter.migration_config import load_config_from_dict

    _bk, _ = st.columns([1, 5])
    with _bk:
        if st.button("← Home", key="home_from_converter", use_container_width=True):
            st.session_state["_nav_target"] = "Home"
            st.rerun()
    st.title("SAS → PySpark Converter")
    st.markdown(
        "Moving analytics from SAS to Databricks means translating every script into a different language. "
        "This converter does that in two ways. "
        "A rule engine matches known SAS patterns and produces a consistent translation. "
        "For patterns the rules cannot handle, it calls an LLM via API. "
        "In the live demo, examples run through the rule engine. "
        "Enterprises with an API key configured get both. "
        "LLM output should be reviewed for accuracy before use in production."
    )
    st.markdown(
        "<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:16px 0 8px 0;'>"
        "<div style='background:#F3F4F6;border-radius:6px;padding:7px 14px;font-size:0.84em;color:#374151;'>Architecture design</div>"
        "<span style='color:#9CA3AF;'>→</span>"
        "<div style='background:#F3F4F6;border-radius:6px;padding:7px 14px;font-size:0.84em;color:#374151;'>Data mapping</div>"
        "<span style='color:#9CA3AF;'>→</span>"
        "<div style='background:#EFF6FF;border:2px solid #3B82F6;border-radius:6px;padding:7px 14px;"
        "font-size:0.84em;color:#1E40AF;font-weight:600;'>Code translation · this tool</div>"
        "<span style='color:#9CA3AF;'>→</span>"
        "<div style='background:#F3F4F6;border-radius:6px;padding:7px 14px;font-size:0.84em;color:#374151;'>Review &amp; test</div>"
        "<span style='color:#9CA3AF;'>→</span>"
        "<div style='background:#F3F4F6;border-radius:6px;padding:7px 14px;font-size:0.84em;color:#374151;'>Deploy</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # Mode toggle — managed via _conv_mode so the button can switch it without a widget key conflict
    if "_conv_mode" not in st.session_state:
        st.session_state["_conv_mode"] = "Community"
    mode = st.radio(
        "Mode",
        ["Community", "Enterprise"],
        index=["Community", "Enterprise"].index(st.session_state["_conv_mode"]),
        horizontal=True,
    )
    st.session_state["_conv_mode"] = mode
    if mode == "Community":
        st.markdown("Single SAS operation — three preloaded examples, no setup needed.")
    else:
        st.markdown("Complete SAS file — four-block financial analytics example. The rule engine translates all four blocks; Block 4 contains a `RETAIN` statement that it cannot fully translate and flags for manual review. Config included.")

    st.markdown("---")

    EXAMPLES_COMMUNITY = {
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

    EXAMPLE_ENTERPRISE = (
        "/* Financial analytics migration — risklib → trading.bronze */\n\n"
        "PROC SORT DATA=risklib.sp500_prices OUT=sp500_sorted;\n"
        "    BY date;\n"
        "RUN;\n\n"
        "PROC MEANS DATA=risklib.fed_rate;\n"
        "    CLASS year_month;\n"
        "    VAR rate_value;\n"
        "RUN;\n\n"
        "DATA outlib.daily_analytics;\n"
        "    SET sp500_sorted;\n"
        "    WHERE date >= &start_date;\n"
        "    KEEP date close log_return;\n"
        "RUN;\n\n"
        "/* Cumulative return — RETAIN carries state row by row; requires LLM */\n"
        "DATA outlib.cumulative_returns;\n"
        "    SET outlib.daily_analytics;\n"
        "    RETAIN cumulative_return 0;\n"
        "    cumulative_return = cumulative_return + log_return;\n"
        "    OUTPUT;\n"
        "RUN;"
    )

    EXAMPLE_CONFIG = (
        "source:\n"
        "  library_mappings:\n"
        "    risklib: trading.bronze\n"
        "    outlib: trading.silver\n"
        "  dataset_mappings:\n"
        "    risklib.sp500_prices: trading.bronze.bronze_sp500\n"
        "    risklib.fed_rate: trading.bronze.bronze_fed_rate\n"
        "    outlib.daily_analytics: trading.silver.silver_market\n"
        "  macro_vars:\n"
        "    start_date: '2010-01-01'\n"
        "target:\n"
        "  platform: enterprise\n"
        "  catalog: trading\n"
        "  default_schema: silver\n"
        "  unity_catalog: false  # set true for Unity Catalog (full Databricks workspace only)\n"
    )

    api_key = _secret("ANTHROPIC_API_KEY")

    # ---------------------------------------------------------------------------
    # Community mode
    # ---------------------------------------------------------------------------

    if mode == "Community":
        st.markdown(
            "In SAS, analytics operations are written as **procedures** (PROCs) or **DATA steps**. "
            "Each is a self-contained block: a PROC sorts, aggregates, or queries data; "
            "a DATA step transforms it row by row. "
            "Three preloaded examples below — no API key needed."
        )
        st.info("Always validate converted code against the original SAS results before use in production.")

        _tab1, _tab2, _tab3, _tab4 = st.tabs(["PROC SORT", "PROC MEANS", "DATA step", "Where LLM helps"])

        with _tab1:
            st.markdown(
                "**Sort customers by name.** "
                "PROC SORT in SAS always maps to `.orderBy()` in PySpark. "
                "The rule engine handles this reliably and without variation."
            )
            sas_input_1 = st.text_area(
                "SAS code",
                value=EXAMPLES_COMMUNITY["PROC SORT: sort customers by name"],
                height=150,
                key="sas_c1",
            )
            btn_1 = st.button("Convert to PySpark →", type="primary", key="btn_c1", use_container_width=True)
            if btn_1 and sas_input_1.strip():
                with st.spinner("Converting..."):
                    result_1 = convert(sas_input_1, target="pyspark", api_key=api_key or None)
                st.text_area("Output", value=result_1.output, height=180, key="out_c1")
                if result_1.warnings:
                    st.warning("**Translation note:** " + " · ".join(result_1.warnings))
            elif btn_1:
                st.info("No SAS code to convert.")

        with _tab2:
            st.markdown(
                "**Regional sales summary.** "
                "PROC MEANS calculates summary statistics — mean, standard deviation, min, max — "
                "grouped by a category. It maps to `.groupBy().agg()` in PySpark, "
                "with each statistic mapped to its PySpark equivalent."
            )
            sas_input_2 = st.text_area(
                "SAS code",
                value=EXAMPLES_COMMUNITY["PROC MEANS: regional sales summary"],
                height=150,
                key="sas_c2",
            )
            btn_2 = st.button("Convert to PySpark →", type="primary", key="btn_c2", use_container_width=True)
            if btn_2 and sas_input_2.strip():
                with st.spinner("Converting..."):
                    result_2 = convert(sas_input_2, target="pyspark", api_key=api_key or None)
                st.text_area("Output", value=result_2.output, height=180, key="out_c2")
                if result_2.warnings:
                    st.warning("**Translation note:** " + " · ".join(result_2.warnings))
            elif btn_2:
                st.info("No SAS code to convert.")

        with _tab3:
            st.markdown(
                "**Customer tiering with conditional logic.** "
                "A DATA step applies business rules row by row across a dataset. "
                "In this example, IF-THEN-ELSE conditions become `F.when()` calls in PySpark, "
                "WHERE filtering becomes `.filter()`, and KEEP becomes `.select()`. "
                "This pattern is one of the most common in financial analytics."
            )
            sas_input_3 = st.text_area(
                "SAS code",
                value=EXAMPLES_COMMUNITY["DATA step: customer tiering with IF-THEN-ELSE"],
                height=190,
                key="sas_c3",
            )
            btn_3 = st.button("Convert to PySpark →", type="primary", key="btn_c3", use_container_width=True)
            if btn_3 and sas_input_3.strip():
                with st.spinner("Converting..."):
                    result_3 = convert(sas_input_3, target="pyspark", api_key=api_key or None)
                st.text_area("Output", value=result_3.output, height=200, key="out_c3")
                if result_3.warnings:
                    st.warning("**Translation note:** " + " · ".join(result_3.warnings))
            elif btn_3:
                st.info("No SAS code to convert.")

        with _tab4:
            st.markdown(
                "A running total adds each new value to the sum of all previous rows. "
                "In SAS, this is written with `RETAIN` — a keyword that keeps a variable's value "
                "as SAS moves from one row to the next. "
                "Running totals and cumulative portfolio returns are common examples."
            )
            st.markdown(
                "The rule engine translates patterns it recognises. "
                "`RETAIN` carries state across rows: each row's result depends on what the previous rows produced. "
                "The rule engine cannot reason about accumulated state, so it flags these cases for manual review "
                "rather than producing an incorrect translation. "
                "An LLM produces the correct PySpark equivalent using a window function, "
                "which calculates a value across a sequence of rows."
            )
            st.markdown("Below is what each produces for the same input.")
            _col_sas, _col_rule, _col_llm = st.columns(3)
            with _col_sas:
                st.markdown("**SAS input**")
                st.code(
                    "DATA cumulative;\n"
                    "    SET monthly_returns;\n"
                    "    RETAIN running_total 0;\n"
                    "    running_total =\n"
                    "        running_total + return;\n"
                    "    OUTPUT;\n"
                    "RUN;",
                    language="text",
                )
            with _col_rule:
                st.markdown(
                    "<span style='color:#92400E;font-weight:600;'>⚠ Rule engine — needs review</span>",
                    unsafe_allow_html=True,
                )
                st.code(
                    "# RETAIN statement detected —\n"
                    "# manual review required\n"
                    "cumulative_df = monthly_returns_df",
                    language="python",
                )
            with _col_llm:
                st.markdown(
                    "<span style='color:#166534;font-weight:600;'>✓ LLM output</span>",
                    unsafe_allow_html=True,
                )
                st.code(
                    "from pyspark.sql import Window\n"
                    "import pyspark.sql.functions as F\n\n"
                    "window = (\n"
                    "    Window\n"
                    "    .orderBy(F.monotonically_increasing_id())\n"
                    "    .rowsBetween(\n"
                    "        Window.unboundedPreceding,\n"
                    "        Window.currentRow))\n\n"
                    "cumulative_df = (\n"
                    "    monthly_returns_df\n"
                    "    .withColumn(\n"
                    "        'running_total',\n"
                    "        F.sum('return').over(window)))",
                    language="python",
                )
            st.markdown(
                "The rule engine handles predictable patterns reliably; "
                "the LLM handles what rules cannot."
            )

        st.markdown(
            "<div style='background:#EFF6FF;border-left:4px solid #3B82F6;border-radius:6px;"
            "padding:16px 20px;margin-top:20px;margin-bottom:20px;'>"
            "<div style='font-weight:600;color:#1E40AF;margin-bottom:8px;'>Using the output in Databricks</div>"
            "<ol style='margin:0;padding-left:20px;color:#374151;font-size:0.91em;line-height:1.8;'>"
            "<li>Copy the PySpark code from the output box</li>"
            "<li>Paste into a Databricks notebook — <code>spark</code> is available by default, no imports needed</li>"
            "<li>Confirm the source data is accessible in your Databricks environment</li>"
            "<li>Run the cell and check that the output matches the original SAS results</li>"
            "<li>In a migration, this code goes into the Databricks pipeline at the same stage "
            "the original SAS script occupied: data transformation, aggregation, or reporting</li>"
            "</ol></div>",
            unsafe_allow_html=True,
        )

        _ent_col, _ = st.columns([2, 3])
        with _ent_col:
            if st.button("Try Enterprise mode →", key="switch_enterprise", use_container_width=True):
                st.session_state["_conv_mode"] = "Enterprise"
                st.rerun()

    # ---------------------------------------------------------------------------
    # Enterprise mode
    # ---------------------------------------------------------------------------

    else:
        st.markdown(
            "A real SAS analytics file chains many steps in sequence — sort the data, calculate statistics, "
            "apply filters, apply business rules — all in one file, run top to bottom. "
            "Enterprise mode converts the whole file at once, one block at a time, and produces a single Python file."
        )
        st.markdown(
            "**The preloaded script** (visible in the editor below) is a four-block financial analytics file built on actual market data:"
        )
        st.markdown(
            "- **Block 1 — PROC SORT:** Sort S&P 500 price data by date\n"
            "- **Block 2 — PROC MEANS:** Monthly statistics on Federal Reserve rate data "
            "(mean, min, max per month)\n"
            "- **Block 3 — DATA step:** Filter the sorted S&P 500 data from 2010-01-01; "
            "keep only date, closing price, and daily return\n"
            "- **Block 4 — DATA step with RETAIN:** Calculate a running cumulative return. "
            "`RETAIN` carries a value from one row to the next — a pattern the rule engine cannot fully translate. "
            "Block 4 is flagged for review; the LLM window function equivalent appears directly below the rule engine output."
        )
        st.markdown(
            "Input: **Bronze layer** (raw market data). "
            "Output: **Silver layer** (filtered, structured data ready for analytics). "
            "The Analytics Dashboard shows the Gold layer metrics built from this Silver layer data."
        )
        st.markdown(
            "The converter produces **one Python file** with each block separated by a comment. "
            "In Databricks, paste each block into its own notebook cell and run them in order."
        )
        _db_col, _ = st.columns([2, 3])
        with _db_col:
            if st.button("Open Analytics Dashboard →", key="dash_from_converter", use_container_width=True):
                st.session_state["_nav_target"] = "Analytics Dashboard"
                st.rerun()
        st.markdown("---")
        st.markdown("**The config file** maps four things that SAS uses but Databricks does not recognise:")
        st.markdown(
            "- **Library shortcuts** — named pointers to data locations on the SAS server → Databricks catalog paths "
            "(e.g. `risklib` → `trading.bronze`)\n"
            "- **Dataset paths** — specific SAS dataset names → specific Databricks table paths\n"
            "- **Macro variable values** — placeholders like `&start_date` → actual values like `2010-01-01`\n"
            "- **Platform settings** — target catalog, default schema, Unity Catalog flag"
        )
        st.markdown("Written once, the config applies to every file in the migration.")
        st.markdown(
            "The config and script below are already loaded with the three-block example above. "
            "Click **Convert to PySpark →** to see all three blocks translated."
        )

        col_cfg, col_sas = st.columns(2)

        with col_cfg:
            st.markdown("**Migration config (YAML)**")
            config_text = st.text_area(
                "Config YAML",
                value=EXAMPLE_CONFIG,
                height=260,
                key="config_text_example",
            )

        with col_sas:
            st.markdown("**SAS script**")
            sas_input = st.text_area(
                "SAS code",
                value=EXAMPLE_ENTERPRISE,
                height=260,
                key="sas_input_enterprise",
            )

        st.markdown(
            "Each block gets a confidence score (green ≥ 85%, amber ≥ 70%, red < 70%). "
            "Block 4 will be flagged — the rule engine translates what it can and marks the `RETAIN` statement for manual review."
        )
        st.info("Always validate converted code against the original SAS results before use in production.")
        st.markdown("Converted output appears below the button, one section per block.")
        target = "pyspark"
        convert_btn = st.button("Convert to PySpark →", type="primary", use_container_width=True, key="convert_enterprise")

        if convert_btn and sas_input.strip():
            import yaml as _yaml

            config = None
            if config_text.strip():
                try:
                    raw_cfg = _yaml.safe_load(config_text)
                    config = load_config_from_dict(raw_cfg)
                except Exception as e:
                    st.error(f"Could not parse config YAML: {e}")
                    st.stop()

            with st.spinner("Converting..."):
                results = convert_script(sas_input, target=target, api_key=api_key or None, config=config)

            # Combined output for download
            all_output = "\n\n".join(
                f"# --- Block {i+1} ---\n{r.output}"
                for i, r in enumerate(results)
            )

            manifest_str = generate_manifest(
                results,
                source_label="enterprise_input.sas",
                platform="enterprise",
                config_applied=config is not None,
            )

            st.markdown("---")
            st.markdown("**Converted output** — one block per section, in the order they appear in the script.")

            # Per-block results
            for i, result in enumerate(results):
                conf = result.confidence
                if conf >= 0.85:
                    conf_color = "#22c55e"
                    conf_label = "High"
                elif conf >= 0.70:
                    conf_color = "#f59e0b"
                    conf_label = "Medium"
                else:
                    conf_color = "#ef4444"
                    conf_label = "Low"

                method_label = "Rule engine" if result.method == "rule_based" else "LLM"
                review_badge = (
                    " &nbsp;<span style='background:#fef2f2;color:#b91c1c;padding:1px 8px;"
                    "border-radius:4px;font-size:0.82em;border:1px solid #fca5a5;'>"
                    "Needs review</span>"
                    if result.review_required else ""
                )

                st.markdown(
                    f"**Block {i+1}** &nbsp;·&nbsp; {method_label} &nbsp;·&nbsp; "
                    f"Confidence: <span style='color:{conf_color};font-weight:600'>"
                    f"{conf:.0%} ({conf_label})</span>{review_badge}",
                    unsafe_allow_html=True,
                )
                st.text_area(f"Output — block {i+1}", value=result.output, height=180, key=f"out_{i}")

                if result.warnings:
                    st.warning(
                        "**Translation note:** "
                        + " · ".join(result.warnings)
                    )

                # For RETAIN blocks: show the LLM equivalent as a static example
                if any("RETAIN" in w for w in result.warnings):
                    st.markdown(
                        "Block 4 of the SAS script above contains `RETAIN cumulative_return 0;` — "
                        "a SAS keyword that carries a variable's value from one row to the next. "
                        "The rule engine flags this rather than produce an incorrect translation. "
                        "Below is the complete PySpark replacement an LLM produces: "
                        "a window function that replicates the same row-by-row accumulation."
                    )
                    st.markdown(
                        "<span style='color:#166534;font-weight:600;font-size:0.9em;'>"
                        "✓ LLM output — complete PySpark replacement (Block 4)</span>",
                        unsafe_allow_html=True,
                    )
                    st.code(
                        "# RETAIN → window function: cumulative sum over rows ordered by date\n"
                        "from pyspark.sql import Window\n"
                        "import pyspark.sql.functions as F\n\n"
                        "window = (\n"
                        "    Window\n"
                        "    .orderBy('date')\n"
                        "    .rowsBetween(Window.unboundedPreceding, Window.currentRow)\n"
                        ")\n\n"
                        "cumulative_returns_df = (\n"
                        "    daily_analytics_df\n"
                        "    .withColumn(\n"
                        "        'cumulative_return',\n"
                        "        F.sum('log_return').over(window))\n"
                        ")",
                        language="python",
                    )

                st.markdown("")

            # Download row — after reviewing the output
            st.markdown("**Download**")
            dl1, dl2, _ = st.columns([2, 2, 3])
            with dl1:
                st.download_button(
                    "Download converted code (.py)",
                    data=all_output,
                    file_name="converted.py",
                    mime="text/plain",
                )
            with dl2:
                st.download_button(
                    "Download review report (.yaml) — confidence scores and flags per block",
                    data=manifest_str,
                    file_name="migration_manifest.yaml",
                    mime="text/yaml",
                )

            st.markdown(
                "This dashboard converts one script at a time. "
                "For a large codebase, the same converter can be run programmatically across all files at once — "
                "one YAML config applies to every script, and each file produces a converted Python file and a review manifest. "
                "See the [project on GitHub](https://github.com/madhurima-nath/databricks-ai-automated-pipeline) for implementation details."
            )

        elif convert_btn:
            st.info("Paste some SAS code to convert.")

        st.markdown("---")
        st.markdown("**Full migration process**")
        st.markdown(
            """
            <div style="border:1px solid #E2E8F0;border-radius:8px;overflow:hidden;margin-bottom:16px;">
              <div style="display:flex;align-items:flex-start;gap:14px;padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #E2E8F0;">
                <span style="background:#1E3A5F;color:white;border-radius:50%;min-width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:0.82em;font-weight:700;flex-shrink:0;">1</span>
                <div><div style="font-weight:600;color:#1E3A5F;font-size:0.88em;">Design the target architecture</div><div style="color:#6B7280;font-size:0.82em;margin-top:2px;">Plan the pipeline layers — Bronze (raw data), Silver (transformed), Gold (reporting) — and decide which layer each SAS script's output feeds</div></div>
              </div>
              <div style="display:flex;align-items:flex-start;gap:14px;padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #E2E8F0;">
                <span style="background:#1E3A5F;color:white;border-radius:50%;min-width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:0.82em;font-weight:700;flex-shrink:0;">2</span>
                <div><div style="font-weight:600;color:#1E3A5F;font-size:0.88em;">Map the data</div><div style="color:#6B7280;font-size:0.82em;margin-top:2px;">Identify which SAS data sources correspond to which Databricks tables and catalog paths; record these mappings for the whole codebase</div></div>
              </div>
              <div style="display:flex;align-items:flex-start;gap:14px;padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #E2E8F0;">
                <span style="background:#1E3A5F;color:white;border-radius:50%;min-width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:0.82em;font-weight:700;flex-shrink:0;">3</span>
                <div><div style="font-weight:600;color:#1E3A5F;font-size:0.88em;">Write the config</div><div style="color:#6B7280;font-size:0.82em;margin-top:2px;">Map each SAS data reference — library shortcuts, dataset paths, macro variable values — to its Databricks equivalent; one config applies to every file</div></div>
              </div>
              <div style="display:flex;align-items:flex-start;gap:14px;padding:12px 16px;background:#EFF6FF;border-bottom:1px solid #BFDBFE;">
                <span style="background:#1E40AF;color:white;border-radius:50%;min-width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:0.82em;font-weight:700;flex-shrink:0;">4</span>
                <div><div style="font-weight:600;color:#1E40AF;font-size:0.88em;">Convert the scripts</div><div style="color:#6B7280;font-size:0.82em;margin-top:2px;">Run each SAS file through Enterprise mode: the rule engine handles common patterns, the LLM handles complex cases. For large codebases, run the converter programmatically across all files at once.</div></div>
              </div>
              <div style="display:flex;align-items:flex-start;gap:14px;padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #E2E8F0;">
                <span style="background:#1E3A5F;color:white;border-radius:50%;min-width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:0.82em;font-weight:700;flex-shrink:0;">5</span>
                <div><div style="font-weight:600;color:#1E3A5F;font-size:0.88em;">Review the output</div><div style="color:#6B7280;font-size:0.82em;margin-top:2px;">Download the migration manifest. Each block has a confidence score; flagged blocks and any LLM translations need a manual check before proceeding.</div></div>
              </div>
              <div style="display:flex;align-items:flex-start;gap:14px;padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #E2E8F0;">
                <span style="background:#1E3A5F;color:white;border-radius:50%;min-width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:0.82em;font-weight:700;flex-shrink:0;">6</span>
                <div><div style="font-weight:600;color:#1E3A5F;font-size:0.88em;">Test in Databricks</div><div style="color:#6B7280;font-size:0.82em;margin-top:2px;">Paste each converted block into a Databricks notebook. <code>spark</code> is available by default, no imports needed. Run the cell.</div></div>
              </div>
              <div style="display:flex;align-items:flex-start;gap:14px;padding:12px 16px;background:#F0FDF4;">
                <span style="background:#166534;color:white;border-radius:50%;min-width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:0.82em;font-weight:700;flex-shrink:0;">7</span>
                <div><div style="font-weight:600;color:#166534;font-size:0.88em;">Validate and deploy</div><div style="color:#6B7280;font-size:0.82em;margin-top:2px;">Confirm the output matches the original SAS results, then deploy to production</div></div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


