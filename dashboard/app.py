"""
Streamlit Dashboard — Cross-Market Macro Analysis Platform
===========================================================
Two pages:
  1. Market Analysis — S&P 500 and Euro Stoxx 50 vs central bank rates, volatility, regimes
  2. SAS → PySpark Converter — paste legacy SAS code, get PySpark or Databricks SQL output

Run locally:
    streamlit run dashboard/app.py

On Databricks Apps:
    Point the app config to this file and set ANTHROPIC_API_KEY in the secrets store.
"""

import os
import sys
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
    ["Market Analysis", "SAS → PySpark Converter"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Data sources:** S&P 500, Euro Stoxx 50, VIX (Yahoo Finance) · "
    "US Fed Funds Rate, ECB Deposit Facility Rate (FRED)"
)

# ---------------------------------------------------------------------------
# Page 1: Market Analysis
# ---------------------------------------------------------------------------

if page == "Market Analysis":
    st.title("Cross-Market Macro Analysis")
    st.caption(
        "US and EU equity markets alongside central bank policy rates — "
        "2010 to present. Data served from the Gold Delta table."
    )

    # --- Load data ---
    @st.cache_data(ttl=3600)
    def load_gold():
        try:
            import pandas as pd
            import subprocess, json
            # Try to load from a local parquet export if Databricks isn't available
            parquet_path = os.path.join(os.path.dirname(__file__), "..", "data", "gold_analytics.parquet")
            if os.path.exists(parquet_path):
                return pd.read_parquet(parquet_path)
            st.warning("Gold table not found locally. Run the pipeline on Databricks first, then export the table.")
            return None
        except Exception as e:
            st.error(f"Could not load data: {e}")
            return None

    df = load_gold()

    if df is None:
        st.info(
            "No data loaded. Run notebooks 01 → 02 → 03 on Databricks, "
            "then export `gold_analytics` as parquet to `data/gold_analytics.parquet`."
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
        start = st.date_input("From", value=pd.to_datetime("2018-01-01").date(), min_value=min_date, max_value=max_date)
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
    m1.metric("S&P 500", f"{latest['sp500_close']:,.0f}")
    m2.metric("Euro Stoxx 50", f"{latest['eurostoxx_close']:,.0f}")
    m3.metric("VIX", f"{latest['vix_close']:.1f}", help="Market stress: <20 calm, 20–30 elevated, >30 stress")
    m4.metric("US Fed Rate", f"{latest['fed_rate']:.2f}%")
    m5.metric("ECB Rate", f"{latest['ecb_rate']:.2f}%")

    st.markdown("---")

    # --- Chart 1: Equity indices ---
    st.subheader("Equity Indices")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=d["date"], y=d["sp500_close"],    name="S&P 500",       line=dict(color="#1f77b4")), secondary_y=False)
    fig.add_trace(go.Scatter(x=d["date"], y=d["eurostoxx_close"],name="Euro Stoxx 50",  line=dict(color="#ff7f0e")), secondary_y=True)
    fig.update_layout(height=350, margin=dict(t=10, b=10), legend=dict(orientation="h", y=1.05))
    fig.update_yaxes(title_text="S&P 500",      secondary_y=False)
    fig.update_yaxes(title_text="Euro Stoxx 50", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # --- Chart 2: Central bank rates + policy divergence ---
    st.subheader("Central Bank Policy Rates")
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=d["date"], y=d["fed_rate"], name="US Fed Funds Rate", line=dict(color="#1f77b4")))
    fig2.add_trace(go.Scatter(x=d["date"], y=d["ecb_rate"], name="ECB Deposit Rate",  line=dict(color="#ff7f0e")))
    fig2.add_trace(go.Scatter(x=d["date"], y=d["policy_rate_diff"], name="Differential (US − ECB)",
                              line=dict(color="#2ca02c", dash="dot")))
    fig2.update_layout(height=300, margin=dict(t=10, b=10),
                       yaxis_title="Rate (%)", legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig2, use_container_width=True)

    # --- Chart 3: Volatility ---
    st.subheader("Realised Volatility (annualised)")
    col_a, col_b = st.columns(2)
    with col_a:
        fig3a = go.Figure()
        fig3a.add_trace(go.Scatter(x=d["date"], y=d["sp500_vol_20d"],  name="20-day vol", line=dict(color="#1f77b4")))
        fig3a.add_trace(go.Scatter(x=d["date"], y=d["sp500_vol_60d"],  name="60-day vol", line=dict(color="#aec7e8")))
        fig3a.add_trace(go.Scatter(x=d["date"], y=d["vix_close"] / 100, name="VIX / 100", line=dict(color="#d62728", dash="dot")))
        fig3a.update_layout(title="S&P 500", height=280, margin=dict(t=30, b=10))
        st.plotly_chart(fig3a, use_container_width=True)
    with col_b:
        fig3b = go.Figure()
        fig3b.add_trace(go.Scatter(x=d["date"], y=d["eurostoxx_vol_20d"], name="20-day vol", line=dict(color="#ff7f0e")))
        fig3b.add_trace(go.Scatter(x=d["date"], y=d["eurostoxx_vol_60d"], name="60-day vol", line=dict(color="#ffbb78")))
        fig3b.update_layout(title="Euro Stoxx 50", height=280, margin=dict(t=30, b=10))
        st.plotly_chart(fig3b, use_container_width=True)

    # --- Chart 4: US-EU equity correlation ---
    st.subheader("60-Day Rolling Correlation: S&P 500 vs Euro Stoxx 50")
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=d["date"], y=d["us_eu_equity_corr_60d"], name="Return correlation",
                              line=dict(color="#9467bd"), fill="tozeroy", fillcolor="rgba(148,103,189,0.1)"))
    fig4.add_hline(y=0, line_dash="dash", line_color="grey")
    fig4.update_layout(height=260, margin=dict(t=10, b=10), yaxis_title="Correlation", yaxis_range=[-1, 1])
    st.plotly_chart(fig4, use_container_width=True)

    # --- Chart 5: S&P 500 drawdown ---
    st.subheader("S&P 500 Drawdown from 52-Week High")
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=d["date"], y=d["sp500_drawdown_52w"], name="Drawdown %",
                              line=dict(color="#d62728"), fill="tozeroy", fillcolor="rgba(214,39,40,0.15)"))
    fig5.update_layout(height=250, margin=dict(t=10, b=10), yaxis_title="Drawdown (%)")
    st.plotly_chart(fig5, use_container_width=True)

    # --- Regime table ---
    st.subheader("Current Regimes")
    regime_cols = ["us_rate_regime", "eu_rate_regime", "policy_divergence", "vix_regime"]
    st.dataframe(
        d[regime_cols + ["date"]].tail(1).set_index("date").T.rename(columns=lambda c: str(c.date())),
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Page 2: SAS → PySpark Converter
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
                 "Set ANTHROPIC_API_KEY in your environment to avoid pasting it here.",
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
                    st.markdown(f"- ⚠️ {w}")

            st.caption(f"Method: `{result.method}` · Target: `{result.target}`")

        elif convert_btn:
            st.info("Paste some SAS code on the left to convert.")

    # --- Example patterns ---
    with st.expander("Example SAS patterns and their equivalents"):
        st.markdown("""
| SAS construct | PySpark equivalent |
|---|---|
| `PROC SORT DATA=x; BY col; RUN;` | `x_df.orderBy("col")` |
| `PROC MEANS DATA=x; CLASS g; VAR v; RUN;` | `x_df.groupBy("g").agg(F.mean("v"))` |
| `PROC SQL; SELECT ... QUIT;` | `spark.sql(\"SELECT ...\")` |
| `DATA out; SET in; KEEP a b; RUN;` | `in_df.select("a", "b")` |
| `IF x > 0 THEN y = 1; ELSE y = 0;` | `.withColumn("y", F.when(x > 0, 1).otherwise(0))` |
| `RETAIN running_total;` | `F.last(..., True).over(Window.unboundedPreceding...)` |
| `WHERE date = TODAY();` | `.filter(F.col("date") == F.current_date())` |
| `RENAME old=new;` | `.withColumnRenamed("old", "new")` |
        """)
