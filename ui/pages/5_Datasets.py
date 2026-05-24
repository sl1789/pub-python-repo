"""Datasets page: browse the parquet exports each Databricks notebook writes.

Each notebook drops its primary output to ADLS as parquet under a stable
`<container>/<prefix>/<dataset>/[ticker=<TICKER>/]` layout. The backend
`GET /datasets/{name}` endpoint reads those folders; this page is the UI in
front of it.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from api_client import (
    get_dataset,
    get_token_or_stop,
    list_datasets,
    render_session_sidebar,
)

st.set_page_config(page_title="Datasets - MC Orchestrator", layout="wide")
st.title("Datasets")
st.caption(
    "Browse the parquet snapshots produced by the Databricks notebooks. "
    "Ticker-partitioned datasets require a ticker; flat datasets are a "
    "single snapshot per benchmark run."
)

token = get_token_or_stop()
render_session_sidebar()


# ---------------------------------------------------------------------------
# Dataset picker
# ---------------------------------------------------------------------------
try:
    catalog = list_datasets(token)
except Exception as e:
    st.error(f"Failed to load dataset catalog: {e}")
    st.stop()

items = catalog.get("items", [])
if not items:
    st.warning("The backend reports no datasets are configured.")
    st.stop()

# Map name -> spec so we can drive the ticker input off the picked dataset.
specs_by_name = {it["name"]: it for it in items}
names = list(specs_by_name.keys())

col_ds, col_tk = st.columns([2, 1])
with col_ds:
    chosen_name = st.selectbox(
        "Dataset",
        options=names,
        index=0,
        format_func=lambda n: f"{n} — {specs_by_name[n]['description']}",
    )
spec = specs_by_name[chosen_name]

# Common-ticker hint for the partitioned datasets; the user can still type
# anything that matches `^[A-Za-z0-9.\-^=]{1,16}$` server-side.
COMMON_TICKERS = ["^GSPC", "SPY", "AAPL", "MSFT", "GOOGL", "AMZN", "META"]

ticker: str | None = None
with col_tk:
    if spec["ticker_partitioned"]:
        ticker = st.selectbox(
            "Ticker",
            options=COMMON_TICKERS,
            index=0,
            help="The dataset is partitioned by ticker; pick one to load.",
        )
        custom = st.text_input(
            "...or custom ticker",
            value="",
            placeholder="e.g. NVDA",
            help="Overrides the dropdown when non-empty.",
        )
        if custom.strip():
            ticker = custom.strip()
    else:
        st.caption("Flat dataset (no ticker partition).")


load_clicked = st.button("Load dataset", type="primary")
if not load_clicked:
    st.stop()


# ---------------------------------------------------------------------------
# Fetch + render
# ---------------------------------------------------------------------------
with st.spinner("Loading parquet..."):
    try:
        payload = get_dataset(token, chosen_name, ticker)
    except Exception as e:
        st.error(str(e))
        st.stop()

rows = payload.get("rows", [])
if not rows:
    st.warning(f"Dataset `{chosen_name}` returned 0 rows.")
    st.stop()

df = pd.DataFrame(rows)
st.success(f"Loaded {len(df):,} rows from `{chosen_name}`" + (f" (ticker={ticker})" if ticker else ""))
st.dataframe(df, use_container_width=True, height=400)

st.download_button(
    "Download as CSV",
    df.to_csv(index=False).encode("utf-8"),
    file_name=f"{chosen_name}{('_' + ticker) if ticker else ''}.csv",
    mime="text/csv",
)


# ---------------------------------------------------------------------------
# Per-dataset chart. Each branch is best-effort: it picks the columns the
# notebook is known to emit, and silently skips when they aren't there
# (e.g. if a future schema change adds/removes fields).
# ---------------------------------------------------------------------------
st.subheader("Visualization")

try:
    if chosen_name == "mc_vs_actual" and {"actual_call", "mc_call", "method"}.issubset(df.columns):
        plot_df = df[df["actual_call"].notna() & (df["actual_call"] > 0.5)]
        if not plot_df.empty:
            fig = px.scatter(
                plot_df,
                x="actual_call",
                y="mc_call",
                color="method",
                hover_data=[c for c in ("K", "T", "num_runs") if c in plot_df.columns],
                title=f"MC vs Actual call prices ({ticker})",
            )
            max_v = float(max(plot_df["actual_call"].max(), plot_df["mc_call"].max()))
            fig.add_shape(
                type="line", x0=0, y0=0, x1=max_v, y1=max_v,
                line=dict(dash="dash", color="black"),
            )
            st.plotly_chart(fig, use_container_width=True)

    elif chosen_name == "scalability" and {"runs", "CallPrice", "method", "alt_weight"}.issubset(df.columns):
        fig = px.line(
            df.sort_values("runs"),
            x="runs",
            y="CallPrice",
            color="method",
            line_dash="alt_weight",
            log_x=True,
            markers=True,
            title="Scalability: Call price convergence vs # runs",
        )
        st.plotly_chart(fig, use_container_width=True)

    elif chosen_name == "options" and {"strike", "open_interest", "option_type"}.issubset(df.columns):
        fig = px.bar(
            df,
            x="strike",
            y="open_interest",
            color="option_type",
            facet_row="expiration_date" if "expiration_date" in df.columns else None,
            title=f"Open interest by strike ({ticker})",
        )
        st.plotly_chart(fig, use_container_width=True)

    elif chosen_name == "block_length_sweep" and {"block_mean_len", "method"}.issubset(df.columns):
        y_col = "put_mape" if "put_mape" in df.columns else df.select_dtypes("number").columns[-1]
        fig = px.line(
            df.sort_values("block_mean_len"),
            x="block_mean_len",
            y=y_col,
            color="method" if "method" in df.columns else None,
            markers=True,
            title=f"Block-length sweep ({ticker})",
        )
        st.plotly_chart(fig, use_container_width=True)

    elif chosen_name == "lam_sweep" and "lam" in df.columns:
        y_col = "put_mape" if "put_mape" in df.columns else df.select_dtypes("number").columns[-1]
        fig = px.line(
            df.sort_values("lam"),
            x="lam",
            y=y_col,
            markers=True,
            title=f"Lam sweep ({ticker})",
        )
        st.plotly_chart(fig, use_container_width=True)

    elif chosen_name == "emc_diagnostics" and {"method", "mc_call", "actual_call"}.issubset(df.columns):
        # EMC notebook emits paired pre/post rows; if there's an `emc_applied`
        # flag we colour by it, otherwise fall back to method.
        color_col = "emc_applied" if "emc_applied" in df.columns else "method"
        fig = px.scatter(
            df,
            x="actual_call",
            y="mc_call",
            color=color_col,
            title=f"EMC pre/post diagnostics ({ticker})",
        )
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No dataset-specific chart available; showing the raw table only.")
except Exception as e:
    st.warning(f"Chart rendering failed: {e}")
