"""Analysis page: visualise the research outputs of `mc_vs_actual_test.ipynb`.

This page is **read-only**: the underlying Databricks notebook is run
manually in Databricks (it does a sweep over tickers/strikes/scales that
takes minutes, and is not parameterised the way the MC job is). The
notebook writes its four research DataFrames to parquet on ADLS, and
this page renders the panels that the notebook itself draws in
matplotlib — but interactive, ticker-scoped, and with a "data as of …"
freshness badge.

Tabs:
  1. **MC vs Actual**     -> scatter (calls + puts), MAPE bars, MAPE-vs-#runs
  2. **EMC pre/post**     -> server-side join of `mc_vs_actual` x `emc_diagnostics`
  3. **Block-length sweep** -> MAPE vs block_mean_len
  4. **lam sweep**        -> MAPE vs lam
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from api_client import (
    get_dataset,
    get_emc_join,
    get_token_or_stop,
    render_session_sidebar,
)

st.set_page_config(page_title="Analysis - MC Orchestrator", layout="wide")
st.title("Analysis")
st.caption(
    "Research outputs from `mc_vs_actual_test.ipynb`. "
    "This page is read-only — the notebook is run manually in Databricks "
    "(it sweeps the whole option universe and is not submitted from here)."
)

token = get_token_or_stop()
render_session_sidebar()


# ---------------------------------------------------------------------------
# Header controls (ticker scoped, applies to every tab)
# ---------------------------------------------------------------------------
COMMON_TICKERS = ["SPY", "AAPL", "MSFT", "GOOGL", "AMZN", "META"]

c1, c2 = st.columns([1, 3])
with c1:
    ticker = st.selectbox(
        "Ticker",
        options=COMMON_TICKERS,
        index=0,
        help="All four research datasets are partitioned by ticker; "
             "pick one to scope every tab below.",
    )
with c2:
    custom = st.text_input(
        "...or custom ticker",
        value="",
        placeholder="Overrides the dropdown when non-empty (e.g. NVDA)",
    )
if custom.strip():
    ticker = custom.strip()


# ---------------------------------------------------------------------------
# Cached fetchers (per token+ticker; Streamlit reuses across reruns)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=300)
def _fetch_dataset(token: str, name: str, ticker: str) -> dict:
    return get_dataset(token, name, ticker)


@st.cache_data(show_spinner=False, ttl=300)
def _fetch_emc_join(token: str, ticker: str) -> dict:
    return get_emc_join(token, ticker)


def _freshness(payload: dict) -> str:
    """Format the dataset's `ds_updated_at` for display, with a UTC suffix."""
    ts = payload.get("ds_updated_at") if isinstance(payload, dict) else None
    if not ts:
        return "Data freshness unknown (notebook may pre-date `created_at` stamping)."
    return f"Data as of {ts} UTC"


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_mva, tab_emc, tab_block, tab_lam = st.tabs(
    ["MC vs Actual", "EMC pre/post", "Block-length sweep", "lam sweep"]
)


# === Tab 1: MC vs Actual ===================================================
with tab_mva:
    try:
        payload = _fetch_dataset(token, "mc_vs_actual", ticker)
    except Exception as e:
        st.error(f"Failed to load `mc_vs_actual` for ticker={ticker}: {e}")
        st.stop()

    rows = payload.get("rows", [])
    if not rows:
        st.warning(f"`mc_vs_actual` returned 0 rows for ticker={ticker}.")
    else:
        df = pd.DataFrame(rows)
        st.caption(_freshness(payload))

        scales = sorted(df["num_runs"].unique())
        cc1, cc2 = st.columns([2, 1])
        with cc1:
            scale = st.select_slider(
                "Scale (num_runs) for scatter / MAPE panels",
                options=scales,
                value=scales[-1],
            )
        with cc2:
            atm_only = st.toggle(
                "ATM only (|K/S0 - 1| <= 5%)",
                value=False,
                help="Restrict to at-the-money strikes; matches the ATM panel in the notebook.",
            )

        scale_df = df[df["num_runs"] == scale].copy()
        if atm_only and {"K", "S0"}.issubset(scale_df.columns):
            scale_df["moneyness"] = (scale_df["K"] / scale_df["S0"] - 1).abs()
            scale_df = scale_df[scale_df["moneyness"] <= 0.05]

        # --- Panel 1: Call scatter -----------------------------------------
        call_df = scale_df[scale_df["actual_call"].notna() & (scale_df["actual_call"] > 0.5)]
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Calls: MC vs Actual")
            if call_df.empty:
                st.info("No call observations at this scale.")
            else:
                fig = px.scatter(
                    call_df,
                    x="actual_call",
                    y="mc_call",
                    color="method",
                    hover_data=["K", "T"],
                    title=f"{ticker} @ {scale:,} runs",
                )
                m = float(max(call_df["actual_call"].max(), call_df["mc_call"].max()))
                fig.add_shape(type="line", x0=0, y0=0, x1=m, y1=m,
                              line=dict(dash="dash", color="black"))
                fig.update_layout(height=420)
                st.plotly_chart(fig, use_container_width=True)

        # --- Panel 2: Put scatter ------------------------------------------
        put_df = scale_df[scale_df["actual_put"].notna() & (scale_df["actual_put"] > 0.5)]
        with col_b:
            st.subheader("Puts: MC vs Actual")
            if put_df.empty:
                st.info("No put observations at this scale.")
            else:
                fig = px.scatter(
                    put_df,
                    x="actual_put",
                    y="mc_put",
                    color="method",
                    hover_data=["K", "T"],
                    title=f"{ticker} @ {scale:,} runs",
                )
                m = float(max(put_df["actual_put"].max(), put_df["mc_put"].max()))
                fig.add_shape(type="line", x0=0, y0=0, x1=m, y1=m,
                              line=dict(dash="dash", color="black"))
                fig.update_layout(height=420)
                st.plotly_chart(fig, use_container_width=True)

        # --- Panel 3: MAPE-by-method bar -----------------------------------
        st.subheader("Call price MAPE by method")
        if call_df.empty:
            st.info("No call observations at this scale.")
        else:
            mape_df = call_df.copy()
            mape_df["abs_pct_err"] = (
                (mape_df["mc_call"] - mape_df["actual_call"]).abs()
                / mape_df["actual_call"] * 100
            )
            agg = mape_df.groupby("method")["abs_pct_err"].mean().sort_values()
            fig = px.bar(
                agg.reset_index(),
                x="abs_pct_err",
                y="method",
                orientation="h",
                text=agg.round(2).astype(str) + "%",
                labels={"abs_pct_err": "MAPE (%)", "method": ""},
            )
            fig.update_layout(height=360, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # --- Panel 4: Error convergence by scale (across all scales) ------
        st.subheader("Error convergence: MAPE vs # runs")
        # Use the full df (all scales), not scale_df.
        conv_df = df[df["actual_call"].notna() & (df["actual_call"] > 0.5)].copy()
        if atm_only and {"K", "S0"}.issubset(conv_df.columns):
            conv_df["moneyness"] = (conv_df["K"] / conv_df["S0"] - 1).abs()
            conv_df = conv_df[conv_df["moneyness"] <= 0.05]
        if conv_df.empty:
            st.info("No data for convergence panel.")
        else:
            conv_df["abs_pct_err"] = (
                (conv_df["mc_call"] - conv_df["actual_call"]).abs()
                / conv_df["actual_call"] * 100
            )
            scale_mape = (
                conv_df.groupby(["method", "num_runs"])["abs_pct_err"]
                .mean()
                .reset_index()
            )
            fig = px.line(
                scale_mape,
                x="num_runs",
                y="abs_pct_err",
                color="method",
                log_x=True,
                markers=True,
                labels={"num_runs": "Number of MC runs (log)", "abs_pct_err": "MAPE (%)"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("Raw rows"):
            st.dataframe(df, use_container_width=True, height=300)


# === Tab 2: EMC pre/post ===================================================
with tab_emc:
    st.markdown(
        "Compares pre-correction MAPE (from `mc_vs_actual` at the EMC scale) "
        "against post-correction MAPE (from `emc_diagnostics`) per method. "
        "A negative `delta` means EMC reduced the error."
    )
    try:
        payload = _fetch_emc_join(token, ticker)
    except Exception as e:
        st.error(f"Failed to load EMC join for ticker={ticker}: {e}")
        st.stop()

    st.caption(_freshness(payload) + f" — joined at num_runs={payload.get('num_runs')}")
    rows = payload.get("rows", [])
    if not rows:
        st.warning(
            f"No EMC pre/post pairs available for ticker={ticker}. "
            "Re-run `mc_vs_actual_test.ipynb` to populate both datasets."
        )
    else:
        ej_df = pd.DataFrame(rows)
        col_c, col_p = st.columns(2)

        for col, side, title in (
            (col_c, "call", "Call MAPE: pre vs post EMC"),
            (col_p, "put", "Put MAPE: pre vs post EMC"),
        ):
            side_df = ej_df[ej_df["side"] == side]
            with col:
                st.subheader(title)
                if side_df.empty:
                    st.info(f"No {side} pre/post pairs.")
                    continue
                long = side_df.melt(
                    id_vars="method",
                    value_vars=["pre_mape", "post_mape"],
                    var_name="phase",
                    value_name="mape",
                )
                long["phase"] = long["phase"].map({"pre_mape": "pre-EMC", "post_mape": "post-EMC"})
                fig = px.bar(
                    long,
                    x="method",
                    y="mape",
                    color="phase",
                    barmode="group",
                    text=long["mape"].round(2).astype(str),
                    labels={"mape": "MAPE (%)", "method": ""},
                    color_discrete_map={"pre-EMC": "#e67e22", "post-EMC": "#2ecc71"},
                )
                fig.update_layout(height=400, legend_title_text="")
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Delta table")
        st.dataframe(
            ej_df.sort_values(["side", "method"]),
            use_container_width=True,
            height=280,
        )


# === Tab 3: Block-length sweep ============================================
with tab_block:
    try:
        payload = _fetch_dataset(token, "block_length_sweep", ticker)
    except Exception as e:
        st.error(f"Failed to load `block_length_sweep` for ticker={ticker}: {e}")
        st.stop()

    rows = payload.get("rows", [])
    if not rows:
        st.warning(f"`block_length_sweep` returned 0 rows for ticker={ticker}.")
    else:
        df = pd.DataFrame(rows)
        st.caption(_freshness(payload))

        # Mean MAPE per (block_mean_len, side). The notebook uses calls when
        # available and falls back to puts, encoded in the `side` column.
        agg = (
            df.groupby(["block_mean_len", "side"])["abs_pct_err"]
            .mean()
            .reset_index()
            .sort_values("block_mean_len")
        )
        fig = px.line(
            agg,
            x="block_mean_len",
            y="abs_pct_err",
            color="side",
            markers=True,
            labels={"block_mean_len": "Mean block length (days)", "abs_pct_err": "MAPE (%)"},
            title=f"Stationary bootstrap: MAPE vs mean block length ({ticker})",
        )
        # Annotate the argmin per side.
        for side, side_df in agg.groupby("side"):
            best = side_df.loc[side_df["abs_pct_err"].idxmin()]
            fig.add_vline(
                x=float(best["block_mean_len"]),
                line_dash="dash",
                line_color="#2ecc71",
                annotation_text=f"best {side}: L={int(best['block_mean_len'])} ({best['abs_pct_err']:.1f}%)",
                annotation_position="top right",
            )
        fig.update_layout(height=460)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Raw rows"):
            st.dataframe(df, use_container_width=True, height=300)


# === Tab 4: lam sweep =====================================================
with tab_lam:
    try:
        payload = _fetch_dataset(token, "lam_sweep", ticker)
    except Exception as e:
        st.error(f"Failed to load `lam_sweep` for ticker={ticker}: {e}")
        st.stop()

    rows = payload.get("rows", [])
    if not rows:
        st.warning(f"`lam_sweep` returned 0 rows for ticker={ticker}.")
    else:
        df = pd.DataFrame(rows)
        st.caption(_freshness(payload))

        agg = (
            df.groupby(["lam", "side"])["abs_pct_err"]
            .mean()
            .reset_index()
            .sort_values("lam")
        )
        fig = px.line(
            agg,
            x="lam",
            y="abs_pct_err",
            color="side",
            markers=True,
            labels={"lam": "lam (intermittency)", "abs_pct_err": "MAPE (%)"},
            title=f"Multifractal: MAPE vs intermittency parameter ({ticker})",
        )
        for side, side_df in agg.groupby("side"):
            best = side_df.loc[side_df["abs_pct_err"].idxmin()]
            fig.add_vline(
                x=float(best["lam"]),
                line_dash="dash",
                line_color="#8e44ad",
                annotation_text=f"best {side}: lam={best['lam']:.2f} ({best['abs_pct_err']:.1f}%)",
                annotation_position="top right",
            )
        fig.update_layout(height=460)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Raw rows"):
            st.dataframe(df, use_container_width=True, height=300)
