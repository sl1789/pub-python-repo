"""Results page: chart and inspect a finished job's prices across methods."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from api_client import get_job, get_results, get_token_or_stop, render_session_sidebar

st.set_page_config(page_title="Results - MC Orchestrator", layout="wide")
st.title("Results")
st.caption(
    "Compare Call/Put prices across simulation methods for a single job. "
    "Only `SUCCEEDED` jobs return results."
)

token = get_token_or_stop()


# Most commonly-inspected subset; the multiselect below lets users widen this.
DEFAULT_METHODS = ["black_scholes", "fhs_rn", "multifractal", "window"]
ALL_KNOWN_METHODS = [
    "historical", "window", "window_10d", "window_20d",
    "student_t", "black_scholes",
    "multifractal", "multifractal_empirical",
    "block_bootstrap", "fhs", "fhs_rn", "analogue",
]


# ---------------------------------------------------------------------------
# Job picker
# ---------------------------------------------------------------------------
preselected = st.session_state.get("results_job_id") or st.session_state.get("last_job_id")
col_id, col_btn = st.columns([3, 1])
with col_id:
    job_id = st.number_input(
        "Job ID",
        min_value=1,
        value=int(preselected) if preselected else 1,
        step=1,
    )
with col_btn:
    refresh = st.button("Load", use_container_width=True)

if not (preselected or refresh):
    st.info("Enter a job ID and click **Load**, or open from the **Jobs** page.")
    st.stop()

st.session_state["results_job_id"] = int(job_id)


# ---------------------------------------------------------------------------
# Confirm job is ready
# ---------------------------------------------------------------------------
try:
    job = get_job(token, int(job_id))
except Exception as e:
    st.error(f"Failed to fetch job: {e}")
    st.stop()

status = (job.get("status") or "").upper()
st.markdown(f"**Status:** `{status}`  ·  **Runner:** `{job.get('runner')}`")

if status != "SUCCEEDED":
    st.warning(
        f"Job is not finished yet (status=`{status}`). "
        "Results will appear automatically once it succeeds."
    )
    # Cheap poll loop: only when actually pending.
    if status in {"QUEUED", "RUNNING", "PENDING"}:
        import time
        with st.spinner("Waiting for job to finish..."):
            time.sleep(3)
        st.rerun()
    st.stop()


# ---------------------------------------------------------------------------
# Pull results
# ---------------------------------------------------------------------------
try:
    res = get_results(token, int(job_id))
except Exception as e:
    st.error(f"Failed to load results: {e}")
    st.stop()

rows = res.get("rows") or []
if not rows:
    st.info("Job finished but no rows were returned for this job.")
    st.stop()

df = pd.DataFrame(rows)
if "method" not in df.columns:
    st.warning("Results don't include a `method` column; showing raw rows.")
    st.dataframe(df, use_container_width=True)
    st.stop()


# ---------------------------------------------------------------------------
# Method filter
# ---------------------------------------------------------------------------
present_methods = sorted(df["method"].dropna().unique().tolist())
default_selection = [m for m in DEFAULT_METHODS if m in present_methods] or present_methods

selected = st.multiselect(
    "Methods to display",
    options=present_methods,
    default=default_selection,
    help="Filter the comparison to a subset of the methods that ran for this job.",
)
if not selected:
    st.info("Pick at least one method.")
    st.stop()

view = df[df["method"].isin(selected)].copy()


# ---------------------------------------------------------------------------
# Summary chart: per-method Call/Put bars with the Black-Scholes price drawn
# as a horizontal reference line. The BS row in the dataframe *is* the
# closed-form analytical price for this (S0, K, T, sigma), so we reuse it
# instead of recomputing analytics in the UI.
# ---------------------------------------------------------------------------
if {"CallPrice", "PutPrice"}.issubset(view.columns):
    import plotly.graph_objects as go

    plot_df = (
        view.groupby("method", as_index=False)[["CallPrice", "PutPrice"]]
        .mean()  # collapse if multiple (K,T) rows exist per method
        .sort_values("method")
    )

    bs_row = df[df["method"] == "black_scholes"]
    bs_call = float(bs_row["CallPrice"].mean()) if not bs_row.empty else None
    bs_put = float(bs_row["PutPrice"].mean()) if not bs_row.empty else None

    st.subheader("Call / Put price by method")
    st.caption(
        "Bars: Monte Carlo estimate per method. Dashed lines: closed-form "
        "Black-Scholes price (when available) as a reference."
    )

    fig = go.Figure()
    fig.add_bar(
        name="Call", x=plot_df["method"], y=plot_df["CallPrice"],
        marker_color="#4f8bf9",
    )
    fig.add_bar(
        name="Put", x=plot_df["method"], y=plot_df["PutPrice"],
        marker_color="#f97c4f",
    )
    if bs_call is not None:
        fig.add_hline(
            y=bs_call, line_dash="dash", line_color="#4f8bf9",
            annotation_text=f"BS call ${bs_call:.2f}",
            annotation_position="top left",
        )
    if bs_put is not None:
        fig.add_hline(
            y=bs_put, line_dash="dash", line_color="#f97c4f",
            annotation_text=f"BS put ${bs_put:.2f}",
            annotation_position="bottom left",
        )
    fig.update_layout(
        barmode="group",
        height=420,
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        yaxis_title="Option price ($)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Quick scan: how far is each method from the BS analytical reference?
    if bs_call is not None and bs_put is not None:
        diag = plot_df.copy()
        diag["CallΔ%"] = (diag["CallPrice"] - bs_call) / bs_call * 100.0
        diag["PutΔ%"] = (diag["PutPrice"] - bs_put) / bs_put * 100.0
        with st.expander("Per-method deviation from Black-Scholes"):
            st.dataframe(
                diag[["method", "CallPrice", "CallΔ%", "PutPrice", "PutΔ%"]]
                .round({"CallPrice": 4, "CallΔ%": 2, "PutPrice": 4, "PutΔ%": 2}),
                use_container_width=True, hide_index=True,
            )

st.subheader("Raw rows")
st.dataframe(view, use_container_width=True, hide_index=True)

with st.expander("Job metadata"):
    st.json(job)


render_session_sidebar()