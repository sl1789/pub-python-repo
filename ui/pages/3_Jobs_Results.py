"""Jobs Results page: chart and inspect a finished job's prices across methods."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from api_client import (
    get_job,
    get_results,
    get_token_or_stop,
    list_jobs,
    render_session_sidebar,
)

st.set_page_config(page_title="Jobs Results - Monte Carlo Option Pricing", layout="wide")
st.title("Jobs Results")
st.caption(
    "Compare Call/Put prices across simulation methods for a single job. "
    "Only `SUCCEEDED` jobs return results."
)

token = get_token_or_stop()
render_session_sidebar()
st.sidebar.info(
    "Looking for the MC-vs-market comparison, EMC pre/post, or block/lam "
    "sweeps? See the **Analysis** page."
)


ALL_KNOWN_METHODS = [
    "historical", "window", "window_10d", "window_20d",
    "student_t", "black_scholes",
    "multifractal", "multifractal_empirical",
    "block_bootstrap", "fhs", "fhs_rn", "analogue",
]


# ---------------------------------------------------------------------------
# Job picker — dropdown of SUCCEEDED jobs with ticker / strike / horizon /
# path count / submitted-at so the user does not have to remember IDs.
# ---------------------------------------------------------------------------
try:
    jobs_resp = list_jobs(token, status="SUCCEEDED", limit=200)
except Exception as e:
    st.error(f"Failed to list jobs: {e}")
    st.stop()

succeeded = jobs_resp.get("items", [])
if not succeeded:
    st.info(
        "No `SUCCEEDED` jobs yet. Submit one from the **Submit** page; once it "
        "finishes it will appear here automatically."
    )
    st.stop()


def _fmt_submitted(value: str | None) -> str:
    """Render submitted_at as 'YYYY-MM-DD HH:MM' (best-effort)."""
    if not value:
        return "?"
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:16]


def _label(job: dict) -> str:
    runs = job.get("num_simulations")
    runs_s = f"{runs:,}" if isinstance(runs, int) else str(runs)
    return (
        f"#{job.get('job_id')}  \u00b7  {job.get('ticker') or '?'}  "
        f"K={job.get('strike')}  T={job.get('period_days')}d  "
        f"runs={runs_s}  "
        f"\u00b7  submitted {_fmt_submitted(job.get('submitted_at'))}"
    )


labels = {_label(j): int(j["job_id"]) for j in succeeded}

# Pre-select whatever the user just opened from another page, if any.
pre_id = st.session_state.get("results_job_id") or st.session_state.get("last_job_id")
default_idx = 0
if pre_id:
    for i, jid in enumerate(labels.values()):
        if jid == int(pre_id):
            default_idx = i
            break

selected_label = st.selectbox(
    "Job",
    options=list(labels.keys()),
    index=default_idx,
    help="Showing succeeded jobs newest first. Pick one to load its results.",
)
job_id = labels[selected_label]
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

selected = st.multiselect(
    "Methods to display",
    options=present_methods,
    default=present_methods,
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