"""Compare page: visualise multiple methods or jobs side-by-side."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from api_client import (
    get_job,
    get_results,
    get_token_or_stop,
    list_jobs,
    render_session_sidebar,
)

st.set_page_config(page_title="Compare - MC Orchestrator", layout="wide")
st.title("Compare")
st.caption(
    "Compare methods across a single job, or the same method across multiple "
    "jobs (different tickers / strikes / horizons / path counts)."
)

token = get_token_or_stop()


# ---------------------------------------------------------------------------
# Pick the jobs to compare
# ---------------------------------------------------------------------------
try:
    jobs_resp = list_jobs(token, status="SUCCEEDED", limit=100)
except Exception as e:
    st.error(f"Failed to list jobs: {e}")
    st.stop()

succeeded = jobs_resp.get("items", [])
if not succeeded:
    st.info("No SUCCEEDED jobs yet. Submit one first.")
    st.stop()


def _label(job: dict) -> str:
    return (
        f"#{job['job_id']}  {job.get('ticker') or '?'} "
        f"K={job.get('strike')} T={job.get('period_days')}d "
        f"runs={job.get('num_simulations')}"
    )


labels = {_label(j): j["job_id"] for j in succeeded}

mode = st.radio(
    "Comparison mode",
    options=["Methods within one job", "Same data across multiple jobs"],
    horizontal=True,
)

if mode == "Methods within one job":
    selected = st.selectbox("Job", options=list(labels.keys()))
    selected_ids = [labels[selected]]
else:
    selected_keys = st.multiselect(
        "Jobs (pick 2-5)",
        options=list(labels.keys()),
        default=list(labels.keys())[: min(2, len(labels))],
    )
    selected_ids = [labels[k] for k in selected_keys]

if not selected_ids:
    st.info("Pick at least one job.")
    st.stop()


# ---------------------------------------------------------------------------
# Load results for the selected jobs
# ---------------------------------------------------------------------------
frames = []
for jid in selected_ids:
    try:
        res = get_results(token, jid)
        job = get_job(token, jid)
    except Exception as e:
        st.warning(f"Skipping job {jid}: {e}")
        continue
    rows = res.get("rows") or []
    if not rows:
        continue
    df_j = pd.DataFrame(rows)
    df_j["job_id"] = jid
    df_j["job_label"] = (
        f"#{jid} {job.get('ticker') or ''} "
        f"K={job.get('strike')} T={job.get('period_days')}d "
        f"n={job.get('num_simulations')}"
    )
    frames.append(df_j)

if not frames:
    st.info("No rows returned for the selected jobs.")
    st.stop()

big = pd.concat(frames, ignore_index=True)

if "method" not in big.columns:
    st.warning("Results don't include a `method` column.")
    st.dataframe(big, use_container_width=True)
    st.stop()


# ---------------------------------------------------------------------------
# Method filter
# ---------------------------------------------------------------------------
present_methods = sorted(big["method"].dropna().unique().tolist())
default_methods = [
    m for m in ["black_scholes", "fhs_rn", "multifractal", "window"]
    if m in present_methods
] or present_methods
methods = st.multiselect("Methods", options=present_methods, default=default_methods)
if not methods:
    st.info("Pick at least one method.")
    st.stop()

view = big[big["method"].isin(methods)].copy()


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
side = st.radio("Show", options=["Call price", "Put price", "Both"], horizontal=True)
metric_cols = {
    "Call price": ["CallPrice"],
    "Put price": ["PutPrice"],
    "Both": ["CallPrice", "PutPrice"],
}[side]

if mode == "Methods within one job":
    # One panel per side; x=method, y=price.
    for col in metric_cols:
        st.subheader(f"{col} by method")
        fig = px.bar(view.sort_values("method"), x="method", y=col, color="method")
        fig.update_layout(showlegend=False, height=380, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
else:
    # x=method, y=price, one trace per job.
    for col in metric_cols:
        st.subheader(f"{col}: method x job")
        fig = px.bar(
            view.sort_values(["method", "job_label"]),
            x="method", y=col, color="job_label", barmode="group",
        )
        fig.update_layout(
            height=420, margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            legend_title_text="",
        )
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Spread vs Black-Scholes (when BS is present in every selected job)
# ---------------------------------------------------------------------------
if "black_scholes" in present_methods and "black_scholes" in methods:
    bs = (
        big[big["method"] == "black_scholes"]
        .groupby("job_id")[["CallPrice", "PutPrice"]]
        .mean()
        .rename(columns={"CallPrice": "BS_call", "PutPrice": "BS_put"})
    )
    merged = view.merge(bs, left_on="job_id", right_index=True, how="left")
    merged["CallΔ%"] = (merged["CallPrice"] - merged["BS_call"]) / merged["BS_call"] * 100.0
    merged["PutΔ%"] = (merged["PutPrice"] - merged["BS_put"]) / merged["BS_put"] * 100.0
    st.subheader("Deviation from Black-Scholes (%)")
    st.dataframe(
        merged[["job_label", "method", "CallPrice", "CallΔ%", "PutPrice", "PutΔ%"]]
        .round({"CallPrice": 4, "CallΔ%": 2, "PutPrice": 4, "PutΔ%": 2}),
        use_container_width=True,
        hide_index=True,
    )


with st.expander("Raw rows"):
    st.dataframe(view, use_container_width=True, hide_index=True)


render_session_sidebar()