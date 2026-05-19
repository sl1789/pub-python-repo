"""Jobs page: list recent jobs with auto-refresh when any are still running."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from api_client import get_token_or_stop, list_jobs, render_session_sidebar

st.set_page_config(page_title="Jobs - MC Orchestrator", layout="wide")
st.title("Jobs")
st.caption("Recent Monte Carlo jobs. Page auto-refreshes while any job is active.")

token = get_token_or_stop()

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}
ACTIVE_STATES = {"QUEUED", "RUNNING", "PENDING"}

# ---------------------------------------------------------------------------
# Filter controls
# ---------------------------------------------------------------------------
col_f1, col_f2, col_f3, col_f4 = st.columns([2, 1, 1, 1])
with col_f1:
    status_filter = st.selectbox(
        "Status filter",
        options=["(all)", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"],
        index=0,
    )
with col_f2:
    limit = st.number_input("Limit", min_value=5, max_value=200, value=50, step=5)
with col_f3:
    auto_refresh = st.checkbox("Auto-refresh", value=True)
with col_f4:
    if st.button("Refresh now", use_container_width=True):
        st.rerun()


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
try:
    data = list_jobs(
        token,
        status=None if status_filter == "(all)" else status_filter,
        limit=int(limit),
    )
except Exception as e:
    st.error(f"Failed to list jobs: {e}")
    st.stop()

items = data.get("items", [])
total = data.get("total", len(items))


# ---------------------------------------------------------------------------
# Build table
# ---------------------------------------------------------------------------
def _parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


rows = []
active_count = 0
for it in items:
    started = _parse_dt(it.get("started_at"))
    finished = _parse_dt(it.get("finished_at"))
    runtime = None
    if started and finished:
        runtime = round((finished - started).total_seconds(), 1)
    elif started and (it.get("status") or "").upper() in ACTIVE_STATES:
        runtime = round((datetime.now(timezone.utc) - started).total_seconds(), 1)

    status = (it.get("status") or "").upper()
    if status in ACTIVE_STATES:
        active_count += 1

    rows.append({
        "job_id": it.get("job_id"),
        "status": status,
        "ticker": it.get("ticker"),
        "K": it.get("strike"),
        "T": it.get("period_days"),
        "runs": it.get("num_simulations"),
        "runner": it.get("runner"),
        "submitted_at": it.get("submitted_at"),
        "runtime_s": runtime,
        "external_run_id": it.get("external_run_id"),
        "error": (it.get("error_message") or "")[:120],
    })

df = pd.DataFrame(rows)

st.markdown(f"**{total}** total job(s); showing {len(rows)}; {active_count} active.")

if df.empty:
    st.info("No jobs match the current filter.")
else:
    st.dataframe(df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Jump to results for a selected job
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Open results")
col_a, col_b = st.columns([2, 1])
with col_a:
    job_ids = [r["job_id"] for r in rows if r["status"] == "SUCCEEDED"]
    selected = st.selectbox(
        "Choose a finished job",
        options=job_ids if job_ids else ["(no finished jobs)"],
        index=0,
    )
with col_b:
    if st.button("Open in Results", use_container_width=True, disabled=not job_ids):
        st.session_state["results_job_id"] = int(selected)
        st.switch_page("pages/3_Results.py")


# ---------------------------------------------------------------------------
# Auto-refresh: only when something is actually moving
# ---------------------------------------------------------------------------
if auto_refresh and active_count > 0:
    # Avoid an extra pip dep: vanilla time-sleep + rerun is good enough for
    # the small job volumes this UI handles. The user can untick the box.
    import time
    with st.spinner(f"Polling ({active_count} active)..."):
        time.sleep(3)
    st.rerun()

render_session_sidebar()