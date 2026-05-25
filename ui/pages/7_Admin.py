"""Admin page: backend health probe + connection metadata."""

from __future__ import annotations

import os

import streamlit as st

from api_client import API_BASE, health, render_session_sidebar

st.set_page_config(page_title="Admin - Monte Carlo Option Pricing", layout="wide")
st.title("Admin")
st.caption(
    "Operational details: backend connection target and a manual health "
    "probe. Use this page if the UI is unable to reach the API."
)

render_session_sidebar()


# ---------------------------------------------------------------------------
# Backend health
# ---------------------------------------------------------------------------
st.subheader("Backend health")
st.markdown(f"- **API base URL:** `{API_BASE}`")
st.markdown(
    "Hits `GET /health`. If this fails the FastAPI service is not reachable "
    "at the URL above. Check the `API_BASE` env var in `.env`, restart the "
    "backend (`python -m uvicorn app.main:app ...`), and try again."
)

if st.button("Check API health", use_container_width=False):
    try:
        st.json(health())
    except Exception as e:
        st.error(f"Health check failed: {e}")


# ---------------------------------------------------------------------------
# Effective configuration (non-secret subset)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Effective configuration")
st.caption(
    "Subset of env vars the UI uses to talk to the backend / ADLS. Values "
    "marked `<set>` exist in the environment without being printed."
)

NON_SECRET_KEYS = [
    "API_BASE",
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_RESULTS_CONTAINER",
    "AZURE_RESULTS_PREFIX",
    "DATABRICKS_HOST",
    "DATABRICKS_JOB_ID",
]
SECRET_KEYS = [
    "JWT_SECRET",
    "DATABRICKS_TOKEN",
    "AZURE_STORAGE_KEY",
    "DEMO_USER_PASSWORD",
]

rows = []
for k in NON_SECRET_KEYS:
    rows.append((k, os.getenv(k) or "<unset>"))
for k in SECRET_KEYS:
    rows.append((k, "<set>" if os.getenv(k) else "<unset>"))

st.dataframe(
    {"variable": [r[0] for r in rows], "value": [r[1] for r in rows]},
    hide_index=True,
    use_container_width=True,
)
