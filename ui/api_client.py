"""HTTP client + shared Streamlit auth/session helpers.

Keeping a single module for both the requests-level helpers and the
Streamlit session helpers means each page can `from api_client import *`
without each one re-implementing token plumbing.
"""

from __future__ import annotations

import base64
import json
import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000")


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

def login(username: str, password: str) -> str:
    r = requests.post(
        f"{API_BASE}/auth/token",
        data={"username": username, "password": password},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def health() -> dict:
    r = requests.get(f"{API_BASE}/health", timeout=10)
    r.raise_for_status()
    return r.json()


def submit_mc_job(
    token: str,
    ticker: str,
    strike: float,
    period_days: int,
    num_simulations: int,
    runner: str = "databricks",
) -> dict:
    """Submit a Monte Carlo simulation job (databricks-only)."""
    payload = {
        "runner": runner,
        "ticker": ticker,
        "strike": float(strike),
        "period_days": int(period_days),
        "num_simulations": int(num_simulations),
    }
    r = requests.post(
        f"{API_BASE}/jobs", json=payload, headers=_headers(token), timeout=30
    )
    if r.status_code >= 400:
        raise RuntimeError(f"POST /jobs failed ({r.status_code}): {r.text}")
    return r.json()


def list_jobs(
    token: str,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List recent jobs. Server returns {total, items: [JobResponse, ...]}."""
    params = {"limit": limit, "offset": offset}
    if status:
        params["status"] = status
    r = requests.get(
        f"{API_BASE}/jobs", params=params, headers=_headers(token), timeout=20
    )
    r.raise_for_status()
    return r.json()


def get_job(token: str, job_id: int) -> dict:
    r = requests.get(
        f"{API_BASE}/jobs/{job_id}", headers=_headers(token), timeout=20
    )
    r.raise_for_status()
    return r.json()


def get_results(token: str, job_id: int) -> dict:
    r = requests.get(
        f"{API_BASE}/results",
        params={"job_id": job_id},
        headers=_headers(token),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# JWT helpers (decode-only; we trust the server's signature)
# ---------------------------------------------------------------------------

def decode_jwt_payload(token: str) -> dict:
    """Best-effort decode of the JWT payload segment. Never raises."""
    try:
        _, payload_b64, _ = token.split(".")
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(padded)
        return json.loads(raw)
    except Exception:
        return {}


def token_expiry_seconds(token: str) -> Optional[int]:
    """Seconds until the JWT expires, or None if unknown / already expired."""
    payload = decode_jwt_payload(token)
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)):
        return None
    remaining = int(exp - time.time())
    return remaining if remaining > 0 else None


# ---------------------------------------------------------------------------
# Streamlit session helpers (lazy import so non-UI callers stay light)
# ---------------------------------------------------------------------------

def get_token_or_stop():
    """Return the current session token or render a login banner and stop.

    Each page calls this at the top to enforce auth without duplicating the
    login form. The actual login form lives on the landing page.
    """
    import streamlit as st

    token = st.session_state.get("token", "")
    if not token:
        st.warning("Not signed in. Open the **Home** page to sign in first.")
        st.stop()

    remaining = token_expiry_seconds(token)
    if remaining is None:
        return token  # we can't tell; let the server reject if stale
    if remaining <= 0:
        st.session_state["token"] = ""
        st.error("Your session has expired. Sign in again on the Home page.")
        st.stop()
    return token


def render_session_sidebar() -> None:
    """Show the current user + token expiry in the sidebar, plus a Logout button."""
    import streamlit as st

    token = st.session_state.get("token", "")
    with st.sidebar:
        st.divider()
        if not token:
            st.caption("Not signed in")
            return
        payload = decode_jwt_payload(token)
        user = payload.get("sub") or payload.get("username") or "user"
        remaining = token_expiry_seconds(token)
        if remaining is not None:
            mins = remaining // 60
            secs = remaining % 60
            st.caption(f"Signed in as **{user}**  \nExpires in {mins:02d}:{secs:02d}")
        else:
            st.caption(f"Signed in as **{user}**")
        if st.button("Logout", use_container_width=True):
            st.session_state["token"] = ""
            for k in ("last_job_id", "submitted_jobs"):
                st.session_state.pop(k, None)
            st.rerun()
