"""Submit page: queue a new Monte Carlo simulation."""

import re

import streamlit as st

from api_client import (
    get_token_or_stop,
    render_session_sidebar,
    submit_mc_job,
)

st.set_page_config(page_title="Submit - MC Orchestrator", layout="wide")
st.title("Submit a simulation")
st.caption(
    "Runs the `monte_carlo_simulation` Databricks notebook for the selected "
    "ticker. Results are written to the ticker-partitioned parquet export."
)

token = get_token_or_stop()


# ---------------------------------------------------------------------------
# Quick presets (most common configurations)
# ---------------------------------------------------------------------------
PRESETS: dict[str, dict] = {
    "(custom)": {},
    "SPY ATM 10d / 1M paths": {
        "ticker": "SPY", "strike": 600.0, "period_days": 10, "num_simulations": 1_000_000,
    },
    "^GSPC ATM 30d / 100K paths": {
        "ticker": "^GSPC", "strike": 5700.0, "period_days": 30, "num_simulations": 100_000,
    },
    "AAPL ATM 14d / 500K paths": {
        "ticker": "AAPL", "strike": 220.0, "period_days": 14, "num_simulations": 500_000,
    },
    "MSFT ATM 21d / 250K paths": {
        "ticker": "MSFT", "strike": 420.0, "period_days": 21, "num_simulations": 250_000,
    },
}

preset_name = st.selectbox("Quick preset", list(PRESETS), index=0)
preset = PRESETS[preset_name]


# ---------------------------------------------------------------------------
# Validation rules mirror app/schemas/jobs.py so the user gets fast feedback.
# ---------------------------------------------------------------------------
_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-^=]{1,16}$")


def _validate(ticker: str, strike: float, period_days: int, num_sims: int) -> list[str]:
    errors = []
    if not _TICKER_RE.match(ticker or ""):
        errors.append("Ticker must be 1-16 chars of A-Z, 0-9, `.`, `-`, `^`, `=`.")
    if strike <= 0:
        errors.append("Strike must be > 0.")
    if period_days <= 0:
        errors.append("Period must be > 0 days.")
    if num_sims <= 0:
        errors.append("Number of simulations must be > 0.")
    return errors


# ---------------------------------------------------------------------------
# Submission form
# ---------------------------------------------------------------------------
with st.form("submit_form", clear_on_submit=False):
    col1, col2 = st.columns(2)
    with col1:
        ticker = st.text_input("Ticker", value=preset.get("ticker", "^GSPC"))
        strike = st.number_input(
            "Strike price (K)",
            min_value=0.01,
            value=float(preset.get("strike", 5700.0)),
            step=10.0,
        )
    with col2:
        period_days = st.number_input(
            "Period T (days)",
            min_value=1,
            value=int(preset.get("period_days", 10)),
            step=1,
        )
        num_simulations = st.number_input(
            "Number of simulations",
            min_value=1,
            value=int(preset.get("num_simulations", 1000)),
            step=100,
            help="Larger is more accurate. 1M paths runs in ~1-2s per method.",
        )
    submitted = st.form_submit_button("Submit Monte Carlo", use_container_width=True)

if submitted:
    errors = _validate(ticker, float(strike), int(period_days), int(num_simulations))
    if errors:
        for e in errors:
            st.error(e)
    else:
        try:
            resp = submit_mc_job(
                token=token,
                ticker=ticker,
                strike=float(strike),
                period_days=int(period_days),
                num_simulations=int(num_simulations),
            )
        except Exception as e:
            st.error(f"Submission failed: {e}")
        else:
            job_id = resp.get("job_id")
            st.session_state["last_job_id"] = job_id
            history = st.session_state.setdefault("submitted_jobs", [])
            history.insert(0, {
                "job_id": job_id,
                "ticker": ticker,
                "K": float(strike),
                "T": int(period_days),
                "runs": int(num_simulations),
                "status": resp.get("status"),
            })
            st.session_state["submitted_jobs"] = history[:20]  # cap

            st.success(f"Queued job #{job_id}. Open the **Jobs** page to monitor.")
            st.json(resp)


# ---------------------------------------------------------------------------
# Recent local submissions (this browser session only)
# ---------------------------------------------------------------------------
recent = st.session_state.get("submitted_jobs", [])
if recent:
    st.divider()
    st.subheader("Recent submissions (this session)")
    st.dataframe(recent, use_container_width=True, hide_index=True)


render_session_sidebar()