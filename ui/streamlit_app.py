import streamlit as st
from api_client import (
    health,
    submit_mc_job,
    get_results,
    get_job,
    login,
)

st.title("Job Orchestrator UI")

st.subheader("Backend health")
if st.button("Check health"):
    st.json(health())

# Auth -----------------------------------------------------------------
if "token" not in st.session_state:
    st.session_state["token"] = ""

with st.sidebar:
    st.subheader("Auth")
    username = st.text_input("Username", value="demo")
    password = st.text_input("Password", value="demo123", type="password")
    if st.button("Login"):
        st.session_state["token"] = login(username, password)
        st.success("Logged in")

token = st.session_state["token"]
if not token:
    st.warning("Login first to submit jobs.")
    st.stop()

# Job submission -------------------------------------------------------
st.divider()
st.subheader("Submit a Monte Carlo simulation")
st.caption(
    "Runs the `monte_carlo_simulation` Databricks notebook. Results are written "
    "to the ticker-partitioned parquet export."
)

ticker = st.text_input("Ticker", value="^GSPC")
strike = st.number_input("Strike price (K)", min_value=0.01, value=5700.0, step=10.0)
period_days = st.number_input("Period T (days)", min_value=1, value=10, step=1)
num_simulations = st.number_input(
    "Number of simulations", min_value=1, value=1000, step=100
)

if st.button("Submit Monte Carlo"):
    resp = submit_mc_job(
        token=token,
        ticker=ticker,
        strike=float(strike),
        period_days=int(period_days),
        num_simulations=int(num_simulations),
    )
    st.session_state["job_id"] = resp["job_id"]
    st.success(f"Submitted job_id={resp['job_id']}")
    st.json(resp)

# Status + results -----------------------------------------------------
st.divider()
st.subheader("Query results")

job_id = st.session_state.get("job_id")
if job_id:
    st.subheader(f"Job status (job_id={job_id})")
    if st.button("Refresh status"):
        st.json(get_job(token, job_id))

    status = get_job(token, job_id)
    if status["status"] == "SUCCEEDED":
        st.success("Job finished. Loading results...")
        res = get_results(token, job_id)
        if res.get("rows"):
            st.dataframe(res["rows"])
        else:
            st.info("No rows returned for this job.")
