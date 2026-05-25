"""Landing + login page for the Monte Carlo Option Pricing UI.

Multi-page layout: this file is the entrypoint, the actual workflow pages
live under `ui/pages/` and are auto-discovered by Streamlit.
"""

import streamlit as st

from api_client import (
    decode_jwt_payload,
    login,
    render_session_sidebar,
    token_expiry_seconds,
)

st.set_page_config(
    page_title="Monte Carlo Option Pricing",
    page_icon="MC",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Monte Carlo Option Pricing")
st.caption(
    "Submit, monitor and inspect Monte Carlo option-pricing jobs that run on "
    "Databricks. Use the pages in the sidebar to navigate."
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
if "token" not in st.session_state:
    st.session_state["token"] = ""
token = st.session_state["token"]

col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("Sign in")
    if token and token_expiry_seconds(token):
        payload = decode_jwt_payload(token)
        user = payload.get("sub") or payload.get("username") or "user"
        st.success(f"Signed in as **{user}**.")
    else:
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("Username", value="demo")
            password = st.text_input("Password", value="demo123", type="password")
            submitted = st.form_submit_button("Sign in", use_container_width=True)
        if submitted:
            try:
                tok = login(username, password)
            except Exception as e:
                st.error(f"Login failed: {e}")
            else:
                st.session_state["token"] = tok
                st.rerun()

with col_right:
    st.subheader("Where to go next")
    st.markdown(
        "- **Submit** — queue a new Monte Carlo simulation on Databricks.\n"
        "- **Jobs Monitoring** — watch recent jobs and refresh their status.\n"
        "- **Jobs Results** — chart prices and compare methods for one job.\n"
        "- **Compare** — line up multiple jobs or methods side by side.\n"
        "- **Analysis** — research outputs (MC vs market, EMC, sweeps).\n"
        "- **About** — architecture, methods, Databricks notebooks.\n"
        "- **Admin** — backend health and connection details."
    )

render_session_sidebar()
