"""Landing + login page for the MC Orchestrator UI.

Multi-page layout: this file is the entrypoint, the actual workflow pages
live under `ui/pages/` and are auto-discovered by Streamlit.
"""

import streamlit as st

from api_client import (
    decode_jwt_payload,
    health,
    login,
    render_session_sidebar,
    token_expiry_seconds,
)

st.set_page_config(
    page_title="MC Orchestrator",
    page_icon="MC",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Monte Carlo Orchestrator")
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
        st.markdown("- Use **Submit** to queue a new simulation.")
        st.markdown("- Use **Jobs** to see recent jobs and refresh their status.")
        st.markdown("- Use **Results** to chart prices and compare methods.")
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
    st.subheader("Backend health")
    if st.button("Check API health", use_container_width=True):
        try:
            st.json(health())
        except Exception as e:
            st.error(f"Health check failed: {e}")
    st.caption(
        "If this fails the FastAPI service is not reachable at `API_BASE`. "
        "Check the env var or restart the backend."
    )

render_session_sidebar()
