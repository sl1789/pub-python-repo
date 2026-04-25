import streamlit as st
from datetime import date, timedelta
from api_client import health, submit_job, get_results, get_job, login

st.title("Job Orchestrator UI")

st.subheader("Backend health")
if st.button("Check health"):
    st.json(health())
    
    
    
# 1) Login (Phase E) - store token in session
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
    
st.divider()
st.subheader("Submit a job (toy)")
# 2) Job params
start= st.date_input("Start date", value=date.today()-timedelta(days=7))
end = st.date_input("End date", value=date.today())

runner = st.selectbox("Runner", ["local", "databricks"]) # keep it simple
country=st.text_input("Filter: country (optional)", value="")
# 3) Submit
if st.button("Submit"):
    resp = submit_job(
        token=token,
        start_date=start,
        end_date=end, 
        filters={"country": country} if country else {},
        runner=runner,)
    st.session_state["job_id"]= resp["job_id"]
    st.success(f"Submitted job_id={resp['job_id']}")
    st.json(resp)

st.divider()
st.subheader("Query results")
# 4) Status + results
job_id = st.session_state.get("job_id")
if job_id:
    st.subheader(f"Job status (job_id={job_id})")
    
    if st.button("Refresh status"):
        status = get_job(token,job_id)
        st.json(status)
    
    # If succeeded, show results
    status = get_job(token,job_id)
    if status["status"] == "SUCCEEDED":
        st.success("Job finished. Loading results...")    
        res=get_results(token,job_id,start,end)
        if res.get("rows"):
            st.dataframe(res["rows"])