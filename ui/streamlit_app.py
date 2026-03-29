import streamlit as st
from datetime import date, timedelta
from api_client import health, submit_job, get_results, get_job

st.title("Async Jobs Demo")

st.subheader("Backend health")
if st.button("Check health"):
    st.json(health())
    
st.divider()
st.subheader("Submit a job (toy)")

start= st.date_input("Start date", value=date.today()-timedelta(days=7))
end = st.date_input("End date", value=date.today())
country=st.text_input("Filter: country (optional)", value="")

if st.button("Submit"):
    resp = submit_job(start_date=start,end_date=end, 
                      filters={"country": country} if country else {})
    st.session_state["job_id"]= resp["job_id"]
    st.success(f"Submitted job_id={resp['job_id']}")
    st.json(resp)

st.divider()
st.subheader("Query results")

job_id = st.session_state.get("job_id")
if job_id:
    st.subheader(f"Job status (job_id={job_id})")
    
    if st.button("Refresh status"):
        status = get_job(job_id)
        st.json(status)
    
    # If succeeded, show results
    status = get_job(job_id)
    if status["status"] == "SUCCEEDED":
        st.success("Job finished. Loading results...")    
        res=get_results(job_id,start,end)
        if res.get("rows"):
            st.dataframe(res["rows"])