import streamlit as st
from datetime import date, timedelta
from api_client import health, submit_job, get_results

st.title("Toy UI: Submit Job + View Results")

st.subheader("Backend health")
if st.button("Check /health"):
    st.json(health())
    
st.divider()
st.subheader("Submit a job (toy)")

start= st.date_input("Start date", value=date.today()-timedelta(days=7))
end = st.date_input("End date", value=date.today())
country=st.text_input("Filter: country (optional)", value="")

if st.button("Submit"):
    resp = submit_job(start_date=start,end_date=end, 
                      filters={"country": country} if country else {})
    st.success("Submitted")
    st.json(resp)

st.divider()
st.subheader("Query results")

if st.button("Load results"):
    res=get_results(start,end)
    st.json(res)
    if res.get("rows"):
        st.dataframe(res["rows"])