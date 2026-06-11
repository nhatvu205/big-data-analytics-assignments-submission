import requests
import streamlit as st

st.set_page_config(page_title="People Counting Dashboard", layout="wide")
st.title("People Counting Dashboard")

api_url = st.sidebar.text_input("Storage API URL", value="http://localhost:8000")

try:
    stats = requests.get(f"{api_url}/stats", timeout=5).json()
    latest = requests.get(f"{api_url}/latest", params={"limit": 10}, timeout=5).json()["results"]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Frames", int(stats.get("total_frames", 0)))
    c2.metric("Max People", int(stats.get("max_count", 0)))
    c3.metric("Avg People", round(float(stats.get("avg_count", 0)), 2))

    st.subheader("Latest detection results")
    st.dataframe(latest, use_container_width=True)
except Exception as exc:
    st.error(f"Could not connect to storage API: {exc}")
