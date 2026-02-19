import streamlit as st
import requests
import pandas as pd

st.title("Polymarket (live)")

# 1) Fetch data
url = "https://gamma-api.polymarket.com/events?limit=20"
data = requests.get(url).json()  # returns a list of events

# 2) Put into a table
df = pd.DataFrame(data)
st.dataframe(df)

# 3) Simple visualization (titles exist almost always)
df["title_len"] = df["title"].astype(str).str.len()
st.bar_chart(df["title_len"])
