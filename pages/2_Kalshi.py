import streamlit as st
import requests

st.title("Kalshi (live)")

data = requests.get(
    "https://api.elections.kalshi.com/trade-api/v2/series/KXHIGHNY"
).json()
series = data["series"]

st.write("Title:", series["title"])
st.write("Frequency:", series["frequency"])
st.write("Category:", series["category"])

# simple viz: show 3 values as a bar chart (counts of characters)
vals = {
    "title_len": len(series["title"]),
    "frequency_len": len(series["frequency"]),
    "category_len": len(series["category"]),
}
st.bar_chart(vals)
