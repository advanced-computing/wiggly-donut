import streamlit as st
import requests
import pandas as pd
import json
import plotly.express as px

st.title("Polymarket (live)")
slug = "khamenei-out-as-supreme-leader-of-iran-by-march-31"

# gamma API returns a list, so we grab [0], each event has multiple markets too
event = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()[0]
market = event["markets"][0]
token_id = json.loads(market["clobTokenIds"])[0]

# feth the full price history for the "Yes" tokens
history = requests.get(
    f"https://clob.polymarket.com/prices-history",
    params={"market": token_id, "interval": "max", "fidelity": 1440},
).json()

# build df from the history; each entry has "t" and "p" (price 0–1)
df_hist = pd.DataFrame(history["history"])
df_hist["t"] = pd.to_datetime(df_hist["t"], unit="s")
df_hist["p"] = df_hist["p"].astype(float) * 100

# create line chart of price over time
fig = px.line(
    df_hist, x="t", y="p",
    title=event["title"],
    labels={"t": "Date", "p": "Yes Price (%)"},
)
fig.update_layout(yaxis_range=[0, 100])
st.plotly_chart(fig, use_container_width=True)
