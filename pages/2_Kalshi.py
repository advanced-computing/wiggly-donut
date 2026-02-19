import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime

st.title("Kalshi (live)")
start_ts = int(datetime(2026, 1, 9).timestamp())
end_ts = int(datetime.now().timestamp())

# each candlestick covers 1 day (1440 minutes)
candles = requests.get(
    "https://api.elections.kalshi.com/trade-api/v2/series/KXKHAMENEIOUT/markets/KXKHAMENEIOUT-AKHA-26APR01/candlesticks",
    params={"period_interval": 1440, "start_ts": start_ts, "end_ts": end_ts},
).json()

# build df from the candlestick data
df_hist = pd.DataFrame([
    {"Date": datetime.fromtimestamp(c["end_period_ts"]), "Close (¢)": c["price"]["close"]}
    for c in candles["candlesticks"]
])

# create line chart of price over time
fig = px.line(
    df_hist, x="Date", y="Close (¢)",
    title="Khamenei Out Before April 1 — Daily Close Price",
    labels={"Close (¢)": "Yes Price (¢)"},
)
fig.update_layout(yaxis_range=[0, 100])
st.plotly_chart(fig, use_container_width=True)

data = requests.get(
    "https://api.elections.kalshi.com/trade-api/v2/markets",
    params={"series_ticker": "KXKHAMENEIOUT"},
).json()
