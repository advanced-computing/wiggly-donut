import streamlit as st
import requests
import pandas as pd
import json
import plotly.express as px
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from Avg_func import average_probabilities

st.title("Average Probability (Polymarket + Kalshi)")

# --- Polymarket ---
slug = "khamenei-out-as-supreme-leader-of-iran-by-march-31"
event = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()[0]
market = event["markets"][0]
token_id = json.loads(market["clobTokenIds"])[0]

history = requests.get(
    "https://clob.polymarket.com/prices-history",
    params={"market": token_id, "interval": "max", "fidelity": 1440},
).json()

df_poly = pd.DataFrame(history["history"])
df_poly["t"] = pd.to_datetime(df_poly["t"], unit="s")
df_poly["p"] = df_poly["p"].astype(float) * 100

# --- Kalshi ---
start_ts = int(datetime(2026, 1, 9).timestamp())
end_ts = int(datetime.now().timestamp())

candles = requests.get(
    "https://api.elections.kalshi.com/trade-api/v2/series/KXKHAMENEIOUT/markets/KXKHAMENEIOUT-AKHA-26APR01/candlesticks",
    params={"period_interval": 1440, "start_ts": start_ts, "end_ts": end_ts},
).json()

df_kalshi = pd.DataFrame(
    [
        {
            "Date": datetime.fromtimestamp(c["end_period_ts"]),
            "Close (¢)": c["price"]["close"],
        }
        for c in candles["candlesticks"]
    ]
)

# --- Average ---
avg = average_probabilities(df_poly, df_kalshi)

fig = px.line(
    avg,
    x="Date",
    y="Average (%)",
    title="Khamenei Out — Average Probability (Polymarket + Kalshi)",
    labels={"Average (%)": "Average Yes Probability (%)"},
)
fig.update_layout(yaxis_range=[0, 100])
st.plotly_chart(fig, use_container_width=True)

st.dataframe(avg)
