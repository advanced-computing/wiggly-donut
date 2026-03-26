import json
import os
import sys
from datetime import datetime

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from google.oauth2.service_account import Credentials
import pandas_gbq

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from Avg_func import average_probabilities

st.title("Average Probability (Polymarket + Kalshi)")

# --- Polymarket ---
@st.cache_data(ttl=600)
def load_poly_data():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    query = "SELECT * FROM `aerial-reef-486622-t2.2444_n.polymarket_khamenei` ORDER BY date"
    df = pandas_gbq.read_gbq(query, project_id="aerial-reef-486622-t2", credentials=creds)
    df = df.rename(columns={"date": "t", "yes_price": "p"})
    return df

df_poly = load_poly_data()

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
