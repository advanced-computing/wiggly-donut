import os
import sys
import time

import plotly.express as px
import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data import load_kalshi

start_time = time.time()

st.title("Kalshi (from BigQuery)")

df = load_kalshi()

fig = px.line(
    df, x="Date", y="Close (¢)",
    title="Khamenei Out Before April 1 — Daily Close Price",
    labels={"Close (¢)": "Yes Price (¢)"},
)
fig.update_layout(yaxis_range=[0, 100])
st.plotly_chart(fig, use_container_width=True)

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
