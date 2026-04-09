import os
import sys
import time

import plotly.express as px
import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from Avg_func import average_probabilities
from data import load_kalshi, load_polymarket

start_time = time.time()

st.title("Average Probability (Polymarket + Kalshi)")

df_poly = load_polymarket()
df_kalshi = load_kalshi()

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

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
