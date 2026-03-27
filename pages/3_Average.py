import os
import sys
import time

import pandas_gbq
import plotly.express as px
import streamlit as st
from google.oauth2.service_account import Credentials

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from Avg_func import average_probabilities

start_time = time.time()

st.title("Average Probability (Polymarket + Kalshi)")


@st.cache_data(ttl=600)
def load_both():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"])

    t0 = time.time()
    df_poly = pandas_gbq.read_gbq(
        "SELECT date, yes_price FROM `sipa-adv-c-wiggly-donut.2444_n.polymarket_khamenei` ORDER BY date",
        project_id="sipa-adv-c-wiggly-donut",
        credentials=creds,
    ).rename(columns={"date": "t", "yes_price": "p"})
    t1 = time.time()

    df_kalshi = pandas_gbq.read_gbq(
        "SELECT date, close_cents FROM `sipa-adv-c-wiggly-donut.2444_n.kalshi_khamenei` ORDER BY date",
        project_id="sipa-adv-c-wiggly-donut",
        credentials=creds,
    ).rename(columns={"date": "Date", "close_cents": "Close (¢)"})
    t2 = time.time()

    return df_poly, df_kalshi, t1 - t0, t2 - t1


df_poly, df_kalshi, poly_load_s, kalshi_load_s = load_both()

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

elapsed = time.time() - start_time
st.caption(
    f"Page loaded in {elapsed:.2f} seconds "
    f"(Polymarket BQ: {poly_load_s:.2f}s, Kalshi BQ: {kalshi_load_s:.2f}s)"
)
