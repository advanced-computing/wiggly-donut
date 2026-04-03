import time

import pandas_gbq
import plotly.express as px
import streamlit as st
from google.oauth2.service_account import Credentials

start_time = time.time()

st.title("Polymarket (from BigQuery)")


@st.cache_data(ttl=600)
def load_data():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    query = """
        SELECT date, yes_price
        FROM `sipa-adv-c-wiggly-donut.2444_n.polymarket_khamenei`
        ORDER BY date
    """
    df = pandas_gbq.read_gbq(query, project_id="sipa-adv-c-wiggly-donut", credentials=creds)
    return df.rename(columns={"date": "t", "yes_price": "p"})


df_hist = load_data()

fig = px.line(
    df_hist, x="t", y="p",
    title="Khamenei Out Before April 1 — Daily Yes Price",
    labels={"t": "Date", "p": "Yes Price (%)"},
)
fig.update_layout(yaxis_range=[0, 100])
st.plotly_chart(fig, use_container_width=True)

elapsed = time.time() - start_time
st.caption(f"Page loaded in {elapsed:.2f} seconds")
