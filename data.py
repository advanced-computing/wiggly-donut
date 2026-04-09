import pandas as pd
import pandas_gbq
import streamlit as st
from google.oauth2.service_account import Credentials

PROJECT_ID = "sipa-adv-c-wiggly-donut"


def _get_credentials() -> Credentials:
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"])


def _query_bq(query: str) -> pd.DataFrame:
    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=_get_credentials())


@st.cache_data(ttl=600)
def load_polymarket() -> pd.DataFrame:
    """Return Polymarket data with columns ``t`` (datetime) and ``p`` (float, 0–100 %)."""
    df = _query_bq(
        "SELECT date, yes_price"
        " FROM `sipa-adv-c-wiggly-donut.2444_n.polymarket_khamenei`"
        " ORDER BY date"
    )
    return df.rename(columns={"date": "t", "yes_price": "p"})


@st.cache_data(ttl=600)
def load_kalshi() -> pd.DataFrame:
    """Return Kalshi data with columns ``Date`` (datetime) and ``Close (¢)`` (float, 0–100 ¢)."""
    df = _query_bq(
        "SELECT date, close_cents"
        " FROM `sipa-adv-c-wiggly-donut.2444_n.kalshi_khamenei`"
        " ORDER BY date"
    )
    return df.rename(columns={"date": "Date", "close_cents": "Close (¢)"})
