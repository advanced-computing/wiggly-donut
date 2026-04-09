import pandas as pd
import pandas_gbq
import streamlit as st
from google.oauth2.service_account import Credentials

PROJECT_ID = "sipa-adv-c-wiggly-donut"
DATASET_ID = "2444_n"
TABLE_NAME_HEADLINES = "daily_headlines"
TABLE_NAME_MATCHES = "daily_market_matches"
TABLE_NAME_BASKETS = "daily_story_baskets"
BASKET_NUMERIC_COLUMNS = [
    "headline_rank",
    "polymarket_yes_price",
    "polymarket_prev_yes_price",
    "polymarket_change_1d",
    "kalshi_yes_price",
    "kalshi_prev_yes_price",
    "kalshi_change_1d",
    "matched_platform_count",
    "change_platform_count",
    "basket_yes_price",
    "basket_prev_yes_price",
    "basket_change_1d",
    "rank_by_abs_change",
]
MATCH_NUMERIC_COLUMNS = [
    "headline_rank",
    "candidate_rank",
    "attena_rank",
    "token_overlap",
    "match_score",
    "yes_price",
    "no_price",
    "volume",
    "volume_24h",
    "bracket_count",
]


def _get_credentials() -> Credentials:
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"])


def _query_bq(query: str) -> pd.DataFrame:
    return pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=_get_credentials())


def _safe_query_bq(query: str) -> pd.DataFrame:
    try:
        return _query_bq(query)
    except Exception:
        return pd.DataFrame()


def _coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


@st.cache_data(ttl=600)
def load_story_baskets() -> pd.DataFrame:
    query = f"""
        WITH latest AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_BASKETS}`
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date)
                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_BASKETS}`
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, snapshot_story_id
                ORDER BY loaded_at DESC
            ) = 1
        )
        SELECT *
        FROM latest
        ORDER BY rank_by_abs_change
    """
    df = _safe_query_bq(query)
    if not df.empty:
        df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
        df = _coerce_numeric_columns(df, BASKET_NUMERIC_COLUMNS)
    return df


@st.cache_data(ttl=600)
def load_selected_matches(source: str | None = None) -> pd.DataFrame:
    source_clause = ""
    if source in {"polymarket", "kalshi"}:
        source_clause = f"AND source = '{source}'"

    query = f"""
        WITH latest AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_MATCHES}`
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date)
                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_MATCHES}`
            )
              AND selected = TRUE
              {source_clause}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, snapshot_story_id, source
                ORDER BY loaded_at DESC
            ) = 1
        )
        SELECT *
        FROM latest
        ORDER BY headline_rank
    """
    df = _safe_query_bq(query)
    if not df.empty:
        df["close_time"] = pd.to_datetime(df["close_time"], utc=True, errors="coerce")
        if "event_date" in df.columns:
            df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
        df = _coerce_numeric_columns(df, MATCH_NUMERIC_COLUMNS)
    return df


@st.cache_data(ttl=600)
def load_recent_selected_matches(snapshot_count: int = 3) -> pd.DataFrame:
    query = f"""
        WITH recent_dates AS (
            SELECT snapshot_date
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_MATCHES}`
            GROUP BY snapshot_date
            ORDER BY snapshot_date DESC
            LIMIT {snapshot_count}
        ),
        recent_matches AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_MATCHES}`
            WHERE selected = TRUE
              AND snapshot_date IN (SELECT snapshot_date FROM recent_dates)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, source, market_id
                ORDER BY loaded_at DESC
            ) = 1
        )
        SELECT *
        FROM recent_matches
        ORDER BY snapshot_date, headline_rank, source
    """
    df = _safe_query_bq(query)
    if not df.empty:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
        df["close_time"] = pd.to_datetime(df["close_time"], utc=True, errors="coerce")
        if "event_date" in df.columns:
            df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
        df = _coerce_numeric_columns(df, MATCH_NUMERIC_COLUMNS)
    return df


@st.cache_data(ttl=600)
def load_news() -> pd.DataFrame:
    query = f"""
        WITH latest AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_HEADLINES}`
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date)
                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_HEADLINES}`
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, snapshot_story_id
                ORDER BY loaded_at DESC
            ) = 1
        )
        SELECT *
        FROM latest
        ORDER BY headline_rank
    """
    df = _safe_query_bq(query)
    if not df.empty:
        df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    return df


@st.cache_data(ttl=600)
def load_polymarket() -> pd.DataFrame:
    return load_selected_matches("polymarket")


@st.cache_data(ttl=600)
def load_kalshi() -> pd.DataFrame:
    return load_selected_matches("kalshi")
