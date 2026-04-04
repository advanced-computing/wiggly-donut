import json
from datetime import datetime

import pandas as pd
import pandas_gbq
import requests

PROJECT_ID = "sipa-adv-c-wiggly-donut"
DATASET_ID = "2444_n"
TABLE_NAME_POLY = "polymarket_khamenei"
TABLE_NAME_KALSHI = "kalshi_khamenei"


def get_polymarket_data():
    """Pulls full price history from the Polymarket API."""
    print("Fetching data from Polymarket API...")
    slug = "khamenei-out-as-supreme-leader-of-iran-by-march-31"

    # gamma API returns a list, so we grab [0], each event has multiple markets too
    response = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}")
    event = response.json()[0]
    market = event["markets"][0]
    token_id = json.loads(market["clobTokenIds"])[0]

    # fetch the full price history for the "Yes" tokens
    history = requests.get(
        "https://clob.polymarket.com/prices-history",
        params={"market": token_id, "interval": "max", "fidelity": 1440},
    ).json()

    # build df from the history; each entry has "t" and "p" (price 0–1)
    df = pd.DataFrame(history["history"])
    df["t"] = pd.to_datetime(df["t"], unit="s")
    df["p"] = df["p"].astype(float) * 100

    # Rename columns to be more descriptive for a database
    df = df.rename(columns={"t": "date", "p": "yes_price"})
    return df


def get_kalshi_data():
    """Pulls full candlestick history from the Kalshi API."""
    print("Fetching data from Kalshi API...")
    start_ts = int(datetime(2026, 1, 9).timestamp())
    end_ts = int(datetime.now().timestamp())

    response = requests.get(
        "https://api.elections.kalshi.com/trade-api/v2/series/KXKHAMENEIOUT"
        "/markets/KXKHAMENEIOUT-AKHA-26APR01/candlesticks",
        params={"period_interval": 1440, "start_ts": start_ts, "end_ts": end_ts},
    )
    candles = response.json()

    df = pd.DataFrame(
        [
            {
                "date": datetime.fromtimestamp(c["end_period_ts"]),
                "close_cents": float(c["price"]["close_dollars"]) * 100,
            }
            for c in candles["candlesticks"]
        ]
    )
    return df


def load_data_to_bq(df, project_id, dataset_id, table_name):
    """Copies the dataframe to BigQuery, replacing the existing table."""
    destination_table = f"{dataset_id}.{table_name}"
    print(f"Uploading {len(df)} rows to {destination_table}...")
    # We use if_exists="replace" because both APIs return the full historical
    # dataset in one call, so a full replace is the correct strategy.
    pandas_gbq.to_gbq(
        df,
        destination_table=destination_table,
        project_id=project_id,
        if_exists="replace",
    )
    print("Upload complete!")


if __name__ == "__main__":
    df_poly = get_polymarket_data()
    load_data_to_bq(df_poly, PROJECT_ID, DATASET_ID, TABLE_NAME_POLY)

    df_kalshi = get_kalshi_data()
    load_data_to_bq(df_kalshi, PROJECT_ID, DATASET_ID, TABLE_NAME_KALSHI)
