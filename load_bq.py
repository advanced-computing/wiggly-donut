import json
import pandas as pd
import requests
import pandas_gbq

# TODO: Fill in your GCP Project ID and Dataset Name
PROJECT_ID = "aerial-reef-486622-t2" 
DATASET_ID = "2444_n"
TABLE_NAME = "polymarket_khamenei"

def get_polymarket_data():
    """Pulls data from the Polymarket API and cleans it."""
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
    df_hist = pd.DataFrame(history["history"])
    df_hist["t"] = pd.to_datetime(df_hist["t"], unit="s")
    df_hist["p"] = df_hist["p"].astype(float) * 100
    
    # Rename columns to be more descriptive for a database
    df_hist = df_hist.rename(columns={"t": "date", "p": "yes_price"})
    return df_hist

def load_data_to_bq(df, project_id, dataset_id, table_name):
    """Copies the dataframe to BigQuery."""
    # The destination table requires dataset.table format
    destination_table = f"{dataset_id}.{table_name}"
    
    print(f"Uploading {len(df)} rows to {destination_table}...")
    
    # pandas-gbq handles authenticating with your local user account automatically 
    # when you run it from the console.
    # We use if_exists="replace" as the 'appropriate technique' here because the 
    # Polymarket API gives us the *entire* historical dataset in one go.
    # It will automatically create the table if it does not exist.
    pandas_gbq.to_gbq(
        df, 
        destination_table=destination_table,
        project_id=project_id,
        if_exists="replace",
    )
    print("Upload complete!")

if __name__ == "__main__":
    if PROJECT_ID == "YOUR_GCP_PROJECT_ID":
        print("Please edit load_bq.py and fill in your PROJECT_ID and DATASET_ID!")
    else:
        df = get_polymarket_data()
        load_data_to_bq(df, PROJECT_ID, DATASET_ID, TABLE_NAME)
