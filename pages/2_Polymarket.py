import os
import sys
import time

import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data import load_polymarket

start_time = time.time()

st.title("Polymarket Matches")

df = load_polymarket()

if df.empty:
    st.warning("No stored Polymarket matches found yet. Run `python load_bq.py` first.")
    st.stop()

st.caption(f"Latest snapshot: **{df['snapshot_date'].iloc[0]}**")

display_df = df[
    [
        "headline_rank",
        "headline_title",
        "market_title",
        "yes_price",
        "volume_24h",
        "close_time",
        "source_url",
    ]
].rename(
    columns={
        "headline_rank": "News Rank",
        "headline_title": "Headline",
        "market_title": "Matched Market",
        "yes_price": "Yes Price (%)",
        "volume_24h": "24h Volume",
        "close_time": "Closes",
        "source_url": "Market Link",
    }
)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Yes Price (%)": st.column_config.NumberColumn(format="%.2f"),
        "24h Volume": st.column_config.NumberColumn(format="%.0f"),
        "Market Link": st.column_config.LinkColumn("Market Link"),
    },
)

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
