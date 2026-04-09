import os
import sys
import time

import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data import load_news

start_time = time.time()

st.title("Stored News Snapshot")
st.write("The latest daily headline snapshot stored in BigQuery.")

df = load_news()

if df.empty:
    st.warning("No stored headlines found yet. Run `python load_bq.py` first.")
    st.stop()

st.caption(f"Latest snapshot: **{df['snapshot_date'].iloc[0]}**")

for _, row in df.iterrows():
    st.markdown(f"**#{int(row['headline_rank'])} [{row['title']}]({row['url']})**")
    st.caption(
        f"{row['news_source']}  •  "
        f"{row['published_at']:%Y-%m-%d %H:%M UTC}"
    )
    if row.get("description"):
        st.write(row["description"])
    st.divider()

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
