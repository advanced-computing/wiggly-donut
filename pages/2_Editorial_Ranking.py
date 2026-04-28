import os
import sys
import time

import pandas as pd
import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data import fetch_newsapi_top

st.set_page_config(page_title="NewsAPI Top Headlines", layout="wide")

start_time = time.time()

st.title("📰 NewsAPI Top Headlines")
st.caption(
    "Editorial baseline. What newsroom editors picked as today's top US stories — "
    "to compare against what's trending in prediction markets on the home page."
)

df = fetch_newsapi_top(limit=30)

if df.empty:
    st.warning(
        "No NewsAPI data. Set the `NEWSAPI_API_KEY` environment variable "
        "or add `[newsapi].api_key` to `.streamlit/secrets.toml`."
    )
    st.stop()

st.caption(f"Showing {len(df)} top US headlines from NewsAPI.")

CARDS_PER_ROW = 3
n = len(df)
for row_start in range(0, n, CARDS_PER_ROW):
    cols = st.columns(CARDS_PER_ROW)
    for offset in range(CARDS_PER_ROW):
        idx = row_start + offset
        if idx >= n:
            break
        article = df.iloc[idx]
        with cols[offset], st.container(border=True):
            title = str(article.get("title") or "")
            if article.get("url"):
                st.markdown(f"#### [{title}]({article['url']})")
            else:
                st.markdown(f"#### {title}")

            bits = []
            if article.get("source"):
                bits.append(str(article["source"]))
            pub = article.get("published_at")
            if pd.notna(pub):
                bits.append(f"{pub:%Y-%m-%d %H:%M UTC}")
            if bits:
                st.caption(" • ".join(bits))

            description = article.get("description")
            if isinstance(description, str) and description.strip():
                st.write(description)

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
