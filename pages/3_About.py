import time

import streamlit as st

start_time = time.time()

st.set_page_config(page_title="About", layout="wide")

st.title("ℹ️ About this project")

st.markdown(
    """
    **Daily News Basket** is a cross-platform prediction-market dashboard for
    politics and geopolitics. It inverts the usual *news → market* flow: it
    starts from the markets that moved most in the last 24 hours, pairs them
    across Polymarket and Kalshi when they track the same outcome, and then
    pulls the news story behind the move.
    """
)

st.subheader("How it works")
st.markdown(
    """
    1. **Fetch top movers** from the public Polymarket Gamma and Kalshi v2 APIs,
       filter to politics/geopolitics (Kalshi by canonical category, Polymarket
       via a Gemini classifier), dedupe by event, and rank by
       `|change_pp| × log(1 + 24h volume)`.
    2. **Pair markets across platforms** with a single Gemini call that requires
       same outcome, direction, and scope.
    3. **Generate the story** for each move with Gemini + Google Search
       grounding, returning a verified article URL from the last 7 days.
    4. **Aggregate baskets** by averaging the Polymarket and Kalshi yes-prices
       per story, comparing today's snapshot against the previous one in
       BigQuery.
    5. **Render** the trending chart and story cards in Streamlit.
    """
)

st.subheader("Pages")
st.markdown(
    """
    - **Prediction Market Ranking** — top trending politics/geopolitics
      stories ranked by volume-weighted basket movement.
    - **Editorial Ranking** — today's top US headlines from NewsAPI, an
      editorial baseline to compare against the market signal.
    - **About** — this page.
    """
)

st.subheader("Data sources")
st.markdown(
    """
    - [Polymarket Gamma API](https://docs.polymarket.com/quickstart/overview)
    - [Kalshi v2 API](https://docs.kalshi.com/welcome)
    - [NewsAPI](https://newsapi.org/)
    - Gemini (Vertex AI) for classification, pairing, and grounded story
      generation.
    - BigQuery for daily snapshot persistence.
    """
)

st.subheader("Team")
st.markdown("Group: **wiggly-donut**, Naveen and Romain, Columbia SIPA Advanced Computing.")

st.caption(
    "For the full proposal, research questions, and methodology, see "
    "[`README.md`](https://github.com/nav-v/adv-comp-project/blob/main/README.md) "
    "in the repo."
)

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
