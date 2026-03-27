# Data Loading Strategy

This document explains how each data source is loaded into BigQuery and why.

---

## Data Sources

### 1. Polymarket — `polymarket_khamenei`

**Loading type: Full batch replace (`if_exists="replace"`)**

The Polymarket CLOB API exposes a `/prices-history` endpoint that returns the **complete price history** of a market token in a single HTTP response (using `interval=max`). Because the entire dataset is always returned at once, the right strategy is a **full replace**: we drop and recreate the table every time `load_bq.py` runs.

- **Why not append?** The API does not support incremental fetches — it always returns all rows. Appending would create duplicates.
- **Why not streaming?** Polymarket data is daily candlesticks. Real-time streaming would add complexity for no benefit.
- **Table**: `sipa-adv-c-wiggly-donut.2444_n.polymarket_khamenei`
- **Columns**: `date` (TIMESTAMP), `yes_price` (FLOAT, 0–100)

---

### 2. Kalshi — `kalshi_khamenei`

**Loading type: Full batch replace (`if_exists="replace"`)**

The Kalshi candlestick API also returns all historical daily candles in a single call when given a start timestamp at market inception (January 9, 2026). The dataset is small (one row per trading day), so a full replace is fast and keeps the implementation symmetric with Polymarket.

- **Why not append?** Like Polymarket, requesting all data since inception and appending would duplicate rows. Deduplicating after each append would add unnecessary complexity.
- **Why not streaming?** The market resolves on April 1, 2026. One batch refresh per day is more than sufficient.
- **Table**: `sipa-adv-c-wiggly-donut.2444_n.kalshi_khamenei`
- **Columns**: `date` (TIMESTAMP), `close_cents` (FLOAT, 0–100)

---

## Streamlit Read Path

Both tables are queried from BigQuery at page load using `pandas_gbq.read_gbq()`. To avoid paying the BigQuery round-trip cost on every user interaction, each query is wrapped in `@st.cache_data(ttl=600)`, which caches the result in memory for 10 minutes. This keeps page load times well under two seconds after the first cold load.

Queries select only the columns they need (`SELECT date, yes_price` rather than `SELECT *`) to minimize bytes processed and network transfer time.

---

## Refresh Schedule

Run `python load_bq.py` from the project root to refresh both tables. This requires local Google Cloud credentials (`gcloud auth application-default login`). In production, this script could be scheduled as a Cloud Run Job or a GitHub Actions cron workflow once per day.
