import os
import sys
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from typing import Optional

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data import load_recent_selected_matches, load_selected_matches, load_story_baskets

LOOKBACK_SNAPSHOTS = 3


def _to_float(value):
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(numeric) else float(numeric)


def _fmt_pct(value):
    numeric = _to_float(value)
    return "N/A" if numeric is None else f"{numeric:.2f}%"


def _fmt_change(value):
    numeric = _to_float(value)
    return "N/A" if numeric is None else f"{numeric:+.2f} pp"


def _sort_baskets(df: pd.DataFrame) -> pd.DataFrame:
    sort_key = pd.to_numeric(df["basket_change_1d"], errors="coerce").abs().fillna(-1)
    return (
        df.assign(_sort_key=sort_key)
        .sort_values(
            by=["_sort_key", "matched_platform_count", "headline_rank"],
            ascending=[False, False, True],
        )
        .drop(columns="_sort_key")
    )


def _build_history_chart(story_matches: pd.DataFrame, recent_matches: pd.DataFrame) -> Optional[go.Figure]:
    market_keys = {
        (row["source"], row["market_id"])
        for row in story_matches.to_dict("records")
        if row.get("market_id")
    }
    if not market_keys:
        return None

    history = recent_matches[
        recent_matches.apply(lambda row: (row["source"], row["market_id"]) in market_keys, axis=1)
    ].copy()
    if history.empty:
        return None

    history["snapshot_date"] = pd.to_datetime(history["snapshot_date"], errors="coerce")
    history = history.sort_values("snapshot_date")

    pivot = (
        history.pivot_table(
            index="snapshot_date",
            columns="source",
            values="yes_price",
            aggfunc="last",
        )
        .reset_index()
        .sort_values("snapshot_date")
    )
    pivot["basket"] = pivot[[col for col in ["polymarket", "kalshi"] if col in pivot.columns]].mean(axis=1)

    fig = go.Figure()

    if "basket" in pivot.columns:
        fig.add_trace(
            go.Scatter(
                x=pivot["snapshot_date"],
                y=pivot["basket"],
                mode="lines+markers",
                name="Basket",
                line={"width": 4, "color": "#111827"},
            )
        )

    if "polymarket" in pivot.columns:
        fig.add_trace(
            go.Scatter(
                x=pivot["snapshot_date"],
                y=pivot["polymarket"],
                mode="lines+markers",
                name="Polymarket",
                line={"width": 3, "color": "#2563eb"},
            )
        )

    if "kalshi" in pivot.columns:
        fig.add_trace(
            go.Scatter(
                x=pivot["snapshot_date"],
                y=pivot["kalshi"],
                mode="lines+markers",
                name="Kalshi",
                line={"width": 3, "color": "#f97316"},
            )
        )

    fig.update_layout(
        margin={"l": 0, "r": 0, "t": 20, "b": 0},
        height=320,
        yaxis_title="Yes price (%)",
        xaxis_title="Snapshot date",
        yaxis_range=[0, 100],
        legend_title_text="Series",
    )
    return fig


start_time = time.time()

st.title("Story Moves")

baskets = load_story_baskets()
matches = load_selected_matches()
recent_matches = load_recent_selected_matches(snapshot_count=LOOKBACK_SNAPSHOTS)

if baskets.empty:
    st.warning("No daily basket snapshot found in BigQuery yet. Run `python load_bq.py` first.")
    st.stop()

baskets = _sort_baskets(baskets)
snapshot_date = baskets["snapshot_date"].iloc[0]

st.caption(f"Latest stored snapshot: **{snapshot_date}**")
st.write(
    "Stories ranked by basket change over the latest day. "
    f"For now, each expanded chart shows the last **{LOOKBACK_SNAPSHOTS}** snapshot days."
)

for _, story in baskets.iterrows():
    expander_label = (
        f"{_fmt_change(story['basket_change_1d'])} | "
        f"{story['title']} | Basket {_fmt_pct(story['basket_yes_price'])}"
    )
    with st.expander(expander_label):
        st.markdown(f"### [{story['title']}]({story['url']})")
        st.caption(
            f"{story['news_source']}  •  "
            f"{story['published_at']:%Y-%m-%d %H:%M UTC}"
        )

        metric_col1, metric_col2, metric_col3 = st.columns(3)
        metric_col1.metric("Basket", _fmt_pct(story["basket_yes_price"]))
        metric_col2.metric("Change", _fmt_change(story["basket_change_1d"]))
        metric_col3.metric("Matched Platforms", int(story["matched_platform_count"]))

        story_matches = matches[matches["snapshot_story_id"] == story["snapshot_story_id"]]
        fig = _build_history_chart(story_matches, recent_matches)
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{story['snapshot_story_id']}")
        else:
            st.info("Not enough stored history yet to draw the 3-day market chart.")

        detail_col1, detail_col2 = st.columns(2)
        for column, source_name in ((detail_col1, "polymarket"), (detail_col2, "kalshi")):
            with column:
                source_matches = story_matches[story_matches["source"] == source_name]
                st.markdown(f"**{source_name.title()}**")
                if source_matches.empty:
                    st.write("No match stored.")
                    continue

                match = source_matches.iloc[0]
                st.markdown(f"[{match['market_title']}]({match['source_url']})")
                st.write(f"Yes price: {_fmt_pct(match['yes_price'])}")
                if match.get("outcome_label"):
                    st.write(f"Outcome: {match['outcome_label']}")
                if match.get("ticker"):
                    st.caption(f"Ticker: {match['ticker']}")

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
