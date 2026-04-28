import os
import sys
import time

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.append(os.path.dirname(__file__))
from data import load_recent_selected_matches, load_selected_matches, load_story_baskets

LOOKBACK_SNAPSHOTS = 7
TRENDING_TOP_N = 8

st.set_page_config(page_title="Daily News Basket", layout="wide")


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
    change = pd.to_numeric(df["basket_change_1d"], errors="coerce").abs().fillna(0)
    volume = pd.to_numeric(df["basket_volume_24h"], errors="coerce").fillna(0).clip(lower=0)
    sort_key = change * np.log1p(volume)
    return (
        df.assign(_sort_key=sort_key)
        .sort_values(
            by=["_sort_key", "matched_platform_count", "headline_rank"],
            ascending=[False, False, True],
        )
        .drop(columns="_sort_key")
    )


def _build_trending_chart(baskets: pd.DataFrame, recent_matches: pd.DataFrame) -> go.Figure | None:
    if baskets.empty or recent_matches.empty:
        return None

    top = baskets.head(TRENDING_TOP_N).reset_index(drop=True)
    market_to_label: dict[tuple, str] = {}
    label_order: list[str] = []
    for rank, story in enumerate(top.itertuples(index=False), start=1):
        title = str(story.title)
        title_short = title[:55] + ("..." if len(title) > 55 else "")
        change_pp = pd.to_numeric(pd.Series([story.basket_change_1d]), errors="coerce").iloc[0]
        change_str = f"{change_pp:+.1f}pp" if pd.notna(change_pp) else "n/a"
        paired_marker = "🤝 " if int(getattr(story, "matched_platform_count", 0) or 0) >= 2 else ""
        label = f"#{rank}  {change_str}  {paired_marker}{title_short}"
        label_order.append(label)
        for source_field, market_field in (
            ("polymarket", "polymarket_market_id"),
            ("kalshi", "kalshi_market_id"),
        ):
            mid = getattr(story, market_field, None)
            if mid:
                market_to_label[(source_field, mid)] = label

    if not market_to_label:
        return None

    history = recent_matches[
        recent_matches.apply(
            lambda r: (r["source"], r["market_id"]) in market_to_label, axis=1
        )
    ].copy()
    if history.empty:
        return None

    history["snapshot_date"] = pd.to_datetime(history["snapshot_date"], errors="coerce")
    history["story_label"] = history.apply(
        lambda r: market_to_label[(r["source"], r["market_id"])], axis=1
    )
    history = history.sort_values("snapshot_date")

    # One trace per story, plotted as cumulative pp change from the earliest
    # observed price in the lookback window. This puts every line on the same
    # zero baseline so a 92->95 move and a 50->53 move look identical
    # (same magnitude), and a 30pp swing visually dominates a 3pp drift.
    fig = go.Figure()
    for label, group in history.groupby("story_label"):
        daily = (
            group.groupby("snapshot_date", as_index=False)["yes_price"]
            .mean()
            .sort_values("snapshot_date")
        )
        if daily.empty:
            continue
        baseline = daily["yes_price"].iloc[0]
        daily["pp_change"] = daily["yes_price"] - baseline
        fig.add_trace(
            go.Scatter(
                x=daily["snapshot_date"],
                y=daily["pp_change"],
                mode="lines+markers",
                name=label,
                hovertemplate=(
                    "%{y:+.1f} pp from baseline<br>%{x|%Y-%m-%d}"
                    "<extra>%{fullData.name}</extra>"
                ),
            )
        )

    fig.add_hline(y=0, line_dash="dot", line_color="#9ca3af")
    fig.update_layout(
        height=420,
        margin={"l": 0, "r": 0, "t": 30, "b": 0},
        yaxis_title="Cumulative change (pp)",
        xaxis_title="Snapshot date",
        legend={"orientation": "h", "yanchor": "top", "y": -0.25},
    )
    return fig


def _build_history_chart(
    story_matches: pd.DataFrame,
    recent_matches: pd.DataFrame,
) -> go.Figure | None:
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
    pivot["basket"] = pivot[
        [col for col in ["polymarket", "kalshi"] if col in pivot.columns]
    ].mean(axis=1)

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

st.title("📊 Daily News Basket")
st.caption(
    "Cross-platform prediction-market basket for the most-trending politics & geopolitics stories."
)

baskets = load_story_baskets()
matches = load_selected_matches()
recent_matches = load_recent_selected_matches(snapshot_count=LOOKBACK_SNAPSHOTS)

if baskets.empty:
    st.warning("No daily basket snapshot found in BigQuery yet. Run `python load_bq.py` first.")
    st.stop()

baskets = _sort_baskets(baskets)
snapshot_date = baskets["snapshot_date"].iloc[0]

# === Top panel: trending chart ===
st.subheader("🔥 Top trending markets — last 7 days")
trending_fig = _build_trending_chart(baskets, recent_matches)
if trending_fig is not None:
    st.plotly_chart(trending_fig, use_container_width=True, key="trending_chart")
else:
    st.info(
        "Not enough stored history yet to draw the trending chart. "
        "Run the daily ETL for a few days to populate the time-series."
    )

st.divider()


def _render_card(story: pd.Series, *, key_prefix: str) -> None:
    with st.container(border=True):
        title = str(story["title"])
        is_paired = int(story.get("matched_platform_count") or 0) >= 2
        prefix = "🤝 " if is_paired else ""
        if story.get("url"):
            st.markdown(f"#### {prefix}[{title}]({story['url']})")
        else:
            st.markdown(f"#### {prefix}{title}")

        caption_bits = []
        if story.get("news_source"):
            caption_bits.append(str(story["news_source"]))
        published_at = story.get("published_at")
        if pd.notna(published_at):
            caption_bits.append(f"{published_at:%Y-%m-%d %H:%M UTC}")
        if caption_bits:
            st.caption(" • ".join(caption_bits))

        description = story.get("description")
        if isinstance(description, str) and description.strip():
            st.write(description)

        m1, m2, m3 = st.columns(3)
        m1.metric("Basket", _fmt_pct(story["basket_yes_price"]))
        m2.metric("Change", _fmt_change(story["basket_change_1d"]))
        m3.metric("Platforms", int(story["matched_platform_count"]))

        with st.expander("Markets behind this story"):
            story_matches = matches[matches["snapshot_story_id"] == story["snapshot_story_id"]]
            for source_name in ("polymarket", "kalshi"):
                source_rows = story_matches[story_matches["source"] == source_name]
                if source_rows.empty:
                    continue
                m = source_rows.iloc[0]
                st.markdown(f"**{source_name.title()}** — [{m['market_title']}]({m['source_url']})")
                st.caption(
                    f"Yes {_fmt_pct(m['yes_price'])}"
                    + (f" • Ticker {m['ticker']}" if m.get("ticker") else "")
                )


def _render_card_grid(rows: pd.DataFrame, *, key_prefix: str, cols_per_row: int = 3) -> None:
    if rows.empty:
        return
    rows = rows.reset_index(drop=True)
    n = len(rows)
    for row_start in range(0, n, cols_per_row):
        cols = st.columns(cols_per_row)
        for offset in range(cols_per_row):
            idx = row_start + offset
            if idx >= n:
                break
            with cols[offset]:
                _render_card(rows.iloc[idx], key_prefix=f"{key_prefix}_{idx}")


# === Top movers panel: 15 highest volume-weighted movers ===
TOP_N = 15
top_movers = baskets.head(TOP_N)
n_paired = int((top_movers["matched_platform_count"] >= 2).sum())
st.subheader(f"📈 Top {len(top_movers)} stories with most movement")
st.caption(
    f"Latest stored snapshot: **{snapshot_date}**. "
    f"{n_paired} of {len(top_movers)} are cross-platform pairs (🤝 badge). "
    "Ranked by |1-day change in pp| × log(1 + 24h volume) — penalizes thin markets."
)
_render_card_grid(top_movers, key_prefix="top")

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
