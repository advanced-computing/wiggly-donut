from datetime import date, datetime, timezone

import pandas as pd

from load_bq import (
    _build_stories,
    _strip_json_fences,
    build_story_baskets,
)


def test_build_story_baskets_uses_previous_selected_prices():
    headlines = pd.DataFrame(
        [
            {
                "snapshot_date": date(2026, 4, 9),
                "snapshot_story_id": "2026-04-09-01",
                "story_id": "story-1",
                "headline_rank": 1,
                "title": "Fed chair odds jump after new Trump interview",
                "news_source": "AP",
                "published_at": pd.Timestamp("2026-04-09T12:00:00Z"),
                "url": "https://example.com/story",
            }
        ]
    )

    matches = pd.DataFrame(
        [
            {
                "snapshot_story_id": "2026-04-09-01",
                "source": "polymarket",
                "selected": True,
                "market_id": "poly-1",
                "market_title": "Who will Trump nominate as Fed Chair?",
                "yes_price": 61.0,
            },
            {
                "snapshot_story_id": "2026-04-09-01",
                "source": "kalshi",
                "selected": True,
                "market_id": "kalshi-1",
                "market_title": "Who will Trump nominate as Fed Chair?",
                "yes_price": 59.0,
            },
        ]
    )

    previous_prices = pd.DataFrame(
        [
            {"source": "polymarket", "market_id": "poly-1", "yes_price": 56.0},
            {"source": "kalshi", "market_id": "kalshi-1", "yes_price": 54.0},
        ]
    )

    baskets = build_story_baskets(
        headlines=headlines,
        matches=matches,
        previous_prices=previous_prices,
        snapshot_date=date(2026, 4, 9),
        loaded_at=datetime.now(timezone.utc),
    )

    assert len(baskets) == 1
    assert baskets.loc[0, "basket_yes_price"] == 60.0
    assert baskets.loc[0, "basket_prev_yes_price"] == 55.0
    assert baskets.loc[0, "basket_change_1d"] == 5.0


def test_build_story_baskets_handles_missing_previous_prices():
    headlines = pd.DataFrame(
        [
            {
                "snapshot_date": date(2026, 4, 9),
                "snapshot_story_id": "2026-04-09-02",
                "story_id": "story-2",
                "headline_rank": 2,
                "title": "Saudi Arabia pipeline hit by drone attack",
                "news_source": "Bloomberg",
                "published_at": pd.Timestamp("2026-04-09T13:00:00Z"),
                "url": "https://example.com/story-2",
            }
        ]
    )

    matches = pd.DataFrame(
        [
            {
                "snapshot_story_id": "2026-04-09-02",
                "source": "kalshi",
                "selected": True,
                "market_id": "kalshi-2",
                "market_title": "Will oil prices jump after Middle East escalation?",
                "yes_price": 48.0,
            }
        ]
    )

    baskets = build_story_baskets(
        headlines=headlines,
        matches=matches,
        previous_prices=pd.DataFrame(columns=["source", "market_id", "yes_price"]),
        snapshot_date=date(2026, 4, 9),
        loaded_at=datetime.now(timezone.utc),
    )

    assert len(baskets) == 1
    assert baskets.loc[0, "basket_yes_price"] == 48.0
    assert pd.isna(baskets.loc[0, "basket_change_1d"])


def test_strip_json_fences_handles_markdown():
    raw = '```json\n[{"poly_index": 0, "kalshi_index": 1}]\n```'
    assert _strip_json_fences(raw) == '[{"poly_index": 0, "kalshi_index": 1}]'

    plain = '{"a": 1}'
    assert _strip_json_fences(plain) == plain


def test_build_stories_pairs_first_then_unpaired_sorted_by_change():
    def _market(mid, title, change, source):
        return {
            "market_id": mid, "title": title, "change_1d": change,
            "yes_price": 50, "no_price": 50, "source": source,
            "volume_24h": 5000, "close_time": None, "source_url": "",
            "outcome_label": "YES", "ticker": mid,
        }

    poly = [
        _market("p0", "Poly A", 12.0, "polymarket"),
        _market("p1", "Poly B", -3.0, "polymarket"),
    ]
    kalshi = [
        _market("k0", "Kalshi A", 10.0, "kalshi"),
        _market("k1", "Kalshi solo", -8.0, "kalshi"),
    ]
    pairs = [{"poly_index": 0, "kalshi_index": 0, "reason": "same outcome"}]

    stories = _build_stories(pairs, poly, kalshi, max_stories=10)

    # Paired avg |11| ranks above unpaired k1 |8| ranks above unpaired p1 |3|.
    assert stories[0]["poly"]["market_id"] == "p0"
    assert stories[0]["kalshi"]["market_id"] == "k0"
    assert stories[1]["kalshi"]["market_id"] == "k1"
    assert stories[2]["poly"]["market_id"] == "p1"
