from datetime import date, datetime, timezone

import pandas as pd

from load_bq import (
    _clone_headlines_for_date,
    _clone_matches_for_date,
    build_search_query,
    build_story_baskets,
)


def test_build_search_query_prefers_full_headline():
    title = "Fed chair odds jump after new Trump interview"
    assert build_search_query(title, "unused description") == title


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


def test_simulated_clone_offsets_snapshot_date_and_prices():
    headlines = pd.DataFrame(
        [
            {
                "snapshot_date": date(2026, 4, 9),
                "snapshot_story_id": "2026-04-09-01",
                "story_id": "story-1",
                "headline_rank": 1,
            }
        ]
    )
    matches = pd.DataFrame(
        [
            {
                "snapshot_date": date(2026, 4, 9),
                "snapshot_story_id": "2026-04-09-01",
                "story_id": "story-1",
                "source": "polymarket",
                "yes_price": 60.0,
                "no_price": 40.0,
            }
        ]
    )

    target_date = date(2026, 4, 7)
    loaded_at = datetime.now(timezone.utc)
    cloned_headlines = _clone_headlines_for_date(headlines, target_date, loaded_at)
    cloned_matches = _clone_matches_for_date(
        matches=matches,
        headlines_for_date=cloned_headlines,
        snapshot_date=target_date,
        loaded_at=loaded_at,
        day_offset=2,
    )

    assert cloned_headlines.loc[0, "snapshot_story_id"] == "2026-04-07-01"
    assert cloned_matches.loc[0, "snapshot_story_id"] == "2026-04-07-01"
    assert cloned_matches.loc[0, "yes_price"] != 60.0
