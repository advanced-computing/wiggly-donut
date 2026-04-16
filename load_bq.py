import argparse
import hashlib
import json
import os
import toml
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import pandas_gbq
import requests
from google import genai
from google.oauth2.service_account import Credentials

PROJECT_ID = "sipa-adv-c-wiggly-donut"
DATASET_ID = "2444_n"
TABLE_NAME_HEADLINES = "daily_headlines"
TABLE_NAME_MATCHES = "daily_market_matches"
TABLE_NAME_BASKETS = "daily_story_baskets"

NEWS_API_BASE = "https://newsapi.org/v2/top-headlines"
ATTENA_SEARCH_BASE = "https://attena-api.fly.dev/api/search/"
DEFAULT_HEADLINE_LIMIT = 70
DEFAULT_MATCH_LIMIT = 5
NEWSAPI_TOPUP_CATEGORIES = ["business", "technology", "science", "sports", "health"]

GEMINI_MODEL = "gemini-3.1-flash-lite-preview"
GEMINI_LOCATION = "global"

SEARCH_QUERY_PROMPT = """\
You are helping match news headlines to prediction market contracts on Polymarket and Kalshi.

For each headline below, generate a SHORT search query (2-6 words) that would find
directly related prediction market contracts. Focus on:
- The core event, person, or entity (NOT the news outlet name)
- Named people, companies, countries, or specific events
- Terms a prediction market would use (elections, prices, outcomes)

If a headline is about something that would NEVER have a prediction market
(e.g. recipes, lifestyle tips, obituaries, human interest stories, movie reviews,
sports game recaps), return null for that entry.

Return a JSON array with one entry per headline, in the same order.
Each entry is either a search query string or null.

Headlines:
{headlines_json}

Respond ONLY with the JSON array, no markdown fences.
"""

MATCH_VALIDATION_PROMPT = """\
You are evaluating whether prediction market contracts are DIRECTLY related to a news headline.

A match is DIRECT only if:
- The market is about the SAME specific event, person, or outcome in the headline
- The headline's news would materially affect the market's probability
- A reasonable person would say "this market is about this story"

A match is NOT direct if:
- The market is only tangentially related (same broad topic but different specific event)
- The connection requires multiple logical leaps
- They merely share a keyword (e.g. headline about NBA playoffs -> market about NBA MVP)
- The market is about a completely different timeframe or context

Headline: {headline_title}
Description: {headline_description}

Candidate markets:
{candidates_json}

For each candidate, respond with a JSON array of objects:
[{{"index": 0, "is_direct_match": true, "reason": "one sentence"}}, ...]

Respond ONLY with the JSON array, no markdown fences.
"""

COHERENCE_CHECK_PROMPT = """\
You are checking whether two prediction market contracts from different platforms
measure the SAME specific outcome for a news headline.

They are coherent if:
- Both markets bet on the same person, entity, or specific outcome
- Averaging their yes-prices would be a meaningful comparison
- Example: "Will Biden win 2024?" on Polymarket and "Biden 2024 presidency" on Kalshi = coherent

They are NOT coherent if:
- They are about the same broader event but different specific outcomes
- Example: "Tom Steyer wins CA governor" vs "Eric Swalwell wins CA governor" = NOT coherent
- Example: "S&P 500 above 6000" vs "Nasdaq above 20000" = NOT coherent

Headline: {headline_title}

Polymarket match:
  Title: {poly_title}
  Outcome: {poly_outcome}
  Ticker: {poly_ticker}

Kalshi match:
  Title: {kalshi_title}
  Outcome: {kalshi_outcome}
  Ticker: {kalshi_ticker}

Respond with a JSON object:
{{"coherent": true/false, "reason": "one sentence", "refined_query": "a better 2-6 word search query to find a matching market on the weaker platform, or null if coherent"}}

Respond ONLY with the JSON object, no markdown fences.
"""


def _load_local_secrets() -> dict:
    secrets_path = Path(__file__).resolve().parent / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return {}
    with open(secrets_path, "r", encoding="utf-8") as handle:
        return toml.load(handle)


def _get_news_api_key() -> str:
    if os.getenv("NEWSAPI_API_KEY"):
        return os.environ["NEWSAPI_API_KEY"]

    secrets = _load_local_secrets()
    news_section = secrets.get("newsapi", {})
    key = news_section.get("api_key") or news_section.get("api_token")
    if key:
        return key

    raise RuntimeError(
        "Missing NewsAPI key. Set NEWSAPI_API_KEY or add [newsapi].api_key "
        "to .streamlit/secrets.toml."
    )


def _get_credentials() -> Optional[Credentials]:
    if os.getenv("GCP_SERVICE_ACCOUNT_JSON"):
        info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
        return Credentials.from_service_account_info(info)

    secrets = _load_local_secrets()
    service_account = secrets.get("gcp_service_account")
    if service_account:
        return Credentials.from_service_account_info(dict(service_account))

    return None


def _get_gemini_client() -> genai.Client:
    """Return a Vertex AI Gemini client using the project service account."""
    creds = _get_credentials()
    if creds is not None:
        creds = creds.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
    return genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location=GEMINI_LOCATION,
        credentials=creds,
    )


def _read_gbq(query: str) -> pd.DataFrame:
    credentials = _get_credentials()
    kwargs = {"project_id": PROJECT_ID}
    if credentials is not None:
        kwargs["credentials"] = credentials
    return pandas_gbq.read_gbq(query, **kwargs)


def _write_to_bq(df: pd.DataFrame, table_name: str) -> None:
    if df.empty:
        print(f"Skipping upload for {table_name}: no rows.")
        return

    credentials = _get_credentials()
    kwargs = {
        "destination_table": f"{DATASET_ID}.{table_name}",
        "project_id": PROJECT_ID,
        "if_exists": "append",
    }
    if credentials is not None:
        kwargs["credentials"] = credentials

    print(f"Uploading {len(df)} rows to {DATASET_ID}.{table_name}...")
    try:
        pandas_gbq.to_gbq(df, **kwargs)
    except Exception as exc:
        error_text = str(exc)
        if "bigquery.tables.create denied" in error_text or "Permission bigquery.tables.create denied" in error_text:
            raise RuntimeError(
                f"BigQuery can read dataset {DATASET_ID}, but it cannot create table {table_name}. "
                "Create the table first or grant the service account permission to create tables."
            ) from exc
        raise
    print("Upload complete.")


def _story_id(url: Optional[str], title: Optional[str]) -> str:
    base = (url or title or "").strip().lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def _clamp_percent(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(max(1.0, min(99.0, value)), 2)


def _simulation_step(story_id: str, source: str) -> float:
    digest = hashlib.sha1(f"{story_id}:{source}".encode("utf-8")).hexdigest()
    seed = int(digest[:8], 16)
    direction = 1 if seed % 2 == 0 else -1
    magnitude = 1.25 + ((seed // 7) % 350) / 100
    return round(direction * magnitude, 2)


def build_search_query(title: Optional[str], description: Optional[str] = None) -> str:
    """Fallback search query builder (used when Gemini is unavailable)."""
    return (title or description or "").strip()


def generate_search_queries(headlines: list) -> list:
    """Use Gemini to generate clean prediction-market search queries for each headline.

    Returns a list the same length as *headlines* where each element is either
    a short query string or None (meaning no market is expected).
    """
    titled = [
        {"rank": h["headline_rank"], "title": h["title"]}
        for h in headlines
    ]
    client = _get_gemini_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=SEARCH_QUERY_PROMPT.format(
            headlines_json=json.dumps(titled, indent=2)
        ),
    )
    raw = response.text.strip()
    # Strip markdown fences if the model added them
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw[:raw.rfind("```")]
    try:
        queries = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  WARNING: Gemini returned unparseable JSON for search queries, falling back")
        queries = [build_search_query(h["title"]) for h in headlines]
    # Ensure length matches
    if len(queries) != len(headlines):
        print(f"  WARNING: Gemini returned {len(queries)} queries for {len(headlines)} headlines, padding")
        queries.extend([None] * (len(headlines) - len(queries)))
    return queries[:len(headlines)]


def validate_matches(headline: dict, candidates: list) -> list:
    """Use Gemini to decide which candidate markets are DIRECT matches for a headline.

    Returns a list of dicts with keys: index, is_direct_match, reason.
    """
    candidates_for_prompt = [
        {
            "index": i,
            "market_title": c.get("title", ""),
            "category": c.get("category", ""),
        }
        for i, c in enumerate(candidates)
    ]
    client = _get_gemini_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=MATCH_VALIDATION_PROMPT.format(
            headline_title=headline.get("title", ""),
            headline_description=headline.get("description", ""),
            candidates_json=json.dumps(candidates_for_prompt, indent=2),
        ),
    )
    raw = response.text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw[:raw.rfind("```")]
    try:
        verdicts = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  WARNING: Gemini returned unparseable validation JSON, rejecting all")
        verdicts = [{"index": i, "is_direct_match": False, "reason": "parse error"} for i in range(len(candidates))]
    return verdicts


def check_coherence(headline: dict, poly_record: dict, kalshi_record: dict) -> dict:
    """Ask Gemini whether the Polymarket and Kalshi matches measure the same outcome.

    Returns a dict with keys: coherent, reason, refined_query.
    """
    client = _get_gemini_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=COHERENCE_CHECK_PROMPT.format(
            headline_title=headline.get("title", ""),
            poly_title=poly_record.get("market_title", ""),
            poly_outcome=poly_record.get("outcome_label", ""),
            poly_ticker=poly_record.get("ticker", ""),
            kalshi_title=kalshi_record.get("market_title", ""),
            kalshi_outcome=kalshi_record.get("outcome_label", ""),
            kalshi_ticker=kalshi_record.get("ticker", ""),
        ),
    )
    raw = response.text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw[:raw.rfind("```")]
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"  WARNING: Gemini returned unparseable coherence JSON, assuming incoherent")
        return {"coherent": False, "reason": "parse error", "refined_query": None}


def _to_percent(value: Optional[Union[float, int]]) -> Optional[float]:
    if value is None:
        return None
    value = float(value)
    return round(value * 100, 2) if value <= 1 else round(value, 2)


def _fetch_newsapi_articles(
    country: str,
    limit: int,
    category: Optional[str] = None,
) -> list[dict]:
    params = {"country": country, "pageSize": limit}
    if category:
        params["category"] = category

    response = requests.get(
        NEWS_API_BASE,
        params=params,
        headers={"X-Api-Key": _get_news_api_key()},
        timeout=20,
    )
    response.raise_for_status()
    return response.json().get("articles", [])


def get_top_headlines(
    snapshot_date: date,
    loaded_at: datetime,
    country: str = "us",
    limit: int = DEFAULT_HEADLINE_LIMIT,
) -> pd.DataFrame:
    print("Fetching top headlines from NewsAPI...")
    articles = _fetch_newsapi_articles(country=country, limit=limit)
    seen_ids: set[str] = set()
    ordered_articles: list[dict] = []

    for article in articles:
        story_id = _story_id(article.get("url"), article.get("title"))
        if story_id in seen_ids:
            continue
        seen_ids.add(story_id)
        ordered_articles.append(article)

    if len(ordered_articles) < limit:
        for category in NEWSAPI_TOPUP_CATEGORIES:
            if len(ordered_articles) >= limit:
                break
            top_up_articles = _fetch_newsapi_articles(country=country, limit=limit, category=category)
            for article in top_up_articles:
                story_id = _story_id(article.get("url"), article.get("title"))
                if story_id in seen_ids:
                    continue
                seen_ids.add(story_id)
                ordered_articles.append(article)
                if len(ordered_articles) >= limit:
                    break

    records = []
    for rank, article in enumerate(ordered_articles[:limit], start=1):
        title = article.get("title")
        description = article.get("description")
        story_id = _story_id(article.get("url"), title)
        records.append(
            {
                "snapshot_date": snapshot_date,
                "loaded_at": loaded_at,
                "snapshot_story_id": f"{snapshot_date.isoformat()}-{rank:02d}",
                "story_id": story_id,
                "headline_rank": rank,
                "title": title,
                "description": description,
                "content": article.get("content"),
                "news_source": article.get("source", {}).get("name"),
                "published_at": pd.to_datetime(article.get("publishedAt"), utc=True, errors="coerce"),
                "url": article.get("url"),
                "image_url": article.get("urlToImage"),
                "search_query": build_search_query(title, description),
            }
        )

    return pd.DataFrame(records)


def search_attena(query: str, source: str, limit: int = DEFAULT_MATCH_LIMIT) -> list[dict]:
    if not query:
        return []

    response = requests.get(
        ATTENA_SEARCH_BASE,
        params={"q": query, "source": source, "limit": limit},
        timeout=20,
    )
    response.raise_for_status()
    return response.json().get("results", [])


def _score_candidate(candidate: dict, candidate_rank: int) -> tuple:
    """Score a candidate by Attena rank and trading volume."""
    volume_24h = float(candidate.get("volume_24h") or 0)
    rank_bonus = 1 / max(float(candidate.get("rank") or candidate_rank), 1)
    volume_bonus = min(volume_24h / 1_000_000, 5) / 10
    score = rank_bonus + volume_bonus
    return round(score, 4), 0


def build_market_matches(
    headlines: pd.DataFrame,
    snapshot_date: date,
    loaded_at: datetime,
    limit_per_source: int = DEFAULT_MATCH_LIMIT,
) -> pd.DataFrame:
    headline_list = headlines.to_dict("records")
    records: list = []

    # --- STEP 1: Batch-generate search queries via Gemini ---
    print("Generating search queries with Gemini...")
    search_queries = generate_search_queries(headline_list)
    for h, q in zip(headline_list, search_queries):
        label = q if q else "(skipped)"
        print(f"  #{h['headline_rank']}: {h['title'][:60]}...  ->  {label}")

    for headline, query in zip(headline_list, search_queries):
        if query is None:
            print(f"  Skipping headline #{headline['headline_rank']}: no market expected")
            continue

        # Store the Gemini-generated query
        headline["search_query"] = query

        # Gather candidates from both platforms
        all_candidates = []
        for source in ("polymarket", "kalshi"):
            print(
                f"  Searching Attena for #{headline['headline_rank']} on {source}: {query}"
            )
            candidates = search_attena(query, source, limit=limit_per_source)
            for c in candidates:
                c["_source"] = source
            all_candidates.extend(candidates)

        if not all_candidates:
            continue

        # --- STEP 2: Validate candidates with Gemini ---
        print(f"  Validating {len(all_candidates)} candidates with Gemini...")
        verdicts = validate_matches(headline, all_candidates)
        verdict_lookup = {}
        for v in verdicts:
            idx = v.get("index")
            if idx is not None:
                verdict_lookup[idx] = v

        # Build records for each candidate, per source
        candidates_by_source = {}
        for i, candidate in enumerate(all_candidates):
            source = candidate.pop("_source")
            is_direct = verdict_lookup.get(i, {}).get("is_direct_match", False)
            reason = verdict_lookup.get(i, {}).get("reason", "")
            candidate_rank = (i % limit_per_source) + 1
            match_score, overlap = _score_candidate(candidate, candidate_rank)

            record = {
                "snapshot_date": snapshot_date,
                "loaded_at": loaded_at,
                "snapshot_story_id": headline["snapshot_story_id"],
                "story_id": headline["story_id"],
                "headline_rank": headline["headline_rank"],
                "headline_title": headline["title"],
                "search_query": query,
                "source": source,
                "candidate_rank": candidate_rank,
                "attena_rank": float(candidate.get("rank") or candidate_rank),
                "token_overlap": overlap,
                "match_score": match_score,
                "market_id": candidate.get("market_id"),
                "market_title": candidate.get("title"),
                "yes_price": _to_percent(candidate.get("yes_price")),
                "no_price": _to_percent(candidate.get("no_price")),
                "volume": float(candidate.get("volume") or 0),
                "volume_24h": float(candidate.get("volume_24h") or 0),
                "category": candidate.get("category"),
                "subcategory": candidate.get("subcategory"),
                "event_date": pd.to_datetime(
                    candidate.get("event_date"), errors="coerce"
                ).date()
                if candidate.get("event_date")
                else None,
                "close_time": pd.to_datetime(
                    candidate.get("close_time"), utc=True, errors="coerce"
                ),
                "source_url": candidate.get("source_url"),
                "ticker": candidate.get("ticker"),
                "outcome_label": candidate.get("outcome_label"),
                "bracket_count": int(candidate.get("bracket_count") or 1),
                "status": candidate.get("status"),
                "selected": False,
                "gemini_direct_match": is_direct,
                "gemini_match_reason": reason,
            }

            records.append(record)
            candidates_by_source.setdefault(source, []).append(record)

        # Select the best DIRECT match per source; if none are direct, skip
        for source, source_records in candidates_by_source.items():
            direct_records = [r for r in source_records if r["gemini_direct_match"]]
            if not direct_records:
                continue
            best = max(
                direct_records,
                key=lambda r: (r["match_score"], -r["candidate_rank"]),
            )
            best["selected"] = True

        # --- STEP 3: Cross-platform coherence check ---
        selected_by_source = {
            source: next((r for r in recs if r["selected"]), None)
            for source, recs in candidates_by_source.items()
        }
        poly_sel = selected_by_source.get("polymarket")
        kalshi_sel = selected_by_source.get("kalshi")

        if poly_sel and kalshi_sel:
            print(f"  Checking coherence: '{poly_sel['market_title']}' vs '{kalshi_sel['market_title']}'")
            coherence = check_coherence(headline, poly_sel, kalshi_sel)

            if not coherence.get("coherent", True):
                print(f"  NOT coherent: {coherence.get('reason', '')}")
                refined_query = coherence.get("refined_query")

                if refined_query:
                    # Decide which platform to retry: keep the higher-volume match
                    if poly_sel["volume_24h"] >= kalshi_sel["volume_24h"]:
                        retry_source = "kalshi"
                        anchor_title = poly_sel["market_title"]
                    else:
                        retry_source = "polymarket"
                        anchor_title = kalshi_sel["market_title"]

                    print(f"  Retrying {retry_source} with refined query: {refined_query}")
                    retry_candidates = search_attena(refined_query, retry_source, limit=limit_per_source)

                    if retry_candidates:
                        for c in retry_candidates:
                            c["_source"] = retry_source
                        print(f"  Validating {len(retry_candidates)} retry candidates...")
                        retry_verdicts = validate_matches(headline, retry_candidates)
                        retry_verdict_lookup = {}
                        for v in retry_verdicts:
                            idx = v.get("index")
                            if idx is not None:
                                retry_verdict_lookup[idx] = v

                        # Deselect the old match on the retry platform
                        old_sel = selected_by_source[retry_source]
                        old_sel["selected"] = False

                        # Build records and pick the best direct match
                        best_retry = None
                        for i, candidate in enumerate(retry_candidates):
                            source = candidate.pop("_source")
                            is_direct = retry_verdict_lookup.get(i, {}).get("is_direct_match", False)
                            reason = retry_verdict_lookup.get(i, {}).get("reason", "")
                            c_rank = i + 1
                            m_score, m_overlap = _score_candidate(candidate, c_rank)

                            retry_record = {
                                "snapshot_date": snapshot_date,
                                "loaded_at": loaded_at,
                                "snapshot_story_id": headline["snapshot_story_id"],
                                "story_id": headline["story_id"],
                                "headline_rank": headline["headline_rank"],
                                "headline_title": headline["title"],
                                "search_query": refined_query,
                                "source": source,
                                "candidate_rank": c_rank,
                                "attena_rank": float(candidate.get("rank") or c_rank),
                                "token_overlap": m_overlap,
                                "match_score": m_score,
                                "market_id": candidate.get("market_id"),
                                "market_title": candidate.get("title"),
                                "yes_price": _to_percent(candidate.get("yes_price")),
                                "no_price": _to_percent(candidate.get("no_price")),
                                "volume": float(candidate.get("volume") or 0),
                                "volume_24h": float(candidate.get("volume_24h") or 0),
                                "category": candidate.get("category"),
                                "subcategory": candidate.get("subcategory"),
                                "event_date": pd.to_datetime(
                                    candidate.get("event_date"), errors="coerce"
                                ).date()
                                if candidate.get("event_date")
                                else None,
                                "close_time": pd.to_datetime(
                                    candidate.get("close_time"), utc=True, errors="coerce"
                                ),
                                "source_url": candidate.get("source_url"),
                                "ticker": candidate.get("ticker"),
                                "outcome_label": candidate.get("outcome_label"),
                                "bracket_count": int(candidate.get("bracket_count") or 1),
                                "status": candidate.get("status"),
                                "selected": False,
                                "gemini_direct_match": is_direct,
                                "gemini_match_reason": reason,
                            }
                            records.append(retry_record)
                            if is_direct and (best_retry is None or m_score > best_retry["match_score"]):
                                best_retry = retry_record

                        if best_retry:
                            best_retry["selected"] = True
                            print(f"  Retry selected: {best_retry['market_title']}")
                        else:
                            print(f"  Retry found no direct match, keeping only single-platform match")
                    else:
                        print(f"  Retry returned no candidates")
            else:
                print(f"  Coherent: {coherence.get('reason', '')}")

    return pd.DataFrame(records)


def get_previous_selected_prices(snapshot_date: date) -> pd.DataFrame:
    query = f"""
        WITH dedup AS (
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_MATCHES}`
            WHERE selected = TRUE
              AND snapshot_date < '{snapshot_date.isoformat()}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, source, market_id
                ORDER BY loaded_at DESC
            ) = 1
        )
        SELECT source, market_id, yes_price, snapshot_date
        FROM dedup
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY source, market_id
            ORDER BY snapshot_date DESC
        ) = 1
    """

    try:
        return _read_gbq(query)
    except Exception:
        return pd.DataFrame(columns=["source", "market_id", "yes_price", "snapshot_date"])


def build_story_baskets(
    headlines: pd.DataFrame,
    matches: pd.DataFrame,
    previous_prices: pd.DataFrame,
    snapshot_date: date,
    loaded_at: datetime,
) -> pd.DataFrame:
    selected_matches = matches[matches["selected"]].copy() if not matches.empty else pd.DataFrame()
    previous_lookup = {
        (row["source"], row["market_id"]): row["yes_price"]
        for row in previous_prices.to_dict("records")
    }

    records = []
    for headline in headlines.to_dict("records"):
        current_matches = (
            selected_matches[selected_matches["snapshot_story_id"] == headline["snapshot_story_id"]]
            if not selected_matches.empty
            else pd.DataFrame()
        )
        matches_by_source = {
            row["source"]: row for row in current_matches.to_dict("records")
        }

        poly = matches_by_source.get("polymarket")
        kalshi = matches_by_source.get("kalshi")

        current_prices = [
            match["yes_price"]
            for match in (poly, kalshi)
            if match and match.get("yes_price") is not None
        ]
        compared_current_prices = []
        previous_platform_prices = []

        for source, match in (("polymarket", poly), ("kalshi", kalshi)):
            if not match:
                continue
            previous_price = previous_lookup.get((source, match["market_id"]))
            if previous_price is None or match.get("yes_price") is None:
                continue
            compared_current_prices.append(match["yes_price"])
            previous_platform_prices.append(previous_price)

        basket_yes_price = round(sum(current_prices) / len(current_prices), 2) if current_prices else None
        basket_prev_yes_price = (
            round(sum(previous_platform_prices) / len(previous_platform_prices), 2)
            if previous_platform_prices
            else None
        )
        basket_change = (
            round(
                (sum(compared_current_prices) / len(compared_current_prices))
                - (sum(previous_platform_prices) / len(previous_platform_prices)),
                2,
            )
            if previous_platform_prices
            else None
        )

        records.append(
            {
                "snapshot_date": snapshot_date,
                "loaded_at": loaded_at,
                "snapshot_story_id": headline["snapshot_story_id"],
                "story_id": headline["story_id"],
                "headline_rank": headline["headline_rank"],
                "title": headline["title"],
                "news_source": headline["news_source"],
                "published_at": headline["published_at"],
                "url": headline["url"],
                "polymarket_market_id": poly["market_id"] if poly else None,
                "polymarket_title": poly["market_title"] if poly else None,
                "polymarket_yes_price": poly["yes_price"] if poly else None,
                "polymarket_prev_yes_price": previous_lookup.get(("polymarket", poly["market_id"]))
                if poly
                else None,
                "polymarket_change_1d": (
                    round(poly["yes_price"] - previous_lookup[("polymarket", poly["market_id"])], 2)
                    if poly and ("polymarket", poly["market_id"]) in previous_lookup
                    else None
                ),
                "kalshi_market_id": kalshi["market_id"] if kalshi else None,
                "kalshi_title": kalshi["market_title"] if kalshi else None,
                "kalshi_yes_price": kalshi["yes_price"] if kalshi else None,
                "kalshi_prev_yes_price": previous_lookup.get(("kalshi", kalshi["market_id"]))
                if kalshi
                else None,
                "kalshi_change_1d": (
                    round(kalshi["yes_price"] - previous_lookup[("kalshi", kalshi["market_id"])], 2)
                    if kalshi and ("kalshi", kalshi["market_id"]) in previous_lookup
                    else None
                ),
                "matched_platform_count": len(current_prices),
                "change_platform_count": len(previous_platform_prices),
                "complete_basket": len(current_prices) == 2,
                "basket_yes_price": basket_yes_price,
                "basket_prev_yes_price": basket_prev_yes_price,
                "basket_change_1d": basket_change,
            }
        )

    baskets = pd.DataFrame(records)
    if baskets.empty:
        return baskets

    sort_key = pd.to_numeric(baskets["basket_change_1d"], errors="coerce").abs().fillna(-1)
    sorted_story_ids = (
        baskets.assign(_sort_key=sort_key)
        .sort_values(
            by=["_sort_key", "matched_platform_count", "headline_rank"],
            ascending=[False, False, True],
        )["snapshot_story_id"]
        .tolist()
    )
    rank_lookup = {story_id: idx for idx, story_id in enumerate(sorted_story_ids, start=1)}
    baskets["rank_by_abs_change"] = baskets["snapshot_story_id"].map(rank_lookup)
    return baskets


def _clone_headlines_for_date(
    headlines: pd.DataFrame,
    snapshot_date: date,
    loaded_at: datetime,
) -> pd.DataFrame:
    cloned = headlines.copy()
    cloned["snapshot_date"] = snapshot_date
    cloned["loaded_at"] = loaded_at
    cloned["snapshot_story_id"] = cloned["headline_rank"].apply(
        lambda rank: f"{snapshot_date.isoformat()}-{int(rank):02d}"
    )
    return cloned


def _clone_matches_for_date(
    matches: pd.DataFrame,
    headlines_for_date: pd.DataFrame,
    snapshot_date: date,
    loaded_at: datetime,
    day_offset: int,
) -> pd.DataFrame:
    cloned = matches.copy()
    story_lookup = {
        row["story_id"]: row["snapshot_story_id"]
        for row in headlines_for_date[["story_id", "snapshot_story_id"]].to_dict("records")
    }
    cloned["snapshot_date"] = snapshot_date
    cloned["loaded_at"] = loaded_at
    cloned["snapshot_story_id"] = cloned["story_id"].map(story_lookup)

    def adjust_yes(row: pd.Series) -> Optional[float]:
        base_price = row.get("yes_price")
        if base_price is None or pd.isna(base_price):
            return None
        adjustment = _simulation_step(row["story_id"], row["source"]) * day_offset
        return _clamp_percent(float(base_price) - adjustment)

    cloned["yes_price"] = cloned.apply(adjust_yes, axis=1)
    cloned["no_price"] = cloned["yes_price"].apply(
        lambda price: None if price is None or pd.isna(price) else round(100 - price, 2)
    )
    return cloned


def run_simulated_snapshot_series(
    end_date: date,
    limit: int = DEFAULT_HEADLINE_LIMIT,
    simulate_days: int = 3,
) -> None:
    loaded_at = datetime.now(timezone.utc)
    headlines = get_top_headlines(snapshot_date=end_date, loaded_at=loaded_at, limit=limit)
    if headlines.empty:
        raise RuntimeError("NewsAPI returned no headlines.")

    matches = build_market_matches(headlines, snapshot_date=end_date, loaded_at=loaded_at)
    dates = [end_date - timedelta(days=days_back) for days_back in range(simulate_days - 1, -1, -1)]
    previous_prices = pd.DataFrame(columns=["source", "market_id", "yes_price", "snapshot_date"])

    for offset_index, snapshot_day in enumerate(dates):
        day_offset = (end_date - snapshot_day).days
        day_loaded_at = loaded_at + timedelta(seconds=offset_index)
        day_headlines = _clone_headlines_for_date(headlines, snapshot_day, day_loaded_at)
        day_matches = _clone_matches_for_date(
            matches=matches,
            headlines_for_date=day_headlines,
            snapshot_date=snapshot_day,
            loaded_at=day_loaded_at,
            day_offset=day_offset,
        )
        day_baskets = build_story_baskets(
            headlines=day_headlines,
            matches=day_matches,
            previous_prices=previous_prices,
            snapshot_date=snapshot_day,
            loaded_at=day_loaded_at,
        )

        _write_to_bq(day_headlines, TABLE_NAME_HEADLINES)
        _write_to_bq(day_matches, TABLE_NAME_MATCHES)
        _write_to_bq(day_baskets, TABLE_NAME_BASKETS)

        previous_prices = day_matches[day_matches["selected"]][
            ["source", "market_id", "yes_price"]
        ].copy()


def run_daily_snapshot(snapshot_date: date, limit: int = DEFAULT_HEADLINE_LIMIT) -> None:
    loaded_at = datetime.now(timezone.utc)

    headlines = get_top_headlines(snapshot_date=snapshot_date, loaded_at=loaded_at, limit=limit)
    if headlines.empty:
        raise RuntimeError("NewsAPI returned no headlines.")

    previous_prices = get_previous_selected_prices(snapshot_date)
    matches = build_market_matches(headlines, snapshot_date=snapshot_date, loaded_at=loaded_at)
    baskets = build_story_baskets(
        headlines=headlines,
        matches=matches,
        previous_prices=previous_prices,
        snapshot_date=snapshot_date,
        loaded_at=loaded_at,
    )

    _write_to_bq(headlines, TABLE_NAME_HEADLINES)
    _write_to_bq(matches, TABLE_NAME_MATCHES)
    _write_to_bq(baskets, TABLE_NAME_BASKETS)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch top headlines, match them to prediction markets, and store daily baskets."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Snapshot date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_HEADLINE_LIMIT,
        help="Number of top headlines to fetch.",
    )
    parser.add_argument(
        "--simulate-days",
        type=int,
        default=0,
        help="If set, backfill a simulated series for this many days using the current top stories.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    snapshot_date = date.fromisoformat(args.date)
    if args.simulate_days and args.simulate_days > 1:
        run_simulated_snapshot_series(
            end_date=snapshot_date,
            limit=args.limit,
            simulate_days=args.simulate_days,
        )
    else:
        run_daily_snapshot(snapshot_date=snapshot_date, limit=args.limit)
