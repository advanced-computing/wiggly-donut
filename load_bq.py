import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pandas_gbq
import requests
import toml
from google import genai
from google.oauth2.service_account import Credentials

PROJECT_ID = "sipa-adv-c-wiggly-donut"
DATASET_ID = "2444_n"
TABLE_NAME_HEADLINES = "daily_headlines"
TABLE_NAME_MATCHES = "daily_market_matches"
TABLE_NAME_BASKETS = "daily_story_baskets"

POLYMARKET_GAMMA_BASE = "https://gamma-api.polymarket.com/markets"
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

GEMINI_MODEL = "gemini-3-flash-preview"  # supports Google Search grounding
GEMINI_LOCATION = "global"

# Politics / geopolitics filtering — pragmatic asymmetric design:
#   - Kalshi has a clean canonical event-level `category` field, so we use it.
#   - Polymarket has no comparable taxonomy (tags[] is empty across the universe),
#     so we delegate the editorial choice to Gemini's classifier.
KALSHI_POLITICS_CATEGORIES = {"politics", "world", "elections", "geopolitics"}
CLASSIFICATION_OVERSAMPLE = 100  # how many top movers to send to Gemini (Polymarket only)

DEFAULT_TOP_MOVERS = 50
DEFAULT_MAX_STORIES = 15
MIN_VOLUME_24H = 1000.0  # USD

PAIRING_PROMPT = """\
You are pairing prediction market contracts ACROSS Polymarket and Kalshi
that are about the SAME underlying event/person/outcome.

Two markets are a VALID pair if ALL of these are true:
1. Same specific event, person, or outcome being asked about.
2. Same direction. "X wins" and "X loses" are inverses, NOT a pair.
3. Same scope. "Next president" and "GOP nominee" are NOT a pair.

Resolution timeframes and exact thresholds may differ between the two
platforms (e.g. one closes in June, the other in December; one asks
"S&P > 6000" and the other "S&P > 6500"). That is OK — pair them anyway
if they are clearly tracking the same underlying outcome.

Polymarket markets:
{poly_json}

Kalshi markets:
{kalshi_json}

Return a JSON array of pairs. Each entry uses the integer index from each list:
[
  {{"poly_index": 0, "kalshi_index": 3, "reason": "both about X winning Y"}}
]

Respond ONLY with the JSON array, no markdown fences.
"""

POLITICS_CLASSIFICATION_PROMPT = """\
For each market title below, classify whether it concerns POLITICS or GEOPOLITICS.

POLITICS includes: US/foreign elections, primaries, candidates, party endorsements,
legislative votes, presidential or executive decisions, supreme-court / judicial outcomes,
political appointments and confirmations, political scandals.

GEOPOLITICS includes: international relations, wars, sanctions, ceasefires, diplomatic
meetings, foreign policy, regime change, military action between nations, treaties,
alliances, terrorism affecting state policy.

NOT politics/geopolitics: sports, esports, crypto / token prices, commodity prices,
weather, AI products, entertainment, science, medical, daily-life trivia, business
deals not driven by policy.

Return a JSON array of booleans, one per market in the same order:
true  = the market is about politics or geopolitics
false = anything else (including economics-without-policy, sports, crypto, etc.)

The array length MUST equal the number of markets below.

Markets:
{markets_json}

Respond ONLY with the JSON array, no markdown fences.
"""

STORY_PROMPT = """\
A prediction market just moved. Find the most relevant recent news story
that explains the move and write a headline plus a substantive summary.

Market: {market_title}
Platform: {source}
Yes price: {yes_price}%
24h change: {change_1d:+.2f} percentage points

Use Google Search to find a published news article from the last 7 days
that directly relates to this market's outcome.

Respond as a JSON object:
{{"title": "<short factual headline, under 100 chars>",
  "description": "<a 200-250 word summary covering: (1) what specifically happened, (2) the key
people, organizations, or events involved, (3) why this matters for the market's specific
outcome, and (4) what concrete development or signal to watch for next. Be factual and grounded
in the article — do not speculate beyond what is reported.>",
  "url": "<full URL to the news article>",
  "source": "<publisher name, e.g. Reuters, NYT>"}}

If no clearly relevant news is found, respond with:
{{"title": null, "description": null, "url": null, "source": null}}

Respond ONLY with the JSON object, no markdown fences.
"""


def _load_local_secrets() -> dict:
    secrets_path = Path(__file__).resolve().parent / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return {}
    with open(secrets_path, encoding="utf-8") as handle:
        return toml.load(handle)


def _get_credentials() -> Credentials | None:
    if os.getenv("GCP_SERVICE_ACCOUNT_JSON"):
        info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
        return Credentials.from_service_account_info(info)

    secrets = _load_local_secrets()
    service_account = secrets.get("gcp_service_account")
    if service_account:
        return Credentials.from_service_account_info(dict(service_account))

    return None


def _get_gemini_client() -> genai.Client:
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
    pandas_gbq.to_gbq(df, **kwargs)
    print("Upload complete.")


def _story_id(*parts: str | None) -> str:
    base = "|".join((p or "").strip().lower() for p in parts)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def _strip_json_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[: raw.rfind("```")]
    return raw.strip()


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_percent(value: float | int | None) -> float | None:
    if value is None:
        return None
    value = float(value)
    return round(value * 100, 2) if abs(value) <= 1 else round(value, 2)


def classify_politics_markets(movers: list[dict]) -> list[bool]:
    """Use Gemini to classify a list of markets as politics/geopolitics or not.

    Returns a list of booleans the same length as `movers`. On any failure
    (parse error, network error, length mismatch) we conservatively keep
    everything (return all True) so the daily run still produces output;
    a warning is logged so it surfaces in the ETL log.
    """
    if not movers:
        return []

    items = [{"index": i, "title": m["title"]} for i, m in enumerate(movers)]
    from google.genai import types as genai_types

    client = _get_gemini_client()
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=POLITICS_CLASSIFICATION_PROMPT.format(
                markets_json=json.dumps(items, indent=2),
            ),
            config=genai_types.GenerateContentConfig(
                thinking_config=genai_types.ThinkingConfig(thinking_level="low"),
            ),
        )
    except Exception as exc:
        print(f"  WARNING: politics classification call failed ({exc}); keeping all")
        return [True] * len(movers)

    raw = _strip_json_fences(response.text or "")
    try:
        verdicts = json.loads(raw)
    except json.JSONDecodeError:
        print("  WARNING: politics classification JSON unparseable; keeping all")
        return [True] * len(movers)

    if len(verdicts) != len(movers):
        print(
            f"  WARNING: classifier returned {len(verdicts)} verdicts for "
            f"{len(movers)} markets; padding with False"
        )
        verdicts = (list(verdicts) + [False] * len(movers))[: len(movers)]
    return [bool(v) for v in verdicts]


def _movement_score(change_1d_pp: float, volume_24h: float) -> float:
    """Volume-weighted movement score. abs(change_pp) * log(1 + volume).

    Penalizes thin markets that wave wildly without conviction. Bigger volume
    means more dispersed information backing the price, so a 5pp move on $5M
    deserves to outrank a 30pp move on $1k.
    """
    import math
    return abs(change_1d_pp) * math.log1p(max(volume_24h, 0.0))


def _dedupe_movers_by_event(movers: list[dict], event_key: str) -> list[dict]:
    """Keep only the top-scoring market per event (by volume-weighted score).

    Markets without a usable event key are kept as-is (each treated as its own event).
    """
    by_event: dict[str, dict] = {}
    untagged: list[dict] = []
    for m in movers:
        key = (m.get(event_key) or "").strip()
        if not key:
            untagged.append(m)
            continue
        score = _movement_score(m["change_1d"], m["volume_24h"])
        existing = by_event.get(key)
        existing_score = (
            _movement_score(existing["change_1d"], existing["volume_24h"])
            if existing
            else float("-inf")
        )
        if score > existing_score:
            by_event[key] = m
    return list(by_event.values()) + untagged


def _fetch_polymarket_pages(max_pages: int = 8, page_size: int = 100) -> list[dict]:
    markets: list[dict] = []
    for page in range(max_pages):
        params = {
            "active": "true",
            "closed": "false",
            "limit": page_size,
            "offset": page * page_size,
            "order": "volume24hr",
            "ascending": "false",
        }
        resp = requests.get(POLYMARKET_GAMMA_BASE, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        markets.extend(batch)
        if len(batch) < page_size:
            break
    return markets


def fetch_polymarket_movers(
    limit: int = DEFAULT_TOP_MOVERS,
    min_volume_24h: float = MIN_VOLUME_24H,
) -> list[dict]:
    """Pull all-active Polymarket movers, then Gemini-classify the top movers
    by volume-weighted score and keep only politics/geopolitics."""
    print("Fetching Polymarket markets (no pre-filter)...")
    markets = _fetch_polymarket_pages()

    movers: list[dict] = []
    for m in markets:
        volume_24h = _safe_float(m.get("volume24hr"))
        if volume_24h < min_volume_24h:
            continue

        last_trade = _safe_float(m.get("lastTradePrice"))
        change_1d = _safe_float(m.get("oneDayPriceChange"))
        if last_trade <= 0:
            continue

        events = m.get("events") or []
        primary_event = events[0] if events else {}
        # Parse the YES outcome's CLOB token id (first of two: YES, NO).
        clob_token_ids: list[str] = []
        raw_clob = m.get("clobTokenIds")
        if isinstance(raw_clob, str):
            try:
                clob_token_ids = json.loads(raw_clob)
            except json.JSONDecodeError:
                clob_token_ids = []
        elif isinstance(raw_clob, list):
            clob_token_ids = raw_clob
        yes_token_id = clob_token_ids[0] if clob_token_ids else None

        movers.append(
            {
                "source": "polymarket",
                "market_id": m.get("conditionId") or str(m.get("id") or ""),
                "title": m.get("question") or "",
                "yes_price": round(last_trade * 100, 2),
                "no_price": round((1 - last_trade) * 100, 2),
                "change_1d": round(change_1d * 100, 2),
                "volume_24h": round(volume_24h, 2),
                "category": primary_event.get("seriesSlug"),
                "event_ticker": primary_event.get("ticker"),
                "close_time": m.get("endDate") or primary_event.get("endDate"),
                "source_url": f"https://polymarket.com/market/{m.get('slug', '')}",
                "outcome_label": "YES",
                "ticker": m.get("conditionId"),
                "yes_token_id": yes_token_id,
            }
        )

    # Dedup by event so each event takes only one classification slot.
    deduped = _dedupe_movers_by_event(movers, event_key="event_ticker")
    deduped.sort(
        key=lambda r: _movement_score(r["change_1d"], r["volume_24h"]), reverse=True,
    )
    print(f"  {len(movers)} markets above volume floor; {len(deduped)} after event dedup")

    # Take top-N candidates and let Gemini classify which are politics.
    candidates = deduped[:CLASSIFICATION_OVERSAMPLE]
    print(f"  Classifying top {len(candidates)} Polymarket movers as politics/geopolitics...")
    keep = classify_politics_markets(candidates)
    politics = [m for m, ok in zip(candidates, keep, strict=False) if ok]
    print(f"  {len(politics)} of {len(candidates)} classified as politics/geopolitics")

    politics.sort(
        key=lambda r: _movement_score(r["change_1d"], r["volume_24h"]), reverse=True,
    )
    return politics[:limit]


def _kalshi_paginate(path: str, params: dict, max_pages: int = 15) -> list[dict]:
    """Cursor-paginate a Kalshi list endpoint with backoff on 429s."""
    import time as _time

    rows: list[dict] = []
    cursor = ""
    base_params = dict(params)
    url = f"{KALSHI_BASE}/{path}"
    for _ in range(max_pages):
        page_params = dict(base_params)
        if cursor:
            page_params["cursor"] = cursor

        backoff = 1.0
        for attempt in range(6):
            resp = requests.get(url, params=page_params, timeout=30)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", backoff))
                print(f"  Kalshi 429; sleeping {retry_after:.1f}s (attempt {attempt + 1})")
                _time.sleep(retry_after)
                backoff = min(backoff * 2, 30.0)
                continue
            resp.raise_for_status()
            break
        else:
            raise RuntimeError(f"Kalshi rate-limit retry exhausted on {path}")

        # Pace requests slightly to stay under the rate limit.
        _time.sleep(0.25)

        data = resp.json()
        for key in ("events", "markets", "data"):
            if key in data and isinstance(data[key], list):
                rows.extend(data[key])
                break
        cursor = data.get("cursor") or ""
        if not cursor:
            break
    return rows


def fetch_kalshi_movers(
    limit: int = DEFAULT_TOP_MOVERS,
    min_volume_24h: float = MIN_VOLUME_24H,
) -> list[dict]:
    """Pull Kalshi markets pre-filtered by Kalshi's own canonical event category
    (Politics / Elections / World / Geopolitics). Kalshi's taxonomy is clean
    and consistently populated, so no Gemini classifier is needed here."""
    print("Fetching Kalshi events with nested markets...")
    events = _kalshi_paginate(
        "events",
        {"status": "open", "limit": 200, "with_nested_markets": "true"},
        max_pages=25,
    )
    politics_events = [
        e for e in events
        if (e.get("category") or "").lower() in KALSHI_POLITICS_CATEGORIES
    ]
    print(
        f"  {len(politics_events)} politics events out of {len(events)} open "
        "(category in {Politics, Elections, World, Geopolitics})"
    )

    movers: list[dict] = []
    for event in politics_events:
        for m in event.get("markets") or []:
            last = _safe_float(m.get("last_price_dollars"))
            prev = _safe_float(m.get("previous_price_dollars"))
            volume_24h = _safe_float(m.get("volume_24h_fp"))
            if volume_24h < min_volume_24h:
                continue
            if last <= 0 and prev <= 0:
                continue

            change_1d = (last - prev) * 100  # dollars (0-1) -> percentage points
            ticker = m.get("ticker") or ""
            event_ticker = event.get("event_ticker")
            movers.append(
                {
                    "source": "kalshi",
                    "market_id": ticker,
                    "title": m.get("title") or event.get("title"),
                    "yes_price": round(last * 100, 2),
                    "no_price": round((1 - last) * 100, 2) if last else None,
                    "change_1d": round(change_1d, 2),
                    "volume_24h": round(volume_24h, 2),
                    "category": event.get("category"),
                    "event_ticker": event_ticker,
                    "series_ticker": event.get("series_ticker"),
                    "close_time": m.get("close_time"),
                    "source_url": f"https://kalshi.com/markets/{(event_ticker or '').lower()}",
                    "outcome_label": m.get("yes_sub_title") or "YES",
                    "ticker": ticker,
                }
            )

    # Pre-filtered by event category, so no Gemini classifier needed here.
    deduped = _dedupe_movers_by_event(movers, event_key="event_ticker")
    deduped.sort(
        key=lambda r: _movement_score(r["change_1d"], r["volume_24h"]), reverse=True,
    )
    print(f"  {len(movers)} markets above volume floor; {len(deduped)} after event dedup")
    return deduped[:limit]


def pair_markets_across_platforms(
    poly_movers: list[dict],
    kalshi_movers: list[dict],
) -> list[dict]:
    """Use Gemini to find pairs of markets across platforms that measure the same outcome."""
    if not poly_movers or not kalshi_movers:
        return []

    poly_for_prompt = [
        {
            "index": i,
            "title": m["title"],
            "yes_price": m["yes_price"],
            "close_time": m["close_time"],
        }
        for i, m in enumerate(poly_movers)
    ]
    kalshi_for_prompt = [
        {
            "index": i,
            "title": m["title"],
            "yes_price": m["yes_price"],
            "close_time": m["close_time"],
        }
        for i, m in enumerate(kalshi_movers)
    ]

    from google.genai import types as genai_types

    client = _get_gemini_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=PAIRING_PROMPT.format(
            poly_json=json.dumps(poly_for_prompt, indent=2, default=str),
            kalshi_json=json.dumps(kalshi_for_prompt, indent=2, default=str),
        ),
        config=genai_types.GenerateContentConfig(
            thinking_config=genai_types.ThinkingConfig(thinking_level="low"),
        ),
    )
    raw = _strip_json_fences(response.text)
    try:
        pairs = json.loads(raw)
    except json.JSONDecodeError:
        print("  WARNING: Gemini returned unparseable pairing JSON; no pairs")
        return []

    valid: list[dict] = []
    for p in pairs:
        try:
            poly_idx = int(p["poly_index"])
            kalshi_idx = int(p["kalshi_index"])
        except (KeyError, TypeError, ValueError):
            continue
        if 0 <= poly_idx < len(poly_movers) and 0 <= kalshi_idx < len(kalshi_movers):
            valid.append(
                {
                    "poly_index": poly_idx,
                    "kalshi_index": kalshi_idx,
                    "reason": p.get("reason", ""),
                }
            )
    return valid


def generate_story_for_pair(
    market_title: str,
    source: str,
    yes_price: float,
    change_1d: float,
) -> dict:
    """Use Gemini with Google Search grounding to find a relevant news story.

    Returns a dict with keys: title, description, url, source. The url and source
    are taken from grounding_metadata when available (verified search result),
    falling back to whatever Gemini wrote in JSON otherwise.
    """
    from google.genai import types as genai_types

    client = _get_gemini_client()
    prompt = STORY_PROMPT.format(
        market_title=market_title,
        source=source,
        yes_price=yes_price,
        change_1d=change_1d,
    )

    config = genai_types.GenerateContentConfig(
        tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
        thinking_config=genai_types.ThinkingConfig(thinking_level="low"),
        temperature=0.2,
    )
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=config,
        )
    except Exception as exc:
        print(f"  Grounded story call failed ({exc})")
        return {"title": None, "description": None, "url": None, "source": None}

    raw = _strip_json_fences(response.text or "")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = {"title": None, "description": None, "url": None, "source": None}

    # Prefer grounding_metadata for url/source — those are real search results.
    # Only fall back to Gemini's JSON-claimed url if grounding returned nothing.
    candidates = response.candidates or []
    gm = candidates[0].grounding_metadata if candidates else None
    chunks = (gm.grounding_chunks if gm else None) or []
    if chunks:
        first = chunks[0].web
        if first and first.uri:
            parsed["url"] = first.uri
            parsed["source"] = first.title or parsed.get("source")
    return parsed


def _generate_headlines_parallel(stories: list[dict], max_workers: int = 5) -> None:
    """Populate s['headline'] for each story by calling Gemini in parallel.

    Streams a one-line progress event per call so you can see grounding land
    in real time (and verify it's actually returning real news, not nulls).
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _one(idx: int, s: dict) -> tuple[int, dict, float]:
        t0 = _time.time()
        primary = s["poly"] or s["kalshi"]
        changes = [mv["change_1d"] for mv in (s["poly"], s["kalshi"]) if mv is not None]
        avg_change = sum(changes) / len(changes) if changes else 0.0
        headline = generate_story_for_pair(
            market_title=primary["title"],
            source="both" if (s["poly"] and s["kalshi"]) else primary["source"],
            yes_price=primary["yes_price"],
            change_1d=avg_change,
        )
        return idx, headline, _time.time() - t0

    print(
        f"Generating {len(stories)} headlines via Gemini grounded search "
        f"({max_workers}-way parallel)...",
        flush=True,
    )
    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_one, i, s): (i, s) for i, s in enumerate(stories)}
        for fut in as_completed(futures):
            idx, headline, dt = fut.result()
            stories[idx]["headline"] = headline
            completed += 1
            title = (headline.get("title") or "")[:55]
            src = headline.get("source") or "no-source"
            url = (headline.get("url") or "")[:50]
            status = f"-> {title!r} [{src}]" if title else "-> (no story found)"
            print(
                f"  [{completed:>2}/{len(stories)}] {dt:5.1f}s  {status}  {url}",
                flush=True,
            )


def _build_stories(
    pairs: list[dict],
    poly_movers: list[dict],
    kalshi_movers: list[dict],
    max_stories: int,
) -> list[dict]:
    """Combine paired and unpaired movers into a ranked story list.

    Cross-platform pairs are ALWAYS included (they are our strongest signal that
    something real moved — two independent venues agreed). Remaining slots are
    filled with the highest volume-weighted movers from either platform.
    """
    paired_poly = {p["poly_index"] for p in pairs}
    paired_kalshi = {p["kalshi_index"] for p in pairs}

    paired_stories: list[dict] = [
        {
            "poly": poly_movers[p["poly_index"]],
            "kalshi": kalshi_movers[p["kalshi_index"]],
            "pair_reason": p.get("reason", ""),
        }
        for p in pairs
    ]

    unpaired_stories: list[dict] = []
    for i, m in enumerate(poly_movers):
        if i not in paired_poly:
            unpaired_stories.append({"poly": m, "kalshi": None, "pair_reason": ""})
    for i, m in enumerate(kalshi_movers):
        if i not in paired_kalshi:
            unpaired_stories.append({"poly": None, "kalshi": m, "pair_reason": ""})

    def avg_score(s: dict) -> float:
        scores = [
            _movement_score(m["change_1d"], m["volume_24h"])
            for m in (s["poly"], s["kalshi"])
            if m is not None
        ]
        return sum(scores) / len(scores) if scores else 0.0

    paired_stories.sort(key=avg_score, reverse=True)
    unpaired_stories.sort(key=avg_score, reverse=True)

    if len(paired_stories) >= max_stories:
        return paired_stories[:max_stories]
    needed = max_stories - len(paired_stories)
    return paired_stories + unpaired_stories[:needed]


def _stories_to_headlines(
    stories: list[dict],
    snapshot_date: date,
    loaded_at: datetime,
) -> pd.DataFrame:
    rows = []
    for rank, s in enumerate(stories, start=1):
        primary = s["poly"] or s["kalshi"]
        headline = s.get("headline") or {}
        title = headline.get("title") or primary["title"]
        description = headline.get("description")
        url = headline.get("url") or primary["source_url"]
        news_source = headline.get("source")
        published_at = pd.to_datetime(loaded_at, utc=True, errors="coerce")

        story_id = _story_id(
            s["poly"]["market_id"] if s["poly"] else None,
            s["kalshi"]["market_id"] if s["kalshi"] else None,
            primary["title"],
        )
        # Content-derived snapshot id so reruns with different stories don't
        # collide with prior runs at the same rank position.
        snapshot_story_id = f"{snapshot_date.isoformat()}-{story_id[:10]}"
        s["story_id"] = story_id
        s["snapshot_story_id"] = snapshot_story_id
        s["headline_rank"] = rank
        s["resolved_title"] = title
        s["resolved_url"] = url
        s["resolved_source"] = news_source

        rows.append(
            {
                "snapshot_date": snapshot_date,
                "loaded_at": loaded_at,
                "snapshot_story_id": snapshot_story_id,
                "story_id": story_id,
                "headline_rank": rank,
                "title": title,
                "description": description,
                "content": None,
                "news_source": news_source,
                "published_at": published_at,
                "url": url,
                "image_url": None,
                "search_query": s.get("pair_reason") or "trending:politics",
            }
        )
    return pd.DataFrame(rows)


def _stories_to_matches(
    stories: list[dict],
    snapshot_date: date,
    loaded_at: datetime,
) -> pd.DataFrame:
    rows: list[dict] = []
    for s in stories:
        for source_key, market in (("polymarket", s["poly"]), ("kalshi", s["kalshi"])):
            if market is None:
                continue
            close_time = pd.to_datetime(market.get("close_time"), utc=True, errors="coerce")
            event_date = (
                close_time.date()
                if isinstance(close_time, pd.Timestamp) and not pd.isna(close_time)
                else None
            )
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "loaded_at": loaded_at,
                    "snapshot_story_id": s["snapshot_story_id"],
                    "story_id": s["story_id"],
                    "headline_rank": s["headline_rank"],
                    "headline_title": s["resolved_title"],
                    "search_query": s.get("pair_reason") or "trending:politics",
                    "source": source_key,
                    "candidate_rank": 1,
                    "attena_rank": None,
                    "token_overlap": None,
                    "match_score": abs(market["change_1d"]),
                    "market_id": market["market_id"],
                    "market_title": market["title"],
                    "yes_price": market["yes_price"],
                    "no_price": market["no_price"],
                    "volume": None,
                    "volume_24h": market["volume_24h"],
                    "category": market.get("category"),
                    "subcategory": None,
                    "event_date": event_date,
                    "close_time": close_time,
                    "source_url": market["source_url"],
                    "ticker": market.get("ticker"),
                    "outcome_label": market.get("outcome_label"),
                    "bracket_count": 1,
                    "status": "open",
                    "selected": True,
                    "gemini_direct_match": True,
                    "gemini_match_reason": s.get("pair_reason") or None,
                }
            )
    return pd.DataFrame(rows)


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
        matches_by_source = {row["source"]: row for row in current_matches.to_dict("records")}

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

        basket_yes_price = (
            round(sum(current_prices) / len(current_prices), 2) if current_prices else None
        )
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
                "basket_volume_24h": round(
                    (
                        sum(float(m.get("volume_24h") or 0) for m in (poly, kalshi) if m)
                        / max(sum(1 for m in (poly, kalshi) if m), 1)
                    ),
                    2,
                ),
                "basket_score": round(
                    _movement_score(
                        sum(
                            abs(float(m.get("match_score") or 0))
                            for m in (poly, kalshi)
                            if m
                        )
                        / max(sum(1 for m in (poly, kalshi) if m), 1),
                        sum(float(m.get("volume_24h") or 0) for m in (poly, kalshi) if m)
                        / max(sum(1 for m in (poly, kalshi) if m), 1),
                    ),
                    4,
                ),
            }
        )

    baskets = pd.DataFrame(records)
    if baskets.empty:
        return baskets

    # Rank by volume-weighted movement score. Tiebreak by complete_basket
    # (paired stories) then headline_rank (original ordering from upstream).
    sort_key = pd.to_numeric(baskets["basket_score"], errors="coerce").fillna(-1)
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


def run_daily_snapshot(
    snapshot_date: date,
    top_movers: int = DEFAULT_TOP_MOVERS,
    max_stories: int = DEFAULT_MAX_STORIES,
) -> None:
    loaded_at = datetime.now(timezone.utc)

    poly_movers = fetch_polymarket_movers(limit=top_movers)
    kalshi_movers = fetch_kalshi_movers(limit=top_movers)

    if not poly_movers and not kalshi_movers:
        raise RuntimeError("No politics movers returned from either platform.")

    print("Pairing markets across platforms with Gemini...")
    pairs = pair_markets_across_platforms(poly_movers, kalshi_movers)
    print(f"  {len(pairs)} valid pairs identified")

    stories = _build_stories(pairs, poly_movers, kalshi_movers, max_stories)
    print(f"Building {len(stories)} stories (paired + unpaired top movers)")

    _generate_headlines_parallel(stories)

    headlines_df = _stories_to_headlines(stories, snapshot_date, loaded_at)
    matches_df = _stories_to_matches(stories, snapshot_date, loaded_at)

    previous_prices = get_previous_selected_prices(snapshot_date)
    baskets_df = build_story_baskets(
        headlines=headlines_df,
        matches=matches_df,
        previous_prices=previous_prices,
        snapshot_date=snapshot_date,
        loaded_at=loaded_at,
    )

    _write_to_bq(headlines_df, TABLE_NAME_HEADLINES)
    _write_to_bq(matches_df, TABLE_NAME_MATCHES)
    _write_to_bq(baskets_df, TABLE_NAME_BASKETS)


def _clamp_percent(value: float | None) -> float | None:
    if value is None:
        return None
    return round(max(1.0, min(99.0, value)), 2)


def _date_from_unix(ts: float) -> date:
    return datetime.fromtimestamp(ts, tz=timezone.utc).date()


def fetch_polymarket_history(yes_token_id: str, days: int) -> dict[date, float]:
    """Return {date: yes_price_percent} for the last `days` days from CLOB prices-history."""
    if not yes_token_id:
        return {}
    import time as _time
    now = int(_time.time())
    start_ts = now - (days + 2) * 86400  # buffer
    try:
        r = requests.get(
            "https://clob.polymarket.com/prices-history",
            params={"market": yes_token_id, "startTs": start_ts, "endTs": now, "fidelity": 1440},
            timeout=20,
        )
        r.raise_for_status()
        history = r.json().get("history") or []
    except Exception as exc:
        print(f"  Polymarket history fetch failed for {yes_token_id[:14]}...: {exc}")
        return {}

    # Keep the last point seen per UTC date.
    by_date: dict[date, float] = {}
    for pt in history:
        try:
            d = _date_from_unix(float(pt["t"]))
            by_date[d] = round(float(pt["p"]) * 100, 2)
        except (KeyError, TypeError, ValueError):
            continue
    return by_date


def fetch_kalshi_history(series_ticker: str, market_ticker: str, days: int) -> dict[date, float]:
    """Return {date: yes_price_percent} for the last `days` days from Kalshi candlesticks."""
    if not series_ticker or not market_ticker:
        return {}
    import time as _time
    now = int(_time.time())
    start_ts = now - (days + 2) * 86400
    url = (
        f"{KALSHI_BASE}/series/{series_ticker}/markets/{market_ticker}/candlesticks"
    )
    try:
        r = requests.get(
            url,
            params={"start_ts": start_ts, "end_ts": now, "period_interval": 1440},
            timeout=20,
        )
        if r.status_code == 429:
            import time as _t
            _t.sleep(2)
            r = requests.get(
                url,
                params={"start_ts": start_ts, "end_ts": now, "period_interval": 1440},
                timeout=20,
            )
        r.raise_for_status()
        candles = r.json().get("candlesticks") or []
    except Exception as exc:
        print(f"  Kalshi history fetch failed for {market_ticker}: {exc}")
        return {}

    by_date: dict[date, float] = {}
    for c in candles:
        try:
            d = _date_from_unix(float(c["end_period_ts"]))
            close = float(c["price"]["close_dollars"])
            by_date[d] = round(close * 100, 2)
        except (KeyError, TypeError, ValueError):
            continue
    return by_date


def _backdate_with_real_history(
    headlines: pd.DataFrame,
    matches: pd.DataFrame,
    history_by_market: dict[tuple[str, str], dict[date, float]],
    snapshot_date: date,
    loaded_at: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clone headlines/matches to `snapshot_date`, overriding yes_price from real history.

    history_by_market maps (source, market_id) -> {date: yes_price_pct}. If a market
    has no entry for `snapshot_date`, we fall back to the nearest earlier-or-equal
    date in the history; if none, the price stays at today's value (best-effort).
    """
    h = headlines.copy()
    h["snapshot_date"] = snapshot_date
    h["loaded_at"] = loaded_at
    h["snapshot_story_id"] = h["story_id"].apply(
        lambda sid: f"{snapshot_date.isoformat()}-{(sid or '')[:10]}"
    )
    sid_lookup = dict(zip(h["story_id"], h["snapshot_story_id"], strict=False))

    m = matches.copy()
    m["snapshot_date"] = snapshot_date
    m["loaded_at"] = loaded_at
    m["snapshot_story_id"] = m["story_id"].map(sid_lookup)

    def lookup_price(row: pd.Series) -> float | None:
        key = (row["source"], row["market_id"])
        history = history_by_market.get(key) or {}
        if not history:
            return row.get("yes_price")
        if snapshot_date in history:
            return _clamp_percent(history[snapshot_date])
        # Nearest prior date (so future dates aren't used).
        prior_dates = [d for d in history if d <= snapshot_date]
        if prior_dates:
            return _clamp_percent(history[max(prior_dates)])
        return row.get("yes_price")

    m["yes_price"] = m.apply(lookup_price, axis=1)
    m["no_price"] = m["yes_price"].apply(
        lambda p: None if p is None or pd.isna(p) else round(100 - float(p), 2)
    )
    return h, m


def run_simulated_snapshot_series(
    end_date: date,
    top_movers: int = DEFAULT_TOP_MOVERS,
    max_stories: int = DEFAULT_MAX_STORIES,
    simulate_days: int = 3,
) -> None:
    """Run the live ETL once for `end_date`, then write backdated snapshots for the
    prior `simulate_days - 1` days using REAL historical prices from each platform's
    history endpoint (Polymarket CLOB prices-history, Kalshi candlesticks).
    """
    loaded_at_base = datetime.now(timezone.utc)

    poly_movers = fetch_polymarket_movers(limit=top_movers)
    kalshi_movers = fetch_kalshi_movers(limit=top_movers)
    if not poly_movers and not kalshi_movers:
        raise RuntimeError("No politics movers returned from either platform.")

    print("Pairing markets across platforms with Gemini...")
    pairs = pair_markets_across_platforms(poly_movers, kalshi_movers)
    print(f"  {len(pairs)} valid pairs identified")

    stories = _build_stories(pairs, poly_movers, kalshi_movers, max_stories)
    print(f"Building {len(stories)} stories (paired + unpaired top movers)")

    _generate_headlines_parallel(stories)

    base_headlines = _stories_to_headlines(stories, end_date, loaded_at_base)
    base_matches = _stories_to_matches(stories, end_date, loaded_at_base)

    # Pre-fetch real history for each market in the cohort.
    print(f"Fetching real {simulate_days}-day history per market...")
    history_by_market: dict[tuple[str, str], dict[date, float]] = {}
    for s in stories:
        if s["poly"]:
            key = ("polymarket", s["poly"]["market_id"])
            history_by_market[key] = fetch_polymarket_history(
                s["poly"].get("yes_token_id") or "", simulate_days
            )
        if s["kalshi"]:
            key = ("kalshi", s["kalshi"]["market_id"])
            history_by_market[key] = fetch_kalshi_history(
                s["kalshi"].get("series_ticker") or "",
                s["kalshi"].get("ticker") or s["kalshi"]["market_id"],
                simulate_days,
            )
    poly_with_hist = sum(1 for k, v in history_by_market.items() if k[0] == "polymarket" and v)
    kalshi_with_hist = sum(1 for k, v in history_by_market.items() if k[0] == "kalshi" and v)
    print(f"  Polymarket: {poly_with_hist} markets returned history")
    print(f"  Kalshi:     {kalshi_with_hist} markets returned history")

    # Walk from oldest to newest so each day's "previous_prices" is the prior day.
    dates = [end_date - timedelta(days=offset) for offset in range(simulate_days - 1, -1, -1)]
    previous_prices = pd.DataFrame(columns=["source", "market_id", "yes_price", "snapshot_date"])

    for idx, day in enumerate(dates):
        offset = (end_date - day).days
        day_loaded_at = loaded_at_base + timedelta(seconds=idx)
        if offset == 0:
            day_headlines, day_matches = base_headlines, base_matches
        else:
            day_headlines, day_matches = _backdate_with_real_history(
                base_headlines, base_matches, history_by_market, day, day_loaded_at,
            )

        day_baskets = build_story_baskets(
            headlines=day_headlines,
            matches=day_matches,
            previous_prices=previous_prices,
            snapshot_date=day,
            loaded_at=day_loaded_at,
        )

        print(f"Writing snapshot for {day} (offset {offset})...")
        _write_to_bq(day_headlines, TABLE_NAME_HEADLINES)
        _write_to_bq(day_matches, TABLE_NAME_MATCHES)
        _write_to_bq(day_baskets, TABLE_NAME_BASKETS)

        previous_prices = day_matches[day_matches["selected"]][
            ["source", "market_id", "yes_price"]
        ].copy()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch trending politics markets, pair across platforms, "
            "and store daily baskets."
        )
    )
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Snapshot date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--top-movers",
        type=int,
        default=DEFAULT_TOP_MOVERS,
        help="Top-N movers to fetch per platform.",
    )
    parser.add_argument(
        "--max-stories",
        type=int,
        default=DEFAULT_MAX_STORIES,
        help="Cap on number of stories written per snapshot.",
    )
    parser.add_argument(
        "--simulate-days",
        type=int,
        default=0,
        help=(
            "If >1, also write backdated snapshots for the prior N-1 days using "
            "deterministic price perturbations. Useful to seed the trending chart."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    snapshot_date = date.fromisoformat(args.date)
    if args.simulate_days and args.simulate_days > 1:
        run_simulated_snapshot_series(
            end_date=snapshot_date,
            top_movers=args.top_movers,
            max_stories=args.max_stories,
            simulate_days=args.simulate_days,
        )
        sys.exit(0)
    run_daily_snapshot(
        snapshot_date=snapshot_date,
        top_movers=args.top_movers,
        max_stories=args.max_stories,
    )
