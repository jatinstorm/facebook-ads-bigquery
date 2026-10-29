"""
Facebook Ads → BigQuery importer at ad level (one row per ad per day).
Sits beside the ad-set importer in main.py; it doesn't touch that table.

GET/POST /                                   → yesterday plus a 7-day look-back
GET/POST /?since=YYYY-MM-DD&until=YYYY-MM-DD → reload a range

Loads replace the requested date range per account, so re-runs never
duplicate and Meta's restatements of recent days are picked up.
Sales/KENP are deliberately not joined in: take them from the KDP tables.
"""

from flask import Flask, request

import os
import re
import json
import time
import logging
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

app = Flask(__name__)

load_dotenv()

GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v24.0")
GRAPH_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "marketing-489109")
BQ_DATASET = os.getenv("BQ_DATASET", "facebook_ads")
INSIGHTS_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.ad_daily_insights"
CREATIVES_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.ad_creatives"

# On Cloud Run the token is a Secret Manager volume, read on every run so a new
# secret version is picked up without redeploying. Locally it comes from env.
FB_TOKEN_FILE = os.getenv("FB_TOKEN_FILE", "/secrets/fb-english-token")

ACCOUNTS = ["act_400611914284700"]  # Storm Publishing (UK/US)

ATTRIBUTION_WINDOWS = ["7d_click", "1d_view"]
LOOKBACK_DAYS = 7
CHUNK_DAYS = 7

INSIGHT_FIELDS = (
    "ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,"
    "spend,impressions,reach,clicks,inline_link_clicks,objective,date_start"
)

AD_FIELDS = (
    "id,name,status,effective_status,created_time,updated_time,"
    "adset_id,campaign_id,account_id,"
    "creative{id,name,object_type,body,title,"
    "call_to_action_type,image_hash,image_url,thumbnail_url,"
    "video_id,object_story_spec,asset_feed_spec,url_tags}"
)
CREATIVE_BATCH = 50

# Throttling error codes: retry with back-off rather than fail the run
RATE_LIMIT_CODES = {4, 17, 32, 613, 80000, 80004}

INSIGHTS_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("account_name", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("adset_id", "STRING"),
    bigquery.SchemaField("adset_name", "STRING"),
    bigquery.SchemaField("ad_id", "STRING"),
    bigquery.SchemaField("ad_name", "STRING"),
    bigquery.SchemaField("edition_id", "STRING"),
    bigquery.SchemaField("objective", "STRING"),
    bigquery.SchemaField("spend", "NUMERIC"),
    bigquery.SchemaField("impressions", "INT64"),
    bigquery.SchemaField("reach", "INT64"),
    bigquery.SchemaField("frequency", "NUMERIC"),
    bigquery.SchemaField("clicks", "INT64"),
    bigquery.SchemaField("link_clicks", "INT64"),
    bigquery.SchemaField("ctr", "NUMERIC"),
    bigquery.SchemaField("link_ctr", "NUMERIC"),
    bigquery.SchemaField("cpc", "NUMERIC"),
    bigquery.SchemaField("cpm", "NUMERIC"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("attribution_windows", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

CREATIVES_SCHEMA = [
    bigquery.SchemaField("ad_id", "STRING"),
    bigquery.SchemaField("ad_name", "STRING"),
    bigquery.SchemaField("adset_id", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("effective_status", "STRING"),
    bigquery.SchemaField("created_time", "TIMESTAMP"),
    bigquery.SchemaField("updated_time", "TIMESTAMP"),
    bigquery.SchemaField("creative_id", "STRING"),
    bigquery.SchemaField("object_type", "STRING"),
    bigquery.SchemaField("primary_text", "STRING"),
    bigquery.SchemaField("headline", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("call_to_action", "STRING"),
    bigquery.SchemaField("image_hash", "STRING"),
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("thumbnail_url", "STRING"),
    bigquery.SchemaField("image_gcs_path", "STRING"),
    bigquery.SchemaField("destination_url", "STRING"),
    bigquery.SchemaField("attribution_tag", "STRING"),
    bigquery.SchemaField("is_multi_asset", "BOOL"),
    bigquery.SchemaField("n_images", "INT64"),
    bigquery.SchemaField("n_bodies", "INT64"),
    bigquery.SchemaField("n_titles", "INT64"),
    bigquery.SchemaField("raw_creative", "JSON"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Facebook API
# -------------------------------------------------------------------


def get_access_token():
    if os.path.exists(FB_TOKEN_FILE):
        with open(FB_TOKEN_FILE, encoding="utf-8") as f:
            return f.read().strip()
    return os.getenv("FB_ENGLISH_ACCESS_TOKEN")


def graph_get(url, params=None, max_retries=6):
    """GET with back-off on Meta's rate-limit errors."""
    for attempt in range(max_retries):
        payload = requests.get(url, params=params, timeout=120).json()
        error = payload.get("error")

        if not error:
            return payload

        if error.get("code") in RATE_LIMIT_CODES and attempt < max_retries - 1:
            wait = 60 * (2 ** attempt)
            logger.warning("Rate limited (%s), waiting %ds", error.get("code"), wait)
            time.sleep(wait)
            continue

        raise RuntimeError(f"Graph API error: {error.get('message')}")


def get_all(url, params):
    all_data = []
    payload = graph_get(url, params)

    while True:
        all_data.extend(payload.get("data", []))

        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            return all_data
        payload = graph_get(next_url)


def get_account(account_id):
    return graph_get(
        f"{GRAPH_URL}/{account_id}",
        {"fields": "name,currency", "access_token": get_access_token()},
    )


def fetch_ad_insights(account_id, since, until):
    return get_all(
        f"{GRAPH_URL}/{account_id}/insights",
        {
            "level": "ad",
            "fields": INSIGHT_FIELDS,
            "time_range": json.dumps({"since": since, "until": until}),
            "time_increment": 1,
            "action_attribution_windows": json.dumps(ATTRIBUTION_WINDOWS),
            "limit": 500,
            "access_token": get_access_token(),
        },
    )


def fetch_ads(ad_ids, pause=0):
    """Ads with their creative expanded, fetched by ID in batches. Works for
    paused, archived and deleted ads, unlike listing /ads."""
    ads = []

    for i in range(0, len(ad_ids), CREATIVE_BATCH):
        batch = ad_ids[i : i + CREATIVE_BATCH]
        payload = graph_get(
            f"{GRAPH_URL}/",
            {"ids": ",".join(batch), "fields": AD_FIELDS, "access_token": get_access_token()},
        )
        ads.extend(payload.values())
        time.sleep(pause)

    return ads


# -------------------------------------------------------------------
# Transformations
# -------------------------------------------------------------------


def edition_id(adset_name, campaign_name):
    """Leading integer of the ad set name, falling back to the campaign name."""
    for name in (adset_name, campaign_name):
        match = re.match(r"\s*(\d+)", name or "")
        if match:
            return match.group(1)
    return None


def ratio(numerator, denominator, scale=1):
    return round(numerator / denominator * scale, 6) if denominator else None


def transform_rows(data, account_id, account):
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for row in data:
        spend = float(row.get("spend", 0))
        impressions = int(row.get("impressions", 0))
        reach = int(row.get("reach", 0))
        clicks = int(row.get("clicks", 0))
        link_clicks = int(row.get("inline_link_clicks", 0))

        rows.append(
            {
                "date": row["date_start"],
                "account_id": account_id,
                "account_name": account.get("name"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name"),
                "adset_id": row.get("adset_id"),
                "adset_name": row.get("adset_name"),
                "ad_id": row.get("ad_id"),
                "ad_name": row.get("ad_name"),
                "edition_id": edition_id(row.get("adset_name"), row.get("campaign_name")),
                "objective": row.get("objective"),
                "spend": round(spend, 2),
                "impressions": impressions,
                "reach": reach,
                # Ratios recomputed from the counts so totals add up
                "frequency": ratio(impressions, reach),
                "clicks": clicks,
                "link_clicks": link_clicks,
                "ctr": ratio(clicks, impressions, 100),
                "link_ctr": ratio(link_clicks, impressions, 100),
                "cpc": ratio(spend, clicks),
                "cpm": ratio(spend, impressions, 1000),
                "currency": account.get("currency"),
                "attribution_windows": ",".join(ATTRIBUTION_WINDOWS),
                "loaded_at": loaded_at,
            }
        )

    return rows


def _first(*values):
    return next((v for v in values if v), None)


def _first_asset(items, key):
    return (items or [{}])[0].get(key)


def to_timestamp(value):
    """Meta sends '2026-10-16T16:24:50+0100'; BigQuery wants an ISO offset."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z").isoformat() if value else None


def attribution_tag(url):
    """Amazon Attribution ad-group tag (maas=...) from the destination URL."""
    query = parse_qs(urlparse(url or "").query)
    return _first(*query.get("maas", []), *query.get("aa_adgroupid", []))


def transform_creatives(ads):
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for ad in ads:
        creative = ad.get("creative", {})
        story = creative.get("object_story_spec", {})
        link_data = story.get("link_data", {})
        video_data = story.get("video_data", {})
        # Multi-image/text ads keep their assets here; take the first of each
        feed = creative.get("asset_feed_spec") or {}

        destination = _first(
            link_data.get("link"),
            video_data.get("call_to_action", {}).get("value", {}).get("link"),
            _first_asset(feed.get("link_urls"), "website_url"),
        )

        rows.append(
            {
                "ad_id": ad.get("id"),
                "ad_name": ad.get("name"),
                "adset_id": ad.get("adset_id"),
                "campaign_id": ad.get("campaign_id"),
                "account_id": f"act_{ad.get('account_id')}",
                "status": ad.get("status"),
                "effective_status": ad.get("effective_status"),
                "created_time": to_timestamp(ad.get("created_time")),
                "updated_time": to_timestamp(ad.get("updated_time")),
                "creative_id": creative.get("id"),
                "object_type": creative.get("object_type"),
                "primary_text": _first(
                    creative.get("body"),
                    link_data.get("message"),
                    video_data.get("message"),
                    _first_asset(feed.get("bodies"), "text"),
                ),
                "headline": _first(
                    creative.get("title"),
                    link_data.get("name"),
                    video_data.get("title"),
                    _first_asset(feed.get("titles"), "text"),
                ),
                "description": _first(
                    link_data.get("description"),
                    video_data.get("link_description"),
                    _first_asset(feed.get("descriptions"), "text"),
                ),
                "call_to_action": _first(
                    creative.get("call_to_action_type"),
                    link_data.get("call_to_action", {}).get("type"),
                    video_data.get("call_to_action", {}).get("type"),
                    *(feed.get("call_to_action_types") or []),
                ),
                "image_hash": _first(
                    creative.get("image_hash"),
                    link_data.get("image_hash"),
                    _first_asset(feed.get("images"), "hash"),
                ),
                "video_id": _first(
                    creative.get("video_id"),
                    video_data.get("video_id"),
                    _first_asset(feed.get("videos"), "video_id"),
                ),
                "thumbnail_url": creative.get("thumbnail_url"),
                "image_gcs_path": None,
                "destination_url": destination,
                "attribution_tag": attribution_tag(destination),
                "is_multi_asset": bool(feed),
                "n_images": len(feed.get("images") or []) + len(feed.get("videos") or []),
                "n_bodies": len(feed.get("bodies") or []),
                "n_titles": len(feed.get("titles") or []),
                "raw_creative": creative,
                "loaded_at": loaded_at,
            }
        )

    return rows


# -------------------------------------------------------------------
# BigQuery
# -------------------------------------------------------------------


def ensure_tables(client):
    table = bigquery.Table(INSIGHTS_TABLE, schema=INSIGHTS_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    table.clustering_fields = ["edition_id", "ad_id"]
    client.create_table(table, exists_ok=True)

    client.create_table(bigquery.Table(CREATIVES_TABLE, schema=CREATIVES_SCHEMA), exists_ok=True)


def replace_creatives(client, rows):
    """One row per ad: drop the ads being refreshed, then load them again."""
    if not rows:
        return 0

    client.query(
        f"DELETE FROM `{CREATIVES_TABLE}` WHERE ad_id IN UNNEST(@ad_ids)",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ad_ids", "STRING", [r["ad_id"] for r in rows])
            ]
        ),
    ).result()

    client.load_table_from_json(
        rows,
        CREATIVES_TABLE,
        job_config=bigquery.LoadJobConfig(
            schema=CREATIVES_SCHEMA, write_disposition="WRITE_APPEND"
        ),
    ).result()

    return len(rows)


def replace_range(client, account_id, rows, since, until):
    """Delete the account's date range, then load the fresh rows (idempotent)."""
    client.query(
        f"DELETE FROM `{INSIGHTS_TABLE}` "
        "WHERE date BETWEEN @since AND @until AND account_id = @account_id",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("since", "DATE", since),
                bigquery.ScalarQueryParameter("until", "DATE", until),
                bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
            ]
        ),
    ).result()

    if rows:
        client.load_table_from_json(
            rows,
            INSIGHTS_TABLE,
            job_config=bigquery.LoadJobConfig(
                schema=INSIGHTS_SCHEMA, write_disposition="WRITE_APPEND"
            ),
        ).result()

    return len(rows)


# -------------------------------------------------------------------
# Entry
# -------------------------------------------------------------------


def date_chunks(since, until, days=CHUNK_DAYS):
    start, end = date.fromisoformat(since), date.fromisoformat(until)
    while start <= end:
        chunk_end = min(start + timedelta(days=days - 1), end)
        yield start.isoformat(), chunk_end.isoformat()
        start = chunk_end + timedelta(days=1)


def import_ad_insights(since=None, until=None, chunk_days=CHUNK_DAYS, pause=0):
    if not get_access_token():
        raise RuntimeError("Facebook token missing (secret fb-english-token)")

    yesterday = date.today() - timedelta(days=1)
    until = until or yesterday.isoformat()
    since = since or (yesterday - timedelta(days=LOOKBACK_DAYS - 1)).isoformat()

    client = bigquery.Client(project=BQ_PROJECT_ID)
    ensure_tables(client)
    results = {}

    for account_id in ACCOUNTS:
        account = get_account(account_id)
        total = 0

        for chunk_since, chunk_until in date_chunks(since, until, chunk_days):
            raw = fetch_ad_insights(account_id, chunk_since, chunk_until)
            rows = transform_rows(raw, account_id, account)
            total += replace_range(client, account_id, rows, chunk_since, chunk_until)
            logger.info("%s: %d ad rows (%s to %s)", account_id, len(rows), chunk_since, chunk_until)
            time.sleep(pause)

        results[account_id] = total

    return results


def import_creatives(since, until, pause=0):
    """Refresh the creative row of every ad that ran in the date range, so new
    ads appear and renamed or edited ones are updated."""
    if not get_access_token():
        raise RuntimeError("Facebook token missing (secret fb-english-token)")

    client = bigquery.Client(project=BQ_PROJECT_ID)
    ensure_tables(client)

    ad_ids = [
        row.ad_id
        for row in client.query(
            f"SELECT DISTINCT ad_id FROM `{INSIGHTS_TABLE}` WHERE date BETWEEN @since AND @until",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("since", "DATE", since),
                    bigquery.ScalarQueryParameter("until", "DATE", until),
                ]
            ),
        ).result()
    ]

    rows = transform_creatives(fetch_ads(ad_ids, pause))
    count = replace_creatives(client, rows)
    logger.info("Creatives: %d of %d ads refreshed (%s to %s)", count, len(ad_ids), since, until)
    return count


@app.route("/", methods=["GET", "POST"])
def run_import():
    try:
        yesterday = date.today() - timedelta(days=1)
        until = request.args.get("until") or yesterday.isoformat()
        since = request.args.get("since") or (
            yesterday - timedelta(days=LOOKBACK_DAYS - 1)
        ).isoformat()

        results = import_ad_insights(since=since, until=until)
        results["creatives"] = import_creatives(since, until)
        return json.dumps(results), 200

    except Exception as e:
        logger.exception("Import failed")
        return (str(e), 500)


# -------------------------------------------------------------------
# One-off back-fill:  python ad_level.py backfill 2023-10-01 2026-09-25
# Oldest first, paced to stay under Meta's limits on the shared ad account.
# Safe to re-run: every chunk replaces its own dates.
# -------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) == 4 and sys.argv[1] == "backfill":
        _, _, since, until = sys.argv
        print(import_ad_insights(since, until, chunk_days=14, pause=20))
        print({"creatives": import_creatives(since, until, pause=10)})
