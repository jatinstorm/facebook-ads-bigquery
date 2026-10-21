"""
Facebook Ads → BigQuery importer for the translation markets (DE / ES / IT).
Each market has its own ad account and its own BigQuery table. Runs as a
separate Cloud Run service from the English importer in main.py.

GET/POST /                        → yesterday, all markets
GET/POST /?since=YYYY-MM-DD&until=YYYY-MM-DD[&market=it]  → backfill a range

Loads replace the requested date range, so re-running a day never duplicates.
"""

from flask import Flask, request

import os
import json
import logging
from datetime import date, timedelta

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

app = Flask(__name__)

load_dotenv()

# On Cloud Run the token is a Secret Manager volume, read on every run so a new
# secret version is picked up without redeploying. Locally it comes from .env.
FB_TOKEN_FILE = os.getenv("FB_TOKEN_FILE", "/secrets/fb-translations-token")
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v24.0")

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "marketing-489109")
BQ_DATASET = os.getenv("BQ_DATASET", "facebook_ads")

MARKETS = {
    "de": {"account_id": "act_1410604697232220", "table": "facebook_ads_de"},
    "es": {"account_id": "act_1426658752134657", "table": "facebook_ads_es"},
    "it": {"account_id": "act_1435686567887152", "table": "facebook_ads_it"},
}

# Facebook returns spend/reach/impressions as 0 on these accounts when
# inline_link_clicks or cost_per_inline_link_click are in the same request,
# so link clicks are fetched separately and merged.
FIELDS = (
    "adset_id,adset_name,date_start,date_stop,reach,frequency,spend,"
    "impressions,inline_link_click_ctr,clicks,ctr,cpc,account_currency"
)
LINK_CLICK_FIELDS = "adset_id,date_start,inline_link_clicks"

SCHEMA = [
    bigquery.SchemaField("adset_name", "STRING"),
    bigquery.SchemaField("date_start", "DATE"),
    bigquery.SchemaField("date_stop", "DATE"),
    bigquery.SchemaField("inline_link_clicks", "INTEGER"),
    bigquery.SchemaField("reach", "INTEGER"),
    bigquery.SchemaField("frequency", "FLOAT"),
    bigquery.SchemaField("cost_per_inline_link_click", "FLOAT"),
    bigquery.SchemaField("spend", "FLOAT"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("inline_link_click_ctr", "FLOAT"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("ctr", "FLOAT"),
    bigquery.SchemaField("cpc", "FLOAT"),
    bigquery.SchemaField("Title", "STRING"),
    bigquery.SchemaField("Buy_Pre_order", "STRING"),
    bigquery.SchemaField("Price", "STRING"),
    bigquery.SchemaField("Targeting_type", "STRING"),
    bigquery.SchemaField("Targeting", "STRING"),
    bigquery.SchemaField("Age_range", "STRING"),
    bigquery.SchemaField("market", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
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
    return os.getenv("FB_TRANSLATIONS_ACCESS_TOKEN")


def _get_all(account_id, fields, since, until):
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{account_id}/insights"
    params = {
        "level": "adset",
        "fields": fields,
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "limit": 500,
        "access_token": get_access_token(),
    }

    all_data = []
    response = requests.get(url, params=params, timeout=120)

    while True:
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(f"{account_id}: {payload['error'].get('message')}")

        all_data.extend(payload.get("data", []))

        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            break
        response = requests.get(next_url, timeout=120)

    return all_data


def fetch_insights(account_id, since, until):
    data = _get_all(account_id, FIELDS, since, until)
    link_clicks = {
        (r["adset_id"], r["date_start"]): r.get("inline_link_clicks", 0)
        for r in _get_all(account_id, LINK_CLICK_FIELDS, since, until)
    }

    for row in data:
        row["inline_link_clicks"] = link_clicks.get((row["adset_id"], row["date_start"]), 0)

    return data


# -------------------------------------------------------------------
# Transformations
# -------------------------------------------------------------------


def split_adset_name(name):
    """Best-effort split of 'Title__BUY__Price__Type__Targeting[__Age]'.
    Names that don't use '__' are kept whole in adset_name only."""
    if not name or "__" not in name:
        return [""] * 6

    parts = [p.strip() for p in name.split("__")]
    parts += [""] * (6 - len(parts))
    parts = parts[:6]

    # Duplicated ad sets end in " - Copy"; drop it from the targeting value
    if parts[4].endswith(" - Copy"):
        parts[4] = parts[4][: -len(" - Copy")]

    return parts


def transform_rows(data, market, account_id):
    rows = []

    for row in data:
        title, preorder, price, targeting_type, targeting, age = split_adset_name(
            row.get("adset_name", "")
        )
        spend = float(row.get("spend", 0))
        link_clicks = int(row.get("inline_link_clicks", 0))

        rows.append(
            {
                "adset_name": row.get("adset_name"),
                "date_start": row.get("date_start"),
                "date_stop": row.get("date_stop"),
                "inline_link_clicks": link_clicks,
                "reach": int(row.get("reach", 0)),
                "frequency": float(row.get("frequency", 0)),
                "cost_per_inline_link_click": round(spend / link_clicks, 6)
                if link_clicks
                else 0.0,
                "spend": spend,
                "impressions": int(row.get("impressions", 0)),
                "inline_link_click_ctr": float(row.get("inline_link_click_ctr", 0)),
                "clicks": int(row.get("clicks", 0)),
                "ctr": float(row.get("ctr", 0)),
                "cpc": float(row.get("cpc", 0)),
                "Title": title,
                "Buy_Pre_order": preorder,
                "Price": price,
                "Targeting_type": targeting_type,
                "Targeting": targeting,
                "Age_range": age,
                "market": market.upper(),
                "account_id": account_id,
                "currency": row.get("account_currency", "EUR"),
            }
        )

    return rows


# -------------------------------------------------------------------
# BigQuery
# -------------------------------------------------------------------


def ensure_table(client, table_id):
    table = bigquery.Table(table_id, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="date_start")
    client.create_table(table, exists_ok=True)


def replace_range(client, table_id, rows, since, until):
    """Delete the date range, then load the fresh rows (idempotent)."""
    client.query(
        f"DELETE FROM `{table_id}` WHERE date_start BETWEEN @since AND @until",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("since", "DATE", since),
                bigquery.ScalarQueryParameter("until", "DATE", until),
            ]
        ),
    ).result()

    if rows:
        client.load_table_from_json(
            rows,
            table_id,
            job_config=bigquery.LoadJobConfig(
                schema=SCHEMA, write_disposition="WRITE_APPEND"
            ),
        ).result()

    return len(rows)


# -------------------------------------------------------------------
# Entry
# -------------------------------------------------------------------


def import_translations(since=None, until=None, markets=None):
    if not get_access_token():
        raise RuntimeError("Facebook token missing (secret fb-translations-token)")

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    since = since or yesterday
    until = until or since
    markets = markets or list(MARKETS)

    client = bigquery.Client(project=BQ_PROJECT_ID)
    results = {}

    for market in markets:
        cfg = MARKETS[market]
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{cfg['table']}"

        ensure_table(client, table_id)
        raw = fetch_insights(cfg["account_id"], since, until)
        rows = transform_rows(raw, market, cfg["account_id"])
        count = replace_range(client, table_id, rows, since, until)

        logger.info("%s: loaded %d rows (%s to %s) into %s", market, count, since, until, table_id)
        results[market] = count

    return results


@app.route("/", methods=["GET", "POST"])
def run_import():
    try:
        market = request.args.get("market")
        if market and market not in MARKETS:
            return f"Unknown market '{market}'", 400

        results = import_translations(
            since=request.args.get("since"),
            until=request.args.get("until"),
            markets=[market] if market else None,
        )
        return json.dumps(results), 200

    except Exception as e:
        logger.exception("Import failed")
        return (str(e), 500)
