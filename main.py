"""
Facebook Ads → BigQuery Importer
Fetches ad insights from Facebook Marketing API and inserts them into BigQuery.
"""

from flask import Flask, request


import os
import json
import logging
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# -------------------------------------------------------------------
# Load environment variables
# -------------------------------------------------------------------

app = Flask(__name__)


load_dotenv()

FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
FB_AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v24.0")

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

BASE_API_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{FB_AD_ACCOUNT_ID}/insights?"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Facebook API
# -------------------------------------------------------------------


def build_api_url():
    query_params = {
        "level": "adset",
        "fields": (
            "adset_name,date_start,date_stop,inline_link_clicks,reach,"
            "frequency,cost_per_inline_link_click,spend,impressions,"
            "inline_link_click_ctr,clicks,ctr,cpc"
        ),
        "date_preset": "yesterday",
        "access_token": FB_ACCESS_TOKEN,
    }

    filtering_value = [
        {"field": "action_type", "operator": "CONTAIN", "value": "link_click"}
    ]

    return (
        BASE_API_URL
        + urlencode(query_params)
        + "&filtering="
        + requests.utils.quote(json.dumps(filtering_value))
    )


def fetch_all_insights():
    all_data = []
    next_url = build_api_url()

    while next_url:
        logger.info("Fetching Facebook data...")

        response = requests.get(next_url, timeout=120)
        response.raise_for_status()
        payload = response.json()

        all_data.extend(payload.get("data", []))

        next_url = payload.get("paging", {}).get("next")

    logger.info("Records fetched: %d", len(all_data))
    return all_data


# -------------------------------------------------------------------
# Transformations
# -------------------------------------------------------------------


def split_adset_name(name):
    parts = name.split("__") if name else []
    parts += [""] * (6 - len(parts))
    return parts[:6]


def transform_rows(data):
    rows = []

    for row in data:
        edition, preorder, territory, targeting_type, targeting, age = split_adset_name(
            row.get("adset_name", "")
        )

        rows.append(
            {
                "adset_name": row.get("adset_name"),
                "date_start": row.get("date_start"),
                "date_stop": row.get("date_stop"),
                "inline_link_clicks": int(row.get("inline_link_clicks", 0)),
                "reach": int(row.get("reach", 0)),
                "frequency": float(row.get("frequency", 0)),
                "cost_per_inline_link_click": float(
                    row.get("cost_per_inline_link_click", 0)
                ),
                "spend": float(row.get("spend", 0)),
                "impressions": int(row.get("impressions", 0)),
                "inline_link_click_ctr": float(row.get("inline_link_click_ctr", 0)),
                "clicks": int(row.get("clicks", 0)),
                "ctr": float(row.get("ctr", 0)),
                "cpc": float(row.get("cpc", 0)),
                "Edition_ID": edition,
                "Buy_Pre_order": preorder,
                "Territory": territory,
                "Targeting_type": targeting_type,
                "Targeting": targeting,
                "Age_range": age,
            }
        )

    return rows


# -------------------------------------------------------------------
# BigQuery
# -------------------------------------------------------------------


def insert_bigquery(rows):
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(errors)

    logger.info("Inserted %d rows into BigQuery", len(rows))
    return len(rows)


# -------------------------------------------------------------------
# Cloud Function Entry
# -------------------------------------------------------------------


def import_facebook_ads(request=None):
    try:
        if not FB_ACCESS_TOKEN:
            raise RuntimeError("FB_ACCESS_TOKEN missing")

        raw_data = fetch_all_insights()

        if not raw_data:
            return ("No data returned", 200)

        rows = transform_rows(raw_data)

        count = insert_bigquery(rows)

        return (f"Success — {count} rows inserted", 200)

    except Exception as e:
        logger.exception("Import failed")
        return (str(e), 500)


# -------------------------------------------------------------------
# Local testing
# -------------------------------------------------------------------

@app.route("/", methods=["GET","POST"])
def run_import():
    message, status = import_facebook_ads(request)
    return message, status