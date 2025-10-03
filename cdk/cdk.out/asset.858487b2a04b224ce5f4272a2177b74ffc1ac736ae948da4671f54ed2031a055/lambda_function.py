import os
import json
import logging
import time
import boto3
import requests
from typing import List
from io import BytesIO
from pydantic import ValidationError

from schema import UserModel

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = os.environ.get("BUCKET")
PREFIX = os.environ.get("PREFIX", "raw/")
API_URL = os.environ.get("API_URL")

def validate_and_normalize(raw_item: dict):
    try:
        user = UserModel.parse_obj(raw_item)
    except ValidationError as e:
        logger.warning("Validation failed for item: %s; errors: %s", raw_item, e)
        return None
    normalized = {
        "gender": user.gender,
        "first_name": user.name.first,
        "last_name": user.name.last,
        "email": user.email,
        "country": user.location.country if user.location else None
    }
    return normalized

def to_ndjson(records: List[dict]) -> bytes:
    lines = [json.dumps(r, default=str) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")

def handler(event, context):
    logger.info("Lambda invoked, fetching data from %s", API_URL)
    try:
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error("Failed to fetch API: %s", e)
        raise

    payload = resp.json()
    items = payload.get("results") or payload.get("data") or payload

    normalized = []
    for idx, item in enumerate(items):
        try:
            record = item
            # randomuser nests fields under 'location' and 'name' already
            validated = validate_and_normalize(record)
            if validated:
                normalized.append(validated)
        except Exception as e:
            logger.exception("Error processing item %d: %s", idx, e)

    if not normalized:
        logger.warning("No valid records after validation")
        return {"status": "no_records"}

    timestamp = int(time.time())
    key_base = f"{PREFIX}users_{timestamp}"

    # fallback to ndjson only (lightweight)
    ndjson_bytes = to_ndjson(normalized)
    s3_key = f"{key_base}.ndjson"
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=ndjson_bytes)
    logger.info("Wrote %d records to s3://%s/%s (ndjson)", len(normalized), BUCKET, s3_key)
    return {"status": "ok", "key": s3_key}