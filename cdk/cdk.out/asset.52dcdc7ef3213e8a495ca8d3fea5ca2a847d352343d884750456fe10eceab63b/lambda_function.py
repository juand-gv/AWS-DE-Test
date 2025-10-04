import os
import json
import logging
import time
import boto3
import requests
from typing import List, Optional
from io import BytesIO
from pydantic import ValidationError

from schema import UserModel

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = os.environ.get("BUCKET")
PREFIX = os.environ.get("PREFIX", "raw/")
API_URL = os.environ.get("API_URL")
FILE_FORMAT = (os.environ.get("FILE_FORMAT") or "ndjson").lower()

# try import pyarrow from layer; if not available, HAS_PYARROW = False
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except Exception:
    HAS_PYARROW = False


def validate_and_normalize(raw_item: dict) -> Optional[dict]:
    try:
        user = UserModel.parse_obj(raw_item)
    except ValidationError as e:
        logger.warning("Validation failed for item: %s; errors: %s", raw_item, e)
        return None

    return {
        "gender": getattr(user, "gender", None),
        "first_name": getattr(getattr(user, "name", None), "first", None),
        "last_name": getattr(getattr(user, "name", None), "last", None),
        "email": getattr(user, "email", None),
        "country": getattr(getattr(user, "location", None), "country", None),
        "state": getattr(getattr(user, "location", None), "state", None),
        "city": getattr(getattr(user, "location", None), "city", None),
        "postcode": getattr(getattr(user, "location", None), "postcode", None),
        "street_number": getattr(getattr(getattr(user, "location", None), "street", None), "number", None),
        "street_name": getattr(getattr(getattr(user, "location", None), "street", None), "name", None),
        "phone": getattr(user, "phone", None),
        "cell": getattr(user, "cell", None),
        "nat": getattr(user, "nat", None),
        "dob": getattr(getattr(user, "dob", None), "date", None),
        "age": getattr(getattr(user, "dob", None), "age", None),
        "registered_date": getattr(getattr(user, "registered", None), "date", None),
        "registered_age": getattr(getattr(user, "registered", None), "age", None),
        "uuid": getattr(getattr(user, "login", None), "uuid", None),
        "username": getattr(getattr(user, "login", None), "username", None),
        "picture_large": getattr(getattr(user, "picture", None), "large", None),
        "picture_medium": getattr(getattr(user, "picture", None), "medium", None),
        "picture_thumbnail": getattr(getattr(user, "picture", None), "thumbnail", None),
        "id_name": getattr(getattr(user, "id", None), "name", None),
        "id_value": getattr(getattr(user, "id", None), "value", None)
    }


def to_ndjson_bytes(records: List[dict]) -> bytes:
    lines = [json.dumps(r, default=str, ensure_ascii=False) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")


def to_parquet_bytes(records: List[dict]) -> bytes:
    if not HAS_PYARROW:
        raise RuntimeError("pyarrow not available for parquet conversion")
    table = pa.Table.from_pylist(records)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.read()


def upload_bytes(bucket: str, key: str, data: bytes, content_type: Optional[str] = None):
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    s3.put_object(Bucket=bucket, Key=key, Body=data, **extra)


def handler(event, context):
    if not BUCKET:
        logger.error("Missing BUCKET env var")
        raise RuntimeError("BUCKET env var required")
    if not API_URL:
        logger.error("Missing API_URL env var")
        raise RuntimeError("API_URL env var required")

    logger.info("Invoked. API=%s format=%s", API_URL, FILE_FORMAT)
    try:
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logger.exception("Failed to fetch API: %s", e)
        raise

    items = payload.get("results") or payload.get("data") or payload
    if isinstance(items, dict):
        items = [items]

    normalized = []
    for idx, it in enumerate(items):
        try:
            n = validate_and_normalize(it)
            if n:
                normalized.append(n)
        except Exception as e:
            logger.exception("Error processing item %d: %s", idx, e)

    if not normalized:
        logger.warning("No valid records after validation")
        return {"status": "no_records", "fetched": len(items)}

    ts = int(time.time())
    base = f"{PREFIX}users_{ts}"
    count = len(normalized)

    # Try Parquet if requested
    if FILE_FORMAT == "parquet":
        try:
            parquet_bytes = to_parquet_bytes(normalized)
            key = f"{base}.parquet"
            upload_bytes(BUCKET, key, parquet_bytes, content_type="application/octet-stream")
            logger.info("Wrote %d records to s3://%s/%s (parquet)", count, BUCKET, key)
            return {"status": "ok", "key": key, "format": "parquet", "records": count}
        except Exception as e:
            logger.exception("Parquet write failed, falling back to NDJSON: %s", e)

    # Fallback NDJSON
    try:
        ndjson = to_ndjson_bytes(normalized)
        key = f"{base}.ndjson"
        upload_bytes(BUCKET, key, ndjson, content_type="application/x-ndjson; charset=utf-8")
        logger.info("Wrote %d records to s3://%s/%s (ndjson)", count, BUCKET, key)
        return {"status": "ok", "key": key, "format": "ndjson", "records": count}
    except Exception as e:
        logger.exception("Failed uploading NDJSON: %s", e)
        raise
