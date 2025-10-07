import os
import json
import logging
import time
from datetime import datetime
from io import BytesIO
from typing import List, Optional

import boto3
import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = os.environ.get("BUCKET", "datapipelinestack-databuckete3889a50-gnj55fcp3puh")
PREFIX = os.environ.get("PREFIX", "raw/")
API_URL = os.environ.get("API_URL", "https://randomuser.me/api/?results=100")
FILE_FORMAT = (os.environ.get("FILE_FORMAT", "parquet") or "ndjson").lower()

# try import pyarrow from layer; if not available, HAS_PYARROW = False
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except Exception:
    HAS_PYARROW = False


# -------- Helpers --------
def http_session_with_retries() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def get_in(d: dict, path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def as_iso(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    # randomuser.me suele venir como ISO; devolvemos tal cual o intentamos parsear
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).isoformat()
    except Exception:
        return str(dt_str)


def to_str(x) -> Optional[str]:
    if x is None:
        return None
    return str(x)


def normalize_item(raw: dict) -> dict:
    """
    Aplana el item a un registro plano con tipos consistentes.
    Forzamos strings en campos propensos a ser mixtos (postcode, phone, cell, id_value).
    """
    return {
        "gender": to_str(get_in(raw, ["gender"])),
        "first_name": to_str(get_in(raw, ["name", "first"])),
        "last_name": to_str(get_in(raw, ["name", "last"])),
        "email": to_str(get_in(raw, ["email"])),
        "country": to_str(get_in(raw, ["location", "country"])),
        "state": to_str(get_in(raw, ["location", "state"])),
        "city": to_str(get_in(raw, ["location", "city"])),
        # postcode puede ser int o string en la API → siempre string
        "postcode": to_str(get_in(raw, ["location", "postcode"])),
        "street_number": get_in(raw, ["location", "street", "number"]),
        "street_name": to_str(get_in(raw, ["location", "street", "name"])),
        "phone": to_str(get_in(raw, ["phone"])),
        "cell": to_str(get_in(raw, ["cell"])),
        "nat": to_str(get_in(raw, ["nat"])),
        "dob": as_iso(get_in(raw, ["dob", "date"])),
        "age": get_in(raw, ["dob", "age"]),
        "registered_date": as_iso(get_in(raw, ["registered", "date"])),
        "registered_age": get_in(raw, ["registered", "age"]),
        "uuid": to_str(get_in(raw, ["login", "uuid"])),
        "username": to_str(get_in(raw, ["login", "username"])),
        "picture_large": to_str(get_in(raw, ["picture", "large"])),
        "picture_medium": to_str(get_in(raw, ["picture", "medium"])),
        "picture_thumbnail": to_str(get_in(raw, ["picture", "thumbnail"])),
        "id_name": to_str(get_in(raw, ["id", "name"])),
        "id_value": to_str(get_in(raw, ["id", "value"])),
    }


def to_ndjson_bytes(records: List[dict]) -> bytes:
    lines = [json.dumps(r, default=str, ensure_ascii=False) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")


def parquet_schema() -> "pa.Schema":
    # Esquema explícito (nullable). Strings donde hay riesgo de mezcla.
    return pa.schema([
        pa.field("gender", pa.string()),
        pa.field("first_name", pa.string()),
        pa.field("last_name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("country", pa.string()),
        pa.field("state", pa.string()),
        pa.field("city", pa.string()),
        pa.field("postcode", pa.string()),
        pa.field("street_number", pa.int64()),
        pa.field("street_name", pa.string()),
        pa.field("phone", pa.string()),
        pa.field("cell", pa.string()),
        pa.field("nat", pa.string()),
        pa.field("dob", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("registered_date", pa.string()),
        pa.field("registered_age", pa.int64()),
        pa.field("uuid", pa.string()),
        pa.field("username", pa.string()),
        pa.field("picture_large", pa.string()),
        pa.field("picture_medium", pa.string()),
        pa.field("picture_thumbnail", pa.string()),
        pa.field("id_name", pa.string()),
        pa.field("id_value", pa.string()),
    ])


def cast_for_arrow(record: dict) -> dict:
    """
    Asegura que ints realmente sean ints (o None), y strings sean strings.
    Si un campo int viene como string numérica, lo convertimos a int; si no, lo dejamos None.
    """
    def to_int_or_none(v):
        if v is None:
            return None
        if isinstance(v, int):
            return v
        try:
            # Evita floats en edades/nums
            return int(str(v))
        except Exception:
            return None

    rec = dict(record)  # shallow copy
    rec["street_number"] = to_int_or_none(rec.get("street_number"))
    rec["age"] = to_int_or_none(rec.get("age"))
    rec["registered_age"] = to_int_or_none(rec.get("registered_age"))
    # El resto ya va como string/None por normalize_item
    return rec


def to_parquet_bytes(records: List[dict]) -> bytes:
    if not HAS_PYARROW:
        raise RuntimeError("pyarrow not available for parquet conversion")
    sch = parquet_schema()
    casted = [cast_for_arrow(r) for r in records]
    table = pa.Table.from_pylist(casted, schema=sch)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    return buf.read()


def upload_bytes(bucket: str, key: str, data: bytes, content_type: Optional[str] = None):
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    s3.put_object(Bucket=bucket, Key=key, Body=data, **extra)


# -------- Handler --------
def handler(event, context):
    if not BUCKET:
        logger.error("Missing BUCKET env var")
        raise RuntimeError("BUCKET env var required")
    if not API_URL:
        logger.error("Missing API_URL env var")
        raise RuntimeError("API_URL env var required")

    logger.info("Invoked. API=%s format=%s", API_URL, FILE_FORMAT)

    try:
        sess = http_session_with_retries()
        resp = sess.get(API_URL, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logger.exception("Failed to fetch API: %s", e)
        raise

    items = payload.get("results") or payload.get("data") or payload
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        logger.error("Unexpected payload shape: %s", type(items))
        raise RuntimeError("Unexpected payload shape")

    # Normalización sin validaciones de esquema
    normalized = [normalize_item(x) for x in items if isinstance(x, dict)]

    if not normalized:
        logger.warning("No valid records after fetch")
        return {"status": "no_records", "fetched": len(items)}

    ts = int(time.time())
    base = f"{PREFIX.rstrip('/')}/users_{ts}"
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


if __name__ == "__main__":
    handler({}, {})
