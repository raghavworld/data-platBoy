from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID

import boto3
import pymongo
import trino
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
STATE_DIR = DATA_DIR / "state"
LOG_DIR = DATA_DIR / "logs"
ENV_PATH = PROJECT_ROOT / ".env"

SENSITIVE_VALUE_PATTERNS = [
    re.compile(r"(mongodb(?:\+srv)?://[^:/@\s]+:)([^@\s]+)(@)", re.IGNORECASE),
    re.compile(r"((?:password|secret|token|key)=)([^&\s]+)()", re.IGNORECASE),
]

SENSITIVE_FIELD_NAMES = {"password", "secret", "token", "api_key", "access_key", "secret_key", "mongo_uri", "key_hash"}


def load_environment() -> None:
    load_dotenv(ENV_PATH)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if value.__class__.__name__ == "Timestamp" and hasattr(value, "isoformat"):
        return value.isoformat()
    if value.__class__.__name__ == "ObjectId":
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_safe(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    return value


def find_non_json_safe_fields(value: Any, path: str = "$") -> list[dict[str, str]]:
    primitive = (str, int, float, bool, type(None))
    if isinstance(value, primitive):
        return []
    if isinstance(value, dict):
        findings: list[dict[str, str]] = []
        for key, inner in value.items():
            findings.extend(find_non_json_safe_fields(inner, f"{path}.{key}"))
        return findings
    if isinstance(value, (list, tuple, set)):
        findings = []
        for index, inner in enumerate(value):
            findings.extend(find_non_json_safe_fields(inner, f"{path}[{index}]"))
        return findings
    return [{"field": path, "type": f"{value.__class__.__module__}.{value.__class__.__name__}", "value": repr(value)}]


def safe_json_dumps(value: Any, **kwargs: Any) -> str:
    try:
        return json.dumps(json_safe(value), **kwargs)
    except TypeError as exc:
        offenders = find_non_json_safe_fields(value)
        print(json.dumps({"event": "json_serialization_failed", "error": str(exc), "fields": offenders[:25]}, sort_keys=True))
        raise


def setup_logging(name: str) -> logging.Logger:
    load_environment()
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = JsonLogFormatter(service=name)

    file_handler = logging.FileHandler(LOG_DIR / f"{name}.log")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def mask_sensitive_text(value: str) -> str:
    masked = value
    for pattern in SENSITIVE_VALUE_PATTERNS:
        masked = pattern.sub(r"\1***\3", masked)
    return masked


def mask_sensitive_payload(value: Any) -> Any:
    if isinstance(value, dict):
        masked: dict[str, Any] = {}
        for key, inner in value.items():
            lowered = str(key).lower()
            if lowered in SENSITIVE_FIELD_NAMES or any(name in lowered for name in SENSITIVE_FIELD_NAMES):
                masked[key] = "***"
            else:
                masked[key] = mask_sensitive_payload(inner)
        return masked
    if isinstance(value, list):
        return [mask_sensitive_payload(item) for item in value]
    if isinstance(value, str):
        return mask_sensitive_text(value)
    return value


class JsonLogFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).replace(microsecond=0).isoformat(),
            "service": self.service,
            "level": record.levelname.lower(),
            "operation": record.funcName,
            "message": mask_sensitive_text(record.getMessage()),
        }
        if hasattr(record, "layer"):
            payload["layer"] = getattr(record, "layer")
        if hasattr(record, "duration"):
            payload["duration"] = getattr(record, "duration")
        if hasattr(record, "status"):
            payload["status"] = getattr(record, "status")
        if record.exc_info:
            payload["error"] = mask_sensitive_text("".join(traceback.format_exception(*record.exc_info)).strip())
        return safe_json_dumps(mask_sensitive_payload(payload), sort_keys=True)


@dataclass(frozen=True)
class MongoSource:
    service_name: str
    database_name: str
    collection_name: str

    @property
    def state_path(self) -> Path:
        return STATE_DIR / f"{self.database_name}_{self.collection_name}.json"

    @property
    def raw_prefix(self) -> str:
        return f"python/{self.database_name}/{self.collection_name}"


def mongo_sources() -> list[MongoSource]:
    return [
        MongoSource("mongo-users", os.environ["MONGO_USERS_DB"], "users"),
        MongoSource("mongo-orders", os.environ["MONGO_ORDERS_DB"], "orders"),
        MongoSource("mongo-products", os.environ["MONGO_PRODUCTS_DB"], "products"),
        MongoSource("mongo-payments", os.environ["MONGO_PAYMENTS_DB"], "payments"),
    ]


def mongo_client(service_name: str) -> pymongo.MongoClient:
    user = os.environ["MONGO_ROOT_USERNAME"]
    password = os.environ["MONGO_ROOT_PASSWORD"]
    uri = f"mongodb://{user}:{password}@{service_name}:27017/admin?authSource=admin"
    return pymongo.MongoClient(uri, tz_aware=True)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
    )


def trino_connection(
    catalog: str = "delta",
    schema: str = "bronze",
    user: str | None = None,
    session_properties: dict[str, str] | None = None,
):
    requested_user = user or os.environ.get("TRINO_USER", "trino")
    allowlist = {
        os.environ.get("TRINO_USER", "trino"),
        os.environ.get("SUPERSET_TRINO_USER", "superset"),
        "airflow",
        "admin",
    }
    strict_user_mode = os.environ.get("TRINO_STRICT_USER_ALLOWLIST", "true").lower() == "true"
    if strict_user_mode and requested_user not in allowlist:
        raise ValueError(f"Blocked Trino user override: {requested_user}")
    return trino.dbapi.connect(
        host="trino",
        port=8080,
        user=requested_user,
        catalog=catalog,
        schema=schema,
        session_properties=session_properties or {},
    )


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(safe_json_dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def serialize_document(document: dict[str, Any]) -> dict[str, Any]:
    def convert(value: Any) -> Any:
        if isinstance(value, datetime):
            return isoformat(value)
        if isinstance(value, list):
            return [convert(item) for item in value]
        if isinstance(value, dict):
            return {key: convert(inner) for key, inner in value.items()}
        return str(value) if value.__class__.__name__ == "ObjectId" else value

    return {key: convert(value) for key, value in document.items()}


def collect_schema_paths(record: Any, prefix: str = "") -> set[str]:
    schema_paths: set[str] = set()
    if isinstance(record, dict):
        for key, value in record.items():
            path = f"{prefix}.{key}" if prefix else key
            schema_paths.add(path)
            schema_paths |= collect_schema_paths(value, path)
    elif isinstance(record, list):
        schema_paths.add(f"{prefix}[]")
        for item in record:
            schema_paths |= collect_schema_paths(item, f"{prefix}[]")
    return schema_paths


def hash_strings(values: Iterable[str]) -> str:
    digest = hashlib.md5()
    for value in values:
        digest.update(value.encode("utf-8"))
    return digest.hexdigest()[:12]


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def safe_identifier(*parts: str, max_length: int = 120) -> str:
    value = "__".join(part for part in parts if part)
    value = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()
    if not value:
        value = "object"
    if value[0].isdigit():
        value = f"t_{value}"
    return value[:max_length]


def bronze_table_name(database_name: str, collection_name: str) -> str:
    return safe_identifier(database_name, collection_name)


def silver_table_name(name: str) -> str:
    return safe_identifier(name)


def ensure_bucket(bucket_name: str) -> None:
    client = s3_client()
    existing = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if bucket_name not in existing:
        client.create_bucket(Bucket=bucket_name)


def run_subprocess(command: list[str], logger: logging.Logger) -> None:
    logger.info("Running command: %s", " ".join(command))
    subprocess.run(command, check=True)


def spark_submit_command() -> list[str]:
    return [
        "spark-submit",
        "--master",
        "local[2]",
        "--packages",
        "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4",
        "/opt/platform/scripts/bronze_raw_to_delta.py",
    ]


def silver_spark_submit_command() -> list[str]:
    return [
        "spark-submit",
        "--master",
        "local[2]",
        "--packages",
        "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4",
        "/opt/platform/scripts/silver_bronze_to_delta.py",
    ]
