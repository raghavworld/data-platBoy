from __future__ import annotations

import gzip
import json
import os
import re
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

import pymongo
from bson import ObjectId

from common import collect_schema_paths, hash_strings, s3_client, serialize_document, setup_logging, utc_now
from dashboard_db import (
    dashboard_connection,
    dashboard_schema_lock,
    ensure_raw_run,
    ensure_raw_ingestion_batches,
    fetch_active_sources,
    finish_raw_ingestion_batch,
    finish_raw_run,
    insert_collection_run_status,
    insert_raw_batch_fingerprint,
    insert_raw_file,
    insert_schema_snapshot,
    latest_collection_state,
    latest_schema_fields,
    raw_batch_fingerprint_exists,
    raw_batch_status_counts,
    record_raw_run_event,
    record_raw_run_timing,
    raw_run_progress_details,
    set_raw_run_retry_lineage,
    set_raw_run_scope,
    start_raw_ingestion_batch,
    update_raw_run_progress,
    upsert_collection_state,
    upsert_raw_database_progress,
)
from raw_schedule import calculate_next_scheduled_run, cursor_timestamp, effective_schedule, schedule_is_due
from source_secrets import get_secret


TIMESTAMP_CURSOR_CANDIDATES = [
    "updatedAt",
    "updated_at",
    "modifiedAt",
    "modified_at",
    "lastModified",
    "last_modified",
    "createdAt",
    "created_at",
]
AUTO_CURSOR_VALUES = {"", "auto", "AUTO", "__auto__"}
OBJECT_ID_CURSOR_FIELD = "_id"
SNAPSHOT_CURSOR_FIELD = "__snapshot_fingerprint__"
INCREMENTAL_TIMESTAMP = "incremental_timestamp"
INCREMENTAL_OBJECTID = "incremental_objectid"
SNAPSHOT_FINGERPRINT = "snapshot_fingerprint"


@dataclass(frozen=True)
class CursorPlan:
    configured_cursor_field: str
    detected_cursor_field: str | None
    effective_cursor_field: str
    ingestion_strategy: str
    cursor_warning: str | None = None
    value_kind: str = "datetime"


@dataclass(frozen=True)
class RawBatchConfig:
    batch_size: int = 1000
    max_batches_per_run: int | None = None
    parallel_collections: int = 1
    parallel_batches: int = 1


def int_env(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name) or default))
    except ValueError:
        return default


def raw_batch_config() -> RawBatchConfig:
    max_batches_text = (os.environ.get("RAW_MAX_BATCHES_PER_RUN") or "").strip()
    max_batches = None
    if max_batches_text:
        try:
            max_batches = max(1, int(max_batches_text))
        except ValueError:
            max_batches = None
    return RawBatchConfig(
        batch_size=int_env("RAW_BATCH_SIZE", 1000),
        max_batches_per_run=max_batches,
        parallel_collections=int_env("RAW_PARALLEL_COLLECTIONS", 1),
        parallel_batches=int_env("RAW_PARALLEL_BATCHES", 1),
    )


def env_truthy(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def safe_batch_id(raw_run_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw_run_id).strip("_")
    return cleaned[:120] or uuid.uuid4().hex


def progress_step_delay() -> None:
    try:
        delay = float(os.environ.get("ONOV8_RAW_PROGRESS_STEP_DELAY_SECONDS") or 0)
    except ValueError:
        delay = 0
    if delay > 0:
        time.sleep(min(delay, 5.0))


def elapsed_since(started: float) -> float:
    return max(0.0, time.monotonic() - started)


def add_elapsed(timings: dict[str, float], key: str, started: float) -> None:
    timings[key] = timings.get(key, 0.0) + elapsed_since(started)


def wait_if_queue_paused(run_id: str, logger) -> None:
    while raw_run_progress_details(run_id).get("queue_paused"):
        logger.info("RAW queue paused for run %s; waiting before starting next batch", run_id)
        try:
            poll_seconds = float(os.environ.get("ONOV8_RAW_QUEUE_PAUSE_POLL_SECONDS", "2"))
        except ValueError:
            poll_seconds = 2.0
        time.sleep(max(0.5, min(poll_seconds, 30.0)))


def raw_run_cancel_requested(run_id: str) -> bool:
    return bool(raw_run_progress_details(run_id).get("cancel_requested"))


def normalize_json_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item) for item in value if str(item)]


def source_mongo_uri(source: dict[str, Any]) -> str:
    secret = get_secret(source.get("secret_reference"))
    if secret.get("mongo_uri"):
        return str(secret["mongo_uri"])

    config = source.get("connection_config_json") or {}
    if isinstance(config, str):
        config = json.loads(config)
    host = config.get("host")
    port = config.get("port", 27017)
    username = config.get("username")
    auth_database = source.get("auth_database") or config.get("auth_database") or "admin"
    if username:
        raise RuntimeError(f"Source {source['source_name']} is missing a secret_reference for authenticated MongoDB")
    if not host:
        raise RuntimeError(f"Source {source['source_name']} is missing MongoDB host configuration")
    return f"mongodb://{host}:{port}/{auth_database}"


def source_config(source: dict[str, Any]) -> dict[str, Any]:
    config = source.get("connection_config_json") or {}
    if isinstance(config, str):
        return json.loads(config)
    return config


def is_auto_cursor(value: str | None) -> bool:
    return value is None or str(value).strip().lower() in {item.lower() for item in AUTO_CURSOR_VALUES}


def configured_cursor_for_collection(source: dict[str, Any], collection_name: str) -> str:
    config = source_config(source)
    overrides = config.get("collection_cursor_fields") or {}
    if isinstance(overrides, str):
        try:
            overrides = json.loads(overrides)
        except json.JSONDecodeError:
            overrides = {}
    collection_cursor = str(overrides.get(collection_name) or "").strip()
    if collection_cursor:
        return collection_cursor
    return str(source.get("cursor_field") or "AUTO").strip() or "AUTO"


def parse_datetime_like(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def timestamp_value_kind(value: Any) -> str:
    return "datetime" if isinstance(value, datetime) else "string"


def usable_timestamp_value(value: Any) -> bool:
    return parse_datetime_like(value) is not None


def timestamp_field_stats(documents: list[dict[str, Any]], field_name: str) -> dict[str, Any]:
    present = [document.get(field_name) for document in documents if field_name in document]
    usable = [value for value in present if usable_timestamp_value(value)]
    return {
        "present": len(present),
        "usable": len(usable),
        "missing": max(len(documents) - len(present), 0),
        "kind": timestamp_value_kind(usable[0]) if usable else "datetime",
    }


def objectid_is_usable(documents: list[dict[str, Any]]) -> bool:
    return bool(documents) and all(isinstance(document.get("_id"), ObjectId) for document in documents)


def detect_cursor_plan_from_documents(documents: list[dict[str, Any]], configured_cursor_field: str | None) -> CursorPlan:
    configured = str(configured_cursor_field or "AUTO").strip() or "AUTO"
    if not documents:
        return CursorPlan(
            configured_cursor_field=configured,
            detected_cursor_field=None,
            effective_cursor_field=SNAPSHOT_CURSOR_FIELD,
            ingestion_strategy=SNAPSHOT_FINGERPRINT,
            cursor_warning="Collection is empty; cursor detection will retry when documents arrive.",
            value_kind="snapshot",
        )

    candidate_fields = [configured] if not is_auto_cursor(configured) else []
    for candidate in TIMESTAMP_CURSOR_CANDIDATES:
        if candidate not in candidate_fields:
            candidate_fields.append(candidate)

    configured_stats = timestamp_field_stats(documents, configured) if not is_auto_cursor(configured) else None
    if configured_stats and configured_stats["usable"]:
        warning = None
        if configured_stats["missing"]:
            warning = f"{configured_stats['missing']} sampled document(s) are missing configured cursor {configured}; ingesting current batch with fallback tolerance."
        return CursorPlan(
            configured_cursor_field=configured,
            detected_cursor_field=configured,
            effective_cursor_field=configured,
            ingestion_strategy=INCREMENTAL_TIMESTAMP,
            cursor_warning=warning,
            value_kind=configured_stats["kind"],
        )

    for candidate in TIMESTAMP_CURSOR_CANDIDATES:
        stats = timestamp_field_stats(documents, candidate)
        if not stats["usable"]:
            continue
        warnings = []
        if not is_auto_cursor(configured) and candidate != configured:
            warnings.append(f"Configured cursor {configured} was not usable; detected {candidate}.")
        if stats["missing"]:
            warnings.append(f"{stats['missing']} sampled document(s) are missing {candidate}.")
        return CursorPlan(
            configured_cursor_field=configured,
            detected_cursor_field=candidate,
            effective_cursor_field=candidate,
            ingestion_strategy=INCREMENTAL_TIMESTAMP,
            cursor_warning=" ".join(warnings) or None,
            value_kind=stats["kind"],
        )

    if objectid_is_usable(documents):
        warning = None if is_auto_cursor(configured) else f"Configured cursor {configured} was not usable; using ObjectId _id timestamp fallback."
        return CursorPlan(
            configured_cursor_field=configured,
            detected_cursor_field=OBJECT_ID_CURSOR_FIELD,
            effective_cursor_field=OBJECT_ID_CURSOR_FIELD,
            ingestion_strategy=INCREMENTAL_OBJECTID,
            cursor_warning=warning,
            value_kind="objectid",
        )

    warning = None if is_auto_cursor(configured) else f"Configured cursor {configured} was not usable and _id is not ObjectId; using snapshot fingerprint mode."
    return CursorPlan(
        configured_cursor_field=configured,
        detected_cursor_field=None,
        effective_cursor_field=SNAPSHOT_CURSOR_FIELD,
        ingestion_strategy=SNAPSHOT_FINGERPRINT,
        cursor_warning=warning or "No timestamp cursor or ObjectId _id cursor was found; using snapshot fingerprint mode.",
        value_kind="snapshot",
    )


def sample_documents(collection, limit: int = 200) -> list[dict[str, Any]]:
    try:
        return list(collection.find({}).sort([("_id", 1)]).limit(limit))
    except Exception:
        return list(collection.find({}).limit(limit))


def parse_cursor_value(value: str | None, value_kind: str = "datetime") -> Any:
    if not value:
        return None
    if value_kind == "string":
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value


def objectid_from_cursor(value: str | None) -> ObjectId | None:
    if not value:
        return None
    token = decode_cursor_token(value)
    if token and token.get("kind") == "objectid":
        value = token.get("value")
    try:
        return ObjectId(value)
    except Exception:
        parsed = parse_cursor_value(value)
        if isinstance(parsed, datetime):
            return ObjectId.from_datetime(parsed)
    return None


def decode_cursor_token(value: str | None) -> dict[str, Any] | None:
    if not value or not str(value).strip().startswith("{"):
        return None
    try:
        token = json.loads(value)
    except json.JSONDecodeError:
        return None
    return token if isinstance(token, dict) else None


def document_cursor_token(document: dict[str, Any], plan: CursorPlan) -> str:
    if plan.ingestion_strategy == INCREMENTAL_OBJECTID:
        return str(document.get("_id"))
    if plan.ingestion_strategy == INCREMENTAL_TIMESTAMP:
        return json.dumps(
            {
                "kind": "timestamp",
                "field": plan.effective_cursor_field,
                "value": cursor_to_text(document.get(plan.effective_cursor_field)),
                "_id": str(document.get("_id")) if isinstance(document.get("_id"), ObjectId) else None,
            },
            separators=(",", ":"),
        )
    if "_id" in document:
        return str(document.get("_id"))
    return document_fingerprint_text(document)


def build_incremental_query(last_cursor_value: str | None, plan: CursorPlan) -> dict[str, Any]:
    if not last_cursor_value:
        return {}
    if plan.ingestion_strategy == INCREMENTAL_OBJECTID:
        last_object_id = objectid_from_cursor(last_cursor_value)
        return {"_id": {"$gt": last_object_id}} if last_object_id is not None else {}
    if plan.ingestion_strategy != INCREMENTAL_TIMESTAMP:
        return {}
    token = decode_cursor_token(last_cursor_value)
    parsed = parse_cursor_value(token.get("value") if token else last_cursor_value, plan.value_kind)
    if parsed is None:
        return {}
    if token and token.get("_id"):
        try:
            token_object_id = ObjectId(str(token["_id"]))
            return {
                "$or": [
                    {plan.effective_cursor_field: {"$gt": parsed}},
                    {plan.effective_cursor_field: parsed, "_id": {"$gt": token_object_id}},
                ]
            }
        except Exception:
            pass
    return {plan.effective_cursor_field: {"$gt": parsed}}


def available_collections(database) -> list[str]:
    return [
        name
        for name in database.list_collection_names()
        if not name.startswith("system.")
    ]


def active_collection_inventory(source: dict[str, Any]) -> list[dict[str, Any]]:
    active = source.get("active_collections_json") or []
    if isinstance(active, str):
        try:
            active = json.loads(active)
        except json.JSONDecodeError:
            active = []
    rows: list[dict[str, Any]] = []
    for item in active:
        if isinstance(item, dict):
            name = str(item.get("collection_name") or "").strip()
            if name:
                rows.append({**item, "collection_name": name})
        elif str(item).strip():
            rows.append({"collection_name": str(item).strip()})
    return rows


def planned_collection_inventory(source: dict[str, Any], target_collection_name: str | None = None) -> list[dict[str, Any]]:
    rows = active_collection_inventory(source)
    if target_collection_name:
        rows = [row for row in rows if row["collection_name"] == target_collection_name]
    return rows


def scheduled_due_sources(sources: list[dict[str, Any]], target_collection_name: str | None, logger) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    due_sources: list[dict[str, Any]] = []
    for source in sources:
        due_collections = []
        for collection in planned_collection_inventory(source, target_collection_name):
            schedule = effective_schedule(source, collection)
            if schedule_is_due(schedule, now):
                due_collections.append({**collection, "effective_schedule": schedule})
        if due_collections:
            due_sources.append({**source, "active_collections_json": due_collections})
        else:
            logger.info("No scheduled RAW collections are due for %s", source.get("source_name") or source.get("database_name"))
    return due_sources


def mark_scheduled_collection_run(source: dict[str, Any], collection_info: dict[str, Any]) -> None:
    schedule = collection_info.get("effective_schedule") or effective_schedule(source, collection_info)
    if not schedule.get("override_enabled"):
        return
    schedule_type = schedule.get("schedule_type") or "manual_only"
    cron = schedule.get("cron")
    next_run_at = calculate_next_scheduled_run(schedule_type, cron)
    now = datetime.now(timezone.utc)
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            if schedule.get("level") == "collection":
                cursor.execute(
                    """
                    UPDATE source_connection_collections
                    SET last_scheduled_run_at = %s,
                        next_scheduled_run_at = %s,
                        updated_at = now()
                    WHERE source_id = %s
                      AND collection_name = %s
                    """,
                    (now, next_run_at, source["id"], collection_info["collection_name"]),
                )
            elif schedule.get("level") == "database":
                cursor.execute(
                    """
                    UPDATE source_connections
                    SET last_scheduled_run_at = %s,
                        next_scheduled_run_at = %s,
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (now, next_run_at, source["id"]),
                )


def estimated_records_for_inventory(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        try:
            total += int(row.get("record_count") or 0)
        except (TypeError, ValueError):
            continue
    return total


def selected_collections(source: dict[str, Any], database, target_collection_name: str | None = None) -> list[dict[str, Any]]:
    discovered = set(available_collections(database))
    active = planned_collection_inventory(source, target_collection_name)
    selected: list[dict[str, Any]] = []
    for item in active:
        name = item["collection_name"]
        selected.append({**item, "exists_in_source": name in discovered})
    return selected


def schema_summary(documents: list[dict[str, Any]]) -> dict[str, Any]:
    fields = sorted(
        {
            path
            for document in documents
            for path in collect_schema_paths(serialize_document(document))
        }
    )
    return {
        "fields": fields,
        "top_level": sorted({field.split(".")[0].replace("[]", "") for field in fields}),
        "nested": sorted([field for field in fields if "." in field or "[]" in field]),
    }


def detect_schema(source: dict[str, Any], database_name: str, collection_name: str, documents: list[dict[str, Any]], logger) -> None:
    sample_documents = documents[:200]
    summary = schema_summary(sample_documents)
    fields = summary["fields"]
    schema_hash = hash_strings(fields) if fields else "empty"
    previous_fields = latest_schema_fields(str(source["id"]), database_name, collection_name)
    previous_set = set(previous_fields)
    current_set = set(fields)

    if not previous_fields:
        change_type = "initial"
    elif previous_set == current_set:
        change_type = "unchanged"
    else:
        change_type = "changed"

    new_fields = sorted(current_set - previous_set)
    removed_fields = sorted(previous_set - current_set)
    insert_schema_snapshot(
        source_id=str(source["id"]),
        database_name=database_name,
        collection_name=collection_name,
        schema_hash=schema_hash,
        fields_json=summary,
        new_fields=new_fields,
        removed_fields=removed_fields,
        change_type=change_type,
    )
    if change_type in {"initial", "changed"}:
        logger.info(
            "Schema %s for %s.%s hash=%s new=%s removed=%s",
            change_type,
            database_name,
            collection_name,
            schema_hash,
            new_fields,
            removed_fields,
        )


def cursor_to_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def document_fingerprint_text(document: dict[str, Any]) -> str:
    return json.dumps(serialize_document(document), sort_keys=True, separators=(",", ":"))


def snapshot_fingerprint(documents: list[dict[str, Any]]) -> str:
    return hash_strings([document_fingerprint_text(document) for document in documents]) if documents else "empty"


def batch_fingerprint(documents: list[dict[str, Any]], plan: CursorPlan) -> dict[str, Any]:
    if plan.ingestion_strategy == SNAPSHOT_FINGERPRINT:
        checksum = snapshot_fingerprint(documents)
        return {
            "min_cursor_value": document_cursor_token(documents[0], plan) if documents else checksum,
            "max_cursor_value": document_cursor_token(documents[-1], plan) if documents else checksum,
            "row_count": len(documents),
            "batch_checksum": checksum,
        }

    if plan.ingestion_strategy == INCREMENTAL_OBJECTID:
        object_ids = [document.get("_id") for document in documents if isinstance(document.get("_id"), ObjectId)]
        if not object_ids:
            checksum = snapshot_fingerprint(documents)
            return {
                "min_cursor_value": document_cursor_token(documents[0], plan) if documents else checksum,
                "max_cursor_value": document_cursor_token(documents[-1], plan) if documents else checksum,
                "row_count": len(documents),
                "batch_checksum": checksum,
            }
        checksum_parts = [f"{str(document.get('_id'))}|{document_fingerprint_text(document)}" for document in documents]
        return {
            "min_cursor_value": document_cursor_token(documents[0], plan),
            "max_cursor_value": document_cursor_token(documents[-1], plan),
            "row_count": len(documents),
            "batch_checksum": hash_strings(checksum_parts),
        }

    cursor_field = plan.effective_cursor_field
    cursor_values = [cursor_to_text(document.get(cursor_field)) for document in documents if document.get(cursor_field) is not None]
    if not cursor_values:
        checksum = snapshot_fingerprint(documents)
        return {
            "min_cursor_value": document_cursor_token(documents[0], plan) if documents else checksum,
            "max_cursor_value": document_cursor_token(documents[-1], plan) if documents else checksum,
            "row_count": len(documents),
            "batch_checksum": checksum,
        }

    checksum_parts = [
        f"{str(document.get('_id'))}|{cursor_to_text(document.get(cursor_field))}|{document_fingerprint_text(document)}"
        for document in documents
    ]
    return {
        "min_cursor_value": document_cursor_token(documents[0], plan),
        "max_cursor_value": document_cursor_token(documents[-1], plan),
        "row_count": len(documents),
        "batch_checksum": hash_strings(checksum_parts),
    }


def upload_batch(
    *,
    source: dict[str, Any],
    database_name: str,
    collection_name: str,
    documents: list[dict[str, Any]],
    airflow_run_id: str,
    batch_number: int,
    logger,
) -> tuple[str, int]:
    now = utc_now()
    bucket_name = os.environ["MINIO_BUCKET_RAW"]
    key = (
        f"python/{database_name}/{collection_name}/dt={now:%Y-%m-%d}/hr={now:%H}/"
        f"batch_{safe_batch_id(airflow_run_id)}_part_{batch_number:06d}.jsonl.gz"
    )

    buffer = BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        for document in documents:
            serialized = serialize_document(document)
            serialized["__source_id"] = str(source["id"])
            serialized["__source_name"] = source["source_name"]
            serialized["__source_db"] = database_name
            serialized["__source_collection"] = collection_name
            serialized["__extracted_at"] = now.replace(microsecond=0).isoformat()
            gz_file.write(json.dumps(serialized, separators=(",", ":")).encode("utf-8"))
            gz_file.write(b"\n")

    payload = buffer.getvalue()
    s3_client().put_object(Bucket=bucket_name, Key=key, Body=payload)
    head = s3_client().head_object(Bucket=bucket_name, Key=key)
    remote_size = int(head.get("ContentLength") or 0)
    if remote_size != len(payload):
        raise RuntimeError(f"Uploaded RAW object size mismatch for {key}: expected {len(payload)} bytes, found {remote_size}")
    logger.info("Uploaded raw batch s3://%s/%s with %s rows", bucket_name, key, len(documents))
    return key, len(payload)


def validate_raw_object_rows(bucket_name: str, object_key: str, expected_rows: int) -> int:
    response = s3_client().get_object(Bucket=bucket_name, Key=object_key)
    row_count = 0
    with gzip.GzipFile(fileobj=response["Body"], mode="rb") as gz_file:
        for line_number, raw_line in enumerate(gz_file, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                json.loads(line.decode("utf-8"))
            except Exception as exc:
                raise RuntimeError(f"Existing RAW object {object_key} has invalid JSON at line {line_number}: {exc}") from exc
            row_count += 1
    if row_count != int(expected_rows or 0):
        raise RuntimeError(f"Existing RAW object {object_key} row count mismatch: expected {expected_rows}, found {row_count}")
    return row_count


def reusable_raw_object_size(bucket_name: str, object_key: str, expected_rows: int, logger) -> int | None:
    if not object_key:
        return None
    try:
        head = s3_client().head_object(Bucket=bucket_name, Key=object_key)
        validate_raw_object_rows(bucket_name, object_key, expected_rows)
        return int(head.get("ContentLength") or 0)
    except Exception as exc:
        logger.warning("RAW object %s could not be reused safely: %s", object_key, exc)
        return None


def sorted_documents(collection, query: dict[str, Any], plan: CursorPlan) -> list[dict[str, Any]]:
    if plan.ingestion_strategy == INCREMENTAL_TIMESTAMP:
        sort_order = [(plan.effective_cursor_field, pymongo.ASCENDING), ("_id", pymongo.ASCENDING)]
    else:
        sort_order = [("_id", pymongo.ASCENDING)]
    try:
        return list(collection.find(query).sort(sort_order))
    except Exception:
        return list(collection.find(query))


def batch_sort_order(plan: CursorPlan) -> list[tuple[str, int]]:
    if plan.ingestion_strategy == INCREMENTAL_TIMESTAMP:
        return [(plan.effective_cursor_field, pymongo.ASCENDING), ("_id", pymongo.ASCENDING)]
    return [("_id", pymongo.ASCENDING)]


def fetch_documents_for_batch(
    collection,
    plan: CursorPlan,
    cursor_value: str | None,
    batch_size: int,
    batch_number: int,
) -> list[dict[str, Any]]:
    if plan.ingestion_strategy == SNAPSHOT_FINGERPRINT:
        # Last-resort mode for collections without a usable timestamp or ObjectId cursor.
        # It is intentionally bounded by RAW_BATCH_SIZE, but skip/limit gets slower on
        # very large collections. Configure a cursor where possible.
        return list(collection.find({}).sort(batch_sort_order(plan)).skip((batch_number - 1) * batch_size).limit(batch_size))
    query = build_incremental_query(cursor_value, plan)
    try:
        return list(collection.find(query).sort(batch_sort_order(plan)).limit(batch_size))
    except Exception:
        return list(collection.find(query).limit(batch_size))


def state_kwargs(plan: CursorPlan, last_snapshot_fingerprint: str | None = None) -> dict[str, Any]:
    return {
        "cursor_field": plan.effective_cursor_field,
        "configured_cursor_field": plan.configured_cursor_field,
        "detected_cursor_field": plan.detected_cursor_field,
        "ingestion_strategy": plan.ingestion_strategy,
        "cursor_warning": plan.cursor_warning,
        "last_snapshot_fingerprint": last_snapshot_fingerprint,
    }


def recommended_fix_for_raw_error(exc: Exception) -> str:
    lowered = str(exc).lower()
    if "not authorized" in lowered or "auth" in lowered:
        return "Check the Mongo credentials and grants for this source, then retry the selected collection."
    if "timed out" in lowered or "timeout" in lowered or "serverselection" in lowered:
        return "Confirm the Mongo source is reachable from the Airflow container and retry with the source online."
    if "collection" in lowered and "not found" in lowered:
        return "Refresh Source Connections inventory, deactivate missing collections, then retry RAW."
    return "Inspect the collection error, refresh inventory/cursor detection, and retry RAW for this collection."


def process_collection(
    source: dict[str, Any],
    database,
    collection_name: str,
    run_id: str,
    airflow_run_id: str,
    logger,
    batch_config: RawBatchConfig,
    progress: dict[str, int] | None = None,
    progress_callback=None,
) -> dict[str, int]:
    started_at = utc_now()
    collection_started_monotonic = time.monotonic()
    database_name = source["database_name"]
    collection = database[collection_name]
    source_id = str(source["id"])
    collection_timings = {
        "source_connect_seconds": 0.0,
        "query_seconds": 0.0,
        "file_write_seconds": 0.0,
        "metadata_update_seconds": 0.0,
    }
    totals = {
        "rows_found": 0,
        "rows_written": 0,
        "files_written": 0,
        "no_new": 0,
        "duplicate": 0,
        "warnings": 0,
        "failed": 0,
        "batches_total": 0,
        "batches_completed": 0,
        "batches_failed": 0,
        "batches_skipped": 0,
        "cancelled": 0,
    }

    def emit_phase(phase: str, message: str | None = None, batch_number: int | None = None) -> None:
        if progress_callback:
            progress_callback(phase, message, batch_number)

    emit_phase("reading source", f"Reading {database_name}.{collection_name}")
    record_raw_run_event(run_id, "started", f"Collection started: {database_name}.{collection_name}", database_name=database_name, collection_name=collection_name)
    query_started = time.monotonic()
    sample = sample_documents(collection)
    configured_cursor = configured_cursor_for_collection(source, collection_name)
    plan = detect_cursor_plan_from_documents(sample, configured_cursor)
    detect_schema(source, database_name, collection_name, sample, logger)

    state = latest_collection_state(source_id, database_name, collection_name)
    previous_cursor_value = state.get("last_cursor_value")
    query = build_incremental_query(previous_cursor_value, plan)
    estimated_rows = collection.count_documents({} if plan.ingestion_strategy == SNAPSHOT_FINGERPRINT else query)
    add_elapsed(collection_timings, "query_seconds", query_started)
    total_batches = max(1, ceil(estimated_rows / batch_config.batch_size)) if estimated_rows else 1
    if batch_config.max_batches_per_run is not None and progress is not None:
        remaining_run_batches = max(0, batch_config.max_batches_per_run - int(progress.get("scheduled_batches", 0)))
        total_batches = max(0, min(total_batches, remaining_run_batches))
    if total_batches <= 0:
        insert_collection_run_status(
            run_id=run_id,
            source_id=source_id,
            database_name=database_name,
            collection_name=collection_name,
            status="skipped",
            rows_found=0,
            rows_written=0,
            previous_cursor_value=previous_cursor_value,
            new_cursor_value=previous_cursor_value,
            cursor_strategy=plan.ingestion_strategy,
            message="RAW_MAX_BATCHES_PER_RUN reached before this collection was processed",
            started_at=started_at,
            finished_at=utc_now(),
        )
        record_raw_run_timing(
            run_id=run_id,
            level="collection",
            database_name=database_name,
            collection_name=collection_name,
            query_seconds=collection_timings.get("query_seconds", 0),
            total_duration_seconds=elapsed_since(collection_started_monotonic),
            warning_message="RAW_MAX_BATCHES_PER_RUN reached before this collection was processed",
        )
        return {**totals, "no_new": 1, "warnings": 1}

    batch_rows = ensure_raw_ingestion_batches(
        run_id=run_id,
        source_id=source_id,
        database_name=database_name,
        collection_name=collection_name,
        total_batches=total_batches,
        estimated_rows=estimated_rows,
        cursor_strategy=plan.ingestion_strategy,
    )
    totals["batches_total"] = len(batch_rows)
    if progress is not None:
        progress["scheduled_batches"] = int(progress.get("scheduled_batches", 0)) + len(batch_rows)
        progress["total_batches"] = max(int(progress.get("total_batches", 0)), int(progress.get("scheduled_batches", 0)))
        progress["batch_size"] = batch_config.batch_size
        progress["parallel_collections"] = batch_config.parallel_collections
        progress["parallel_batches"] = batch_config.parallel_batches

    emit_phase("extracting", f"Queued {len(batch_rows)} batch(es) for {database_name}.{collection_name}")

    cursor_value = previous_cursor_value
    last_raw_path = state.get("last_raw_path")
    last_cursor = previous_cursor_value
    last_snapshot = state.get("last_snapshot_fingerprint")
    raw_object_key = None
    latest_source_document_at = None
    collection_failed = False
    collection_cancelled = False
    no_data_batches = 0
    collection_timing_recorded = False
    snapshot_batch_checksums: list[str] = []

    def record_batch_timing(batch_id: str, batch_timings: dict[str, float], warning_message: str | None = None) -> None:
        record_raw_run_timing(
            run_id=run_id,
            level="batch",
            database_name=database_name,
            collection_name=collection_name,
            batch_id=batch_id,
            source_connect_seconds=batch_timings.get("source_connect_seconds", 0),
            query_seconds=batch_timings.get("query_seconds", 0),
            file_write_seconds=batch_timings.get("file_write_seconds", 0),
            metadata_update_seconds=batch_timings.get("metadata_update_seconds", 0),
            total_duration_seconds=sum(batch_timings.values()),
            warning_message=warning_message,
        )

    def record_collection_timing(warning_message: str | None = None) -> None:
        nonlocal collection_timing_recorded
        if collection_timing_recorded:
            return
        collection_timing_recorded = True
        total_duration = elapsed_since(collection_started_monotonic)
        timing_warning = warning_message
        if not timing_warning and totals["rows_written"] <= 10 and total_duration >= 10:
            startup_metadata = collection_timings.get("source_connect_seconds", 0) + collection_timings.get("metadata_update_seconds", 0)
            query_write = collection_timings.get("query_seconds", 0) + collection_timings.get("file_write_seconds", 0)
            if startup_metadata >= query_write:
                timing_warning = (
                    f"Run processed {totals['rows_written']} rows but took {total_duration:.1f}s. "
                    "Most time was spent in startup/metadata update."
                )
        record_raw_run_timing(
            run_id=run_id,
            level="collection",
            database_name=database_name,
            collection_name=collection_name,
            source_connect_seconds=collection_timings.get("source_connect_seconds", 0),
            query_seconds=collection_timings.get("query_seconds", 0),
            file_write_seconds=collection_timings.get("file_write_seconds", 0),
            metadata_update_seconds=collection_timings.get("metadata_update_seconds", 0),
            total_duration_seconds=total_duration,
            warning_message=timing_warning,
        )

    for batch in batch_rows:
        batch_number = int(batch["batch_number"])
        batch_timings = {
            "source_connect_seconds": 0.0,
            "query_seconds": 0.0,
            "file_write_seconds": 0.0,
            "metadata_update_seconds": 0.0,
        }
        batch_status = str(batch.get("status") or "").lower()
        if batch_status in {"success", "skipped"}:
            cursor_value = batch.get("cursor_end") or cursor_value
            last_cursor = cursor_value
            totals["rows_found"] += int(batch.get("actual_rows") or 0)
            totals["batches_completed"] += 1
            if batch_status == "skipped":
                totals["batches_skipped"] += 1
            continue
        if batch_status == "cancelled":
            collection_cancelled = True
            totals["cancelled"] += 1
            continue
        if batch_status == "running":
            batch_status = "queued"

        if raw_run_cancel_requested(run_id):
            collection_cancelled = True
            finish_raw_ingestion_batch(
                batch_id=str(batch["batch_id"]),
                status="cancelled",
                actual_rows=0,
                processed_rows=0,
                cursor_start=cursor_value,
                cursor_end=cursor_value,
                error_type="UserCancelled",
                error_message="Cancelled before this queued batch started",
                retryable=False,
                recommended_fix="Start a new scoped RAW run when you are ready.",
            )
            totals["cancelled"] += 1
            if progress is not None:
                progress.update(raw_batch_status_counts(run_id))
            continue

        wait_if_queue_paused(run_id, logger)
        if raw_run_cancel_requested(run_id):
            collection_cancelled = True
            finish_raw_ingestion_batch(
                batch_id=str(batch["batch_id"]),
                status="cancelled",
                actual_rows=0,
                processed_rows=0,
                cursor_start=cursor_value,
                cursor_end=cursor_value,
                error_type="UserCancelled",
                error_message="Cancelled while queued",
                retryable=False,
                recommended_fix="Start a new scoped RAW run when you are ready.",
            )
            totals["cancelled"] += 1
            if progress is not None:
                progress.update(raw_batch_status_counts(run_id))
            continue
        started_batch = start_raw_ingestion_batch(str(batch["batch_id"]))
        if not started_batch:
            totals["batches_completed"] += 1
            totals["batches_skipped"] += 1
            if progress is not None:
                progress.update(raw_batch_status_counts(run_id))
            continue
        existing_raw_key = str(started_batch.get("raw_object_key") or batch.get("raw_object_key") or "").strip() or None
        uploaded_key = None
        cursor_start = cursor_value
        emit_phase("extracting", f"Extracting batch {batch_number} for {database_name}.{collection_name}", batch_number)
        try:
            query_started = time.monotonic()
            documents = fetch_documents_for_batch(collection, plan, cursor_value, batch_config.batch_size, batch_number)
            add_elapsed(batch_timings, "query_seconds", query_started)
            collection_timings["query_seconds"] += batch_timings["query_seconds"]
            if not documents:
                no_data_batches += 1
                metadata_started = time.monotonic()
                finished = finish_raw_ingestion_batch(
                    batch_id=str(batch["batch_id"]),
                    status="skipped",
                    actual_rows=0,
                    cursor_start=cursor_start,
                    cursor_end=cursor_value,
                    error_type="NoNewData",
                    error_message="No documents found for this batch",
                    retryable=False,
                    recommended_fix="No action needed unless new records were expected; verify cursor state and source data.",
                )
                add_elapsed(batch_timings, "metadata_update_seconds", metadata_started)
                collection_timings["metadata_update_seconds"] += batch_timings["metadata_update_seconds"]
                record_batch_timing(str(batch["batch_id"]), batch_timings, "No documents found for this batch")
                totals["no_new"] += 1
                totals["batches_skipped"] += 1
                totals["batches_completed"] += 1
                if progress is not None:
                    progress["completed_batches"] = progress.get("completed_batches", 0) + 1
                    progress["skipped_batches"] = progress.get("skipped_batches", 0) + 1
                continue

            fingerprint = batch_fingerprint(documents, plan)
            if plan.ingestion_strategy == SNAPSHOT_FINGERPRINT:
                snapshot_batch_checksums.append(fingerprint["batch_checksum"])
            cursor_value = fingerprint["max_cursor_value"]
            latest_source_document_at = cursor_timestamp(cursor_value) or latest_source_document_at
            duplicate = raw_batch_fingerprint_exists(
                source_id=source_id,
                database_name=database_name,
                collection_name=collection_name,
                cursor_field=plan.effective_cursor_field,
                **fingerprint,
            )
            if duplicate:
                duplicate_raw_key = duplicate.get("raw_object_key")
                last_raw_path = f"s3://{os.environ['MINIO_BUCKET_RAW']}/{duplicate_raw_key}" if duplicate_raw_key else last_raw_path
                metadata_started = time.monotonic()
                finish_raw_ingestion_batch(
                    batch_id=str(batch["batch_id"]),
                    status="skipped",
                    actual_rows=len(documents),
                    cursor_start=cursor_start,
                    cursor_end=cursor_value,
                    raw_object_key=duplicate_raw_key,
                    error_type="DuplicateBatch",
                    error_message="Duplicate batch fingerprint skipped",
                    retryable=False,
                    recommended_fix="No action needed; RAW skipped an already ingested batch.",
                )
                totals["rows_found"] += len(documents)
                totals["duplicate"] += 1
                totals["batches_skipped"] += 1
                totals["batches_completed"] += 1
                if progress is not None:
                    progress["completed_batches"] = progress.get("completed_batches", 0) + 1
                    progress["skipped_batches"] = progress.get("skipped_batches", 0) + 1
                    progress["processed_records"] = progress.get("processed_records", 0) + len(documents)
                upsert_collection_state(
                    source_id=source_id,
                    database_name=database_name,
                    collection_name=collection_name,
                    **state_kwargs(plan, None),
                    last_cursor_value=cursor_value,
                    last_row_count=len(documents),
                    last_raw_path=last_raw_path,
                    latest_source_document_at=latest_source_document_at,
                    estimated_ingestion_lag_seconds=0,
                    records_since_last_run=0,
                    freshness_status="fresh",
                )
                add_elapsed(batch_timings, "metadata_update_seconds", metadata_started)
                collection_timings["metadata_update_seconds"] += batch_timings["metadata_update_seconds"]
                record_batch_timing(str(batch["batch_id"]), batch_timings, "Duplicate batch fingerprint skipped")
                record_raw_run_event(
                    run_id,
                    "metadata_updated",
                    f"Metadata updated for duplicate batch {batch_number}",
                    database_name=database_name,
                    collection_name=collection_name,
                    batch_id=str(batch["batch_id"]),
                )
                last_cursor = cursor_value
                continue

            emit_phase("writing raw file", f"Writing batch {batch_number} with {len(documents)} records", batch_number)
            write_started = time.monotonic()
            bucket_name = os.environ["MINIO_BUCKET_RAW"]
            reusable_size = reusable_raw_object_size(bucket_name, existing_raw_key, len(documents), logger) if existing_raw_key else None
            if reusable_size is not None:
                key, file_size = existing_raw_key, reusable_size
                logger.info("Reusing verified RAW object s3://%s/%s for metadata reconciliation", bucket_name, key)
            else:
                key, file_size = upload_batch(
                    source=source,
                    database_name=database_name,
                    collection_name=collection_name,
                    documents=documents,
                    airflow_run_id=airflow_run_id,
                    batch_number=batch_number,
                    logger=logger,
                )
            uploaded_key = key
            add_elapsed(batch_timings, "file_write_seconds", write_started)
            collection_timings["file_write_seconds"] += batch_timings["file_write_seconds"]
            raw_path = f"s3://{bucket_name}/{key}"
            emit_phase("updating metadata", f"Updating metadata for batch {batch_number}", batch_number)
            metadata_started = time.monotonic()
            insert_raw_file(
                run_id=run_id,
                source_id=source_id,
                database_name=database_name,
                collection_name=collection_name,
                minio_bucket=bucket_name,
                object_key=key,
                row_count=len(documents),
                file_size_bytes=file_size,
            )
            insert_raw_batch_fingerprint(
                source_id=source_id,
                database_name=database_name,
                collection_name=collection_name,
                cursor_field=plan.effective_cursor_field,
                raw_object_key=key,
                **fingerprint,
            )
            finish_raw_ingestion_batch(
                batch_id=str(batch["batch_id"]),
                status="success",
                actual_rows=len(documents),
                cursor_start=cursor_start,
                cursor_end=cursor_value,
                raw_object_key=key,
            )
            upsert_collection_state(
                source_id=source_id,
                database_name=database_name,
                collection_name=collection_name,
                **state_kwargs(plan, None),
                last_cursor_value=cursor_value,
                last_row_count=len(documents),
                last_raw_path=raw_path,
                latest_source_document_at=latest_source_document_at,
                estimated_ingestion_lag_seconds=0,
                records_since_last_run=0,
                freshness_status="fresh",
            )
            add_elapsed(batch_timings, "metadata_update_seconds", metadata_started)
            collection_timings["metadata_update_seconds"] += batch_timings["metadata_update_seconds"]
            record_batch_timing(str(batch["batch_id"]), batch_timings)
            record_raw_run_event(
                run_id,
                "metadata_updated",
                f"Metadata updated for batch {batch_number}",
                database_name=database_name,
                collection_name=collection_name,
                batch_id=str(batch["batch_id"]),
            )
            totals["rows_found"] += len(documents)
            totals["rows_written"] += len(documents)
            totals["files_written"] += 1
            totals["batches_completed"] += 1
            raw_object_key = key
            last_raw_path = raw_path
            last_cursor = cursor_value
            if progress is not None:
                progress["completed_batches"] = progress.get("completed_batches", 0) + 1
                progress["processed_records"] = progress.get("processed_records", 0) + len(documents)
        except Exception as exc:
            collection_failed = True
            totals["failed"] += 1
            totals["warnings"] += 1
            totals["batches_failed"] += 1
            if progress is not None:
                progress["failed_batches"] = progress.get("failed_batches", 0) + 1
            metadata_started = time.monotonic()
            metadata_recovery = bool(uploaded_key)
            finish_raw_ingestion_batch(
                batch_id=str(batch["batch_id"]),
                status="failed",
                cursor_start=cursor_start,
                raw_object_key=uploaded_key,
                error_type="RawMetadataUpdateFailed" if metadata_recovery else type(exc).__name__,
                error_message=str(exc),
                retryable=True,
                recommended_fix=(
                    "Retry failed RAW batches. The uploaded object key was retained so metadata reconciliation can reuse it safely."
                    if metadata_recovery
                    else recommended_fix_for_raw_error(exc)
                ),
            )
            add_elapsed(batch_timings, "metadata_update_seconds", metadata_started)
            collection_timings["metadata_update_seconds"] += batch_timings["metadata_update_seconds"]
            record_batch_timing(str(batch["batch_id"]), batch_timings, str(exc))
            logger.exception("Failed RAW batch %s for %s.%s: %s", batch_number, database_name, collection_name, exc)
            break

        if progress is not None:
            counts = raw_batch_status_counts(run_id)
            progress.update(counts)

    if collection_cancelled and not collection_failed:
        insert_collection_run_status(
            run_id=run_id,
            source_id=source_id,
            database_name=database_name,
            collection_name=collection_name,
            status="cancelled",
            rows_found=totals["rows_found"],
            rows_written=totals["rows_written"],
            previous_cursor_value=previous_cursor_value,
            new_cursor_value=last_cursor,
            raw_object_key=raw_object_key,
            cursor_strategy=plan.ingestion_strategy,
            error_type="UserCancelled",
            message="RAW run was cancelled before all queued batches started",
            recommended_fix="Start a new scoped RAW run to continue. Successful batches remain protected from duplicate writes.",
            started_at=started_at,
            finished_at=utc_now(),
        )
        record_collection_timing("RAW run was cancelled before all queued batches started")
        return totals

    if collection_failed:
        state = latest_collection_state(source_id, database_name, collection_name)
        insert_collection_run_status(
            run_id=run_id,
            source_id=source_id,
            database_name=database_name,
            collection_name=collection_name,
            status="failed",
            rows_found=totals["rows_found"],
            rows_written=totals["rows_written"],
            previous_cursor_value=previous_cursor_value,
            new_cursor_value=state.get("last_cursor_value") or previous_cursor_value,
            raw_object_key=raw_object_key,
            cursor_strategy=plan.ingestion_strategy,
            error_type="RawBatchFailed",
            message="One or more RAW batches failed",
            recommended_fix="Retry failed RAW batches after fixing the collection error.",
            started_at=started_at,
            finished_at=utc_now(),
        )
        record_collection_timing("One or more RAW batches failed")
        return totals

    if plan.ingestion_strategy == SNAPSHOT_FINGERPRINT and snapshot_batch_checksums:
        last_snapshot = hash_strings(snapshot_batch_checksums)
        upsert_collection_state(
            source_id=source_id,
            database_name=database_name,
            collection_name=collection_name,
            **state_kwargs(plan, last_snapshot),
            last_cursor_value=last_cursor,
            last_row_count=max(totals["rows_found"], totals["rows_written"]),
            last_raw_path=last_raw_path,
            latest_source_document_at=latest_source_document_at,
            estimated_ingestion_lag_seconds=0,
            records_since_last_run=0,
            freshness_status="fresh",
        )

    status = "success"
    message = "Batched RAW ingestion completed"
    if totals["files_written"] == 0 and totals["duplicate"] > 0:
        status = "duplicate_batch_skipped"
        message = "Snapshot unchanged; all matching RAW batches were already present" if plan.ingestion_strategy == SNAPSHOT_FINGERPRINT else "All matching RAW batches were duplicates"
    elif totals["files_written"] == 0:
        status = "cursor_fallback" if plan.cursor_warning else "no_new_data"
        message = plan.cursor_warning or "No documents found beyond the current cursor"
    elif previous_cursor_value is None:
        status = "initial_load"

    if totals["rows_found"] == 0 and totals["files_written"] == 0:
        upsert_collection_state(
            source_id=source_id,
            database_name=database_name,
            collection_name=collection_name,
            **state_kwargs(plan, last_snapshot),
            last_cursor_value=previous_cursor_value,
            last_row_count=0,
            last_raw_path=last_raw_path,
            latest_source_document_at=latest_source_document_at,
            estimated_ingestion_lag_seconds=None,
            records_since_last_run=0,
            freshness_status="fresh",
        )

    insert_collection_run_status(
        run_id=run_id,
        source_id=source_id,
        database_name=database_name,
        collection_name=collection_name,
        status="snapshot_no_cursor" if plan.ingestion_strategy == SNAPSHOT_FINGERPRINT and status == "success" else status,
        rows_found=totals["rows_found"],
        rows_written=totals["rows_written"],
        previous_cursor_value=previous_cursor_value,
        new_cursor_value=last_cursor,
        raw_object_key=raw_object_key,
        cursor_strategy=plan.ingestion_strategy,
        message=plan.cursor_warning or message,
        started_at=started_at,
        finished_at=utc_now(),
    )
    if plan.cursor_warning or plan.ingestion_strategy == SNAPSHOT_FINGERPRINT:
        totals["warnings"] += 1
    record_collection_timing(plan.cursor_warning)
    return totals


def process_source(
    source: dict[str, Any],
    run_id: str,
    airflow_run_id: str,
    logger,
    batch_config: RawBatchConfig,
    target_collection_name: str | None = None,
    progress: dict[str, int] | None = None,
    scheduled_only: bool = False,
) -> dict[str, int]:
    mongo_uri = source_mongo_uri(source)
    source_started_monotonic = time.monotonic()
    source_connect_seconds = 0.0
    totals = {
        "rows_found": 0,
        "rows_written": 0,
        "files_written": 0,
        "no_new": 0,
        "duplicate": 0,
        "warnings": 0,
        "failed": 0,
        "batches_total": 0,
        "batches_completed": 0,
        "batches_failed": 0,
        "batches_skipped": 0,
        "cancelled": 0,
    }
    source_id = str(source["id"])
    database_name = source["database_name"]
    planned_collections = planned_collection_inventory(source, target_collection_name)
    database_total_collections = len(planned_collections)
    database_estimated_records = estimated_records_for_inventory(planned_collections)
    database_completed_collections = 0
    database_processed_records = 0
    progress_lock = threading.RLock()

    def publish_progress(
        phase: str,
        *,
        collection_name: str | None = None,
        message: str | None = None,
        database_status: str | None = None,
        database_finished: bool = False,
        error_message: str | None = None,
    ) -> None:
        if not progress:
            return
        current_collection = "" if database_finished and collection_name is None else collection_name
        with progress_lock:
            update_raw_run_progress(
                run_id,
                status="running",
                phase=phase,
                total_databases=progress.get("total_databases", 0),
                completed_databases=progress.get("completed_databases", 0),
                total_collections=progress.get("total_collections", 0),
                completed_collections=progress.get("completed_collections", 0),
                total_estimated_records=progress.get("total_estimated_records", 0),
                processed_records=progress.get("processed_records", 0),
                failed_collections=progress.get("failed_collections", 0),
                total_batches=progress.get("total_batches", 0),
                completed_batches=progress.get("completed_batches", 0),
                failed_batches=progress.get("failed_batches", 0),
                skipped_batches=progress.get("skipped_batches", 0),
                current_batch_number=progress.get("current_batch_number"),
                raw_batch_size=batch_config.batch_size,
                raw_parallel_collections=batch_config.parallel_collections,
                raw_parallel_batches=batch_config.parallel_batches,
                current_database=database_name,
                current_collection=current_collection,
                progress_message=message or f"{phase.replace('_', ' ')} {database_name}{'.' + collection_name if collection_name else ''}",
            )
            upsert_raw_database_progress(
                run_id=run_id,
                source_id=source_id,
                database_name=database_name,
                status=database_status or phase,
                total_collections=database_total_collections,
                completed_collections=database_completed_collections,
                estimated_records=database_estimated_records,
                processed_records=database_processed_records,
                current_collection=current_collection,
                error_message=error_message,
                started=True,
                finished=database_finished,
            )
        progress_step_delay()

    def mark_collection_done(result: dict[str, int], phase: str, collection_name: str, *, failed: bool = False) -> None:
        nonlocal database_completed_collections, database_processed_records
        with progress_lock:
            database_completed_collections += 1
            database_processed_records += max(int(result.get("rows_found") or 0), int(result.get("rows_written") or 0))
            if progress is not None:
                progress["completed_collections"] = progress.get("completed_collections", 0) + 1
                if not int(result.get("batches_total") or 0):
                    progress["processed_records"] = progress.get("processed_records", 0) + max(
                        int(result.get("rows_found") or 0),
                        int(result.get("rows_written") or 0),
                    )
                if failed:
                    progress["failed_collections"] = progress.get("failed_collections", 0) + 1
        publish_progress(
            phase,
            collection_name=collection_name,
            database_status="running",
            message=f"{database_name}.{collection_name} {phase.replace('_', ' ')}",
        )

    try:
        publish_progress("reading source", message=f"Connecting to {database_name}")
        connect_started = time.monotonic()
        with pymongo.MongoClient(
            mongo_uri,
            tz_aware=True,
            connectTimeoutMS=int(os.environ.get("MONGO_CONNECT_TIMEOUT_MS", "3000")),
            serverSelectionTimeoutMS=int(os.environ.get("MONGO_SERVER_SELECTION_TIMEOUT_MS", "3000")),
            socketTimeoutMS=int(os.environ.get("MONGO_SOCKET_TIMEOUT_MS", "5000")),
        ) as client:
            client.admin.command("ping")
            source_connect_seconds = elapsed_since(connect_started)
            database = client[database_name]
            collections = selected_collections(source, database, target_collection_name)
            if not collections:
                logger.warning("Source %s has no active collections selected for RAW", source["source_name"])

            def mark_scheduled_attempt(collection_info: dict[str, Any]) -> None:
                if not scheduled_only:
                    return
                try:
                    mark_scheduled_collection_run(source, collection_info)
                except Exception as exc:
                    logger.warning("Could not update RAW schedule metadata for %s.%s: %s", database_name, collection_info.get("collection_name"), exc)

            def process_collection_info(collection_info: dict[str, Any]) -> tuple[dict[str, int], str, bool, str]:
                collection_name = collection_info["collection_name"]
                if raw_run_cancel_requested(run_id):
                    insert_collection_run_status(
                        run_id=run_id,
                        source_id=source_id,
                        database_name=database_name,
                        collection_name=collection_name,
                        status="cancelled",
                        rows_found=0,
                        rows_written=0,
                        previous_cursor_value=None,
                        new_cursor_value=None,
                        cursor_strategy=collection_info.get("detected_cursor_strategy") or "unknown",
                        error_type="UserCancelled",
                        message="RAW run was cancelled before this collection started",
                        recommended_fix="Start a new scoped RAW run when you are ready.",
                        started_at=utc_now(),
                        finished_at=utc_now(),
                    )
                    return {
                        "rows_found": 0,
                        "rows_written": 0,
                        "files_written": 0,
                        "no_new": 0,
                        "duplicate": 0,
                        "warnings": 0,
                        "failed": 0,
                        "batches_total": 0,
                        "batches_completed": 0,
                        "batches_failed": 0,
                        "batches_skipped": 0,
                        "cancelled": 1,
                    }, "cancelled", False, collection_name
                publish_progress("extracting", collection_name=collection_name, message=f"Preparing {database_name}.{collection_name}")
                if not collection_info.get("exists_in_source"):
                    cursor_strategy = collection_info.get("detected_cursor_strategy") or "unknown"
                    message = f"Selected collection {collection_name} is not present in Mongo database {database_name}"
                    batch_rows = ensure_raw_ingestion_batches(
                        run_id=run_id,
                        source_id=source_id,
                        database_name=database_name,
                        collection_name=collection_name,
                        total_batches=1,
                        estimated_rows=int(collection_info.get("record_count") or 0),
                        cursor_strategy=cursor_strategy,
                    )
                    if batch_rows:
                        batch = batch_rows[0]
                        start_raw_ingestion_batch(str(batch["batch_id"]))
                        finish_raw_ingestion_batch(
                            batch_id=str(batch["batch_id"]),
                            status="failed",
                            actual_rows=0,
                            processed_rows=0,
                            error_type="CollectionNotFound",
                            error_message=message,
                            retryable=True,
                            recommended_fix="Refresh Source Connections inventory, deactivate missing collections, then retry RAW.",
                        )
                    if progress is not None:
                        with progress_lock:
                            progress.update(raw_batch_status_counts(run_id))
                    insert_collection_run_status(
                        run_id=run_id,
                        source_id=source_id,
                        database_name=database_name,
                        collection_name=collection_name,
                        status="failed",
                        rows_found=0,
                        rows_written=0,
                        previous_cursor_value=None,
                        new_cursor_value=None,
                        cursor_strategy=cursor_strategy,
                        error_type="CollectionNotFound",
                        message=message,
                        recommended_fix="Refresh Source Connections inventory, deactivate missing collections, then retry RAW.",
                        started_at=utc_now(),
                        finished_at=utc_now(),
                    )
                    result = {
                        "rows_found": 0,
                        "rows_written": 0,
                        "files_written": 0,
                        "no_new": 0,
                        "duplicate": 0,
                        "warnings": 1,
                        "failed": 1,
                        "batches_total": len(batch_rows),
                        "batches_completed": 0,
                        "batches_failed": len(batch_rows),
                        "batches_skipped": 0,
                        "cancelled": 0,
                    }
                    mark_scheduled_attempt(collection_info)
                    return result, "failed", True, collection_name
                try:
                    def collection_progress_callback(phase, message, batch_number=None, collection_name=collection_name):
                        if progress is not None:
                            with progress_lock:
                                progress["current_batch_number"] = batch_number
                        publish_progress(
                            phase,
                            collection_name=collection_name,
                            message=message,
                            database_status="running",
                        )

                    result = process_collection(
                        source,
                        database,
                        collection_name,
                        run_id,
                        airflow_run_id,
                        logger,
                        batch_config,
                        progress,
                        progress_callback=collection_progress_callback,
                    )
                except Exception as exc:
                    state = latest_collection_state(source_id, database_name, collection_name)
                    insert_collection_run_status(
                        run_id=run_id,
                        source_id=source_id,
                        database_name=database_name,
                        collection_name=collection_name,
                        status="failed",
                        rows_found=0,
                        rows_written=0,
                        previous_cursor_value=state.get("last_cursor_value"),
                        new_cursor_value=state.get("last_cursor_value"),
                        cursor_strategy=state.get("ingestion_strategy") or collection_info.get("detected_cursor_strategy") or "unknown",
                        error_type=type(exc).__name__,
                        message=str(exc),
                        recommended_fix=recommended_fix_for_raw_error(exc),
                        started_at=utc_now(),
                        finished_at=utc_now(),
                    )
                    logger.exception("Skipping failed collection %s.%s: %s", database_name, collection_name, exc)
                    result = {"rows_found": 0, "rows_written": 0, "files_written": 0, "no_new": 0, "duplicate": 0, "warnings": 1, "failed": 1, "cancelled": 0}
                    mark_scheduled_attempt(collection_info)
                    return result, "failed", True, collection_name
                terminal_phase = "skipped/no_new_data" if result.get("no_new") or result.get("duplicate") else "completed"
                mark_scheduled_attempt(collection_info)
                return result, terminal_phase, False, collection_name

            max_workers = max(1, min(int(batch_config.parallel_collections or 1), len(collections) or 1))
            if batch_config.max_batches_per_run is not None:
                max_workers = 1
            collection_results: list[tuple[dict[str, int], str, bool, str]] = []
            if max_workers > 1:
                logger.info("Processing RAW collections for %s with %s workers", database_name, max_workers)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_map = {executor.submit(process_collection_info, collection_info): collection_info for collection_info in collections}
                    for future in as_completed(future_map):
                        collection_results.append(future.result())
            else:
                collection_results = [process_collection_info(collection_info) for collection_info in collections]

            for result, terminal_phase, failed, collection_name in collection_results:
                for key, value in result.items():
                    totals[key] += value
                mark_collection_done(result, terminal_phase, collection_name, failed=failed)
    except Exception as exc:
        if progress is not None:
            progress["completed_databases"] = progress.get("completed_databases", 0) + 1
        publish_progress("failed", message=f"Failed while reading {database_name}: {exc}", database_status="failed", database_finished=True, error_message=str(exc))
        raise

    if progress is not None:
        progress["completed_databases"] = progress.get("completed_databases", 0) + 1
    database_status = "cancelled" if totals["cancelled"] else ("failed" if totals["failed"] else "completed")
    record_raw_run_timing(
        run_id=run_id,
        level="database",
        database_name=database_name,
        source_connect_seconds=source_connect_seconds,
        total_duration_seconds=elapsed_since(source_started_monotonic),
    )
    publish_progress(
        database_status,
        message=f"{database_name} {database_status}",
        database_status=database_status,
        database_finished=True,
    )
    return totals


def main() -> None:
    logger = setup_logging("ingest_mongo_to_raw")
    airflow_run_id = os.environ.get("AIRFLOW_RAW_RUN_ID") or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or f"local__{uuid.uuid4().hex}"
    triggered_by = os.environ.get("ONOV8_TRIGGERED_BY", "airflow")
    target_source_id = (os.environ.get("ONOV8_RAW_SOURCE_ID") or "").strip() or None
    target_collection_name = (os.environ.get("ONOV8_RAW_COLLECTION_NAME") or "").strip() or None
    retry_of_run_id = (os.environ.get("ONOV8_RAW_RETRY_OF_RUN_ID") or "").strip() or None
    retry_reason = (os.environ.get("ONOV8_RAW_RETRY_REASON") or "").strip() or None
    scheduled_only = env_truthy("ONOV8_RAW_SCHEDULED_ONLY")
    run_id = ensure_raw_run(airflow_run_id, triggered_by=triggered_by)
    set_raw_run_scope(run_id, target_source_id, target_collection_name)
    set_raw_run_retry_lineage(run_id, retry_of_run_id, retry_reason)
    batch_config = raw_batch_config()
    totals = {
        "rows_found": 0,
        "rows_written": 0,
        "files_written": 0,
        "no_new": 0,
        "duplicate": 0,
        "warnings": 0,
        "failed": 0,
        "batches_total": 0,
        "batches_completed": 0,
        "batches_failed": 0,
        "batches_skipped": 0,
        "cancelled": 0,
    }

    try:
        sources = fetch_active_sources(target_source_id)
        if not sources:
            if target_source_id:
                logger.warning("RAW target source %s is inactive or not configured; skipping", target_source_id)
                finish_raw_run(run_id, "no_new_data", 0, 0, 0, 0, 0)
                return
            raise RuntimeError("No active source connections configured. Import local Mongo sources and activate one source before running RAW.")
        if scheduled_only:
            sources = scheduled_due_sources(sources, target_collection_name, logger)
            if not sources:
                logger.info("No active RAW collections are due for scheduled ingestion")
                update_raw_run_progress(
                    run_id,
                    status="running",
                    phase="completed",
                    total_databases=0,
                    completed_databases=0,
                    total_collections=0,
                    completed_collections=0,
                    total_estimated_records=0,
                    processed_records=0,
                    total_batches=0,
                    completed_batches=0,
                    progress_message="No scheduled RAW collections were due",
                    details={"scope": "scheduled_due", "scheduled_only": True},
                )
                finish_raw_run(run_id, "no_new_data", 0, 0, 0, 0, 0)
                return

        planned_by_source = {
            str(source["id"]): planned_collection_inventory(source, target_collection_name)
            for source in sources
        }
        progress = {
            "total_databases": len(sources),
            "completed_databases": 0,
            "total_collections": sum(len(collections) for collections in planned_by_source.values()),
            "completed_collections": 0,
            "total_estimated_records": sum(estimated_records_for_inventory(collections) for collections in planned_by_source.values()),
            "processed_records": 0,
            "failed_collections": 0,
            "total_batches": sum(
                sum(max(1, ceil(int(collection.get("record_count") or 0) / batch_config.batch_size)) for collection in collections)
                for collections in planned_by_source.values()
            ),
            "completed_batches": 0,
            "failed_batches": 0,
            "skipped_batches": 0,
            "scheduled_batches": 0,
            "batch_size": batch_config.batch_size,
            "parallel_collections": batch_config.parallel_collections,
            "parallel_batches": batch_config.parallel_batches,
        }
        if batch_config.max_batches_per_run is not None:
            progress["total_batches"] = min(progress["total_batches"], batch_config.max_batches_per_run)
        update_raw_run_progress(
            run_id,
            status="running",
            phase="initializing",
            total_databases=progress["total_databases"],
            completed_databases=0,
            total_collections=progress["total_collections"],
            completed_collections=0,
            total_estimated_records=progress["total_estimated_records"],
            processed_records=0,
            failed_collections=0,
            total_batches=progress["total_batches"],
            completed_batches=0,
            failed_batches=0,
            skipped_batches=0,
            raw_batch_size=batch_config.batch_size,
            raw_parallel_collections=batch_config.parallel_collections,
            raw_parallel_batches=batch_config.parallel_batches,
            current_database=None,
            current_collection=None,
            progress_message="Planning active RAW sources and collections",
            details={
                "scope": "collection" if target_collection_name else ("database" if target_source_id else "all_active"),
                "scheduled_only": scheduled_only,
                "raw_batch_size": batch_config.batch_size,
                "raw_max_batches_per_run": batch_config.max_batches_per_run,
                "raw_parallel_collections": batch_config.parallel_collections,
                "raw_parallel_batches": batch_config.parallel_batches,
            },
        )
        for source in sources:
            source_id = str(source["id"])
            planned = planned_by_source.get(source_id, [])
            upsert_raw_database_progress(
                run_id=run_id,
                source_id=source_id,
                database_name=source["database_name"],
                status="queued",
                total_collections=len(planned),
                completed_collections=0,
                estimated_records=estimated_records_for_inventory(planned),
                processed_records=0,
            )

        with dashboard_schema_lock():
            for source in sources:
                logger.info("Processing raw source %s (%s)", source["source_name"], source["database_name"])
                source_totals = process_source(source, run_id, airflow_run_id, logger, batch_config, target_collection_name, progress, scheduled_only)
                for key, value in source_totals.items():
                    totals[key] += value
                if raw_run_cancel_requested(run_id):
                    logger.info("RAW cancellation requested; stopping after source %s", source["source_name"])
                    break

            if raw_run_cancel_requested(run_id) or totals["cancelled"] > 0:
                run_status = "cancelled"
            elif totals["failed"] > 0:
                run_status = "failed"
            elif totals["files_written"] > 0:
                run_status = "success"
            elif totals["duplicate"] > 0:
                run_status = "duplicate_batch_skipped"
            elif totals["warnings"] > 0:
                run_status = "success"
            else:
                run_status = "no_new_data"

            finish_raw_run(
                run_id,
                run_status,
                totals["rows_found"],
                totals["rows_written"],
                totals["files_written"],
                totals["no_new"],
                totals["duplicate"],
            )
        logger.info(
            "Raw ingestion run %s rows_found=%s rows_written=%s files=%s no_new=%s duplicate=%s warnings=%s failed_collections=%s",
            run_status,
            totals["rows_found"],
            totals["rows_written"],
            totals["files_written"],
            totals["no_new"],
            totals["duplicate"],
            totals["warnings"],
            totals["failed"],
        )
    except Exception as exc:
        logger.error("Raw ingestion run failed: %s\n%s", exc, traceback.format_exc())
        with dashboard_schema_lock():
            finish_raw_run(
                run_id,
                "failed",
                totals["rows_found"],
                totals["rows_written"],
                totals["files_written"],
                totals["no_new"],
                totals["duplicate"],
                str(exc),
            )
        raise


if __name__ == "__main__":
    main()
