from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pymongo
import requests
from psycopg2.extras import Json

from common import s3_client, setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db, new_id
from source_secrets import delete_secret, put_secret


ACTIVE_COLLECTION_ONE = "active_orders"
ACTIVE_COLLECTION_TWO = "active_customers"
INACTIVE_COLLECTION = "inactive_events"
INACTIVE_SOURCE_COLLECTION = "inactive_source_events"
MISSING_COLLECTION = "missing_from_mongo"
LARGE_COLLECTION = "large_orders"
TERMINAL_RAW_STATUSES = {"success", "no_new_data", "duplicate_batch_skipped", "failed"}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request_json(method: str, path: str, *, expect_status: int | None = None, **kwargs: Any) -> Any:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    if expect_status is not None:
        assert_ok(response.status_code == expect_status, f"{path} returned {response.status_code}, expected {expect_status}: {response.text}")
        try:
            return response.json()
        except ValueError:
            return {"text": response.text}
    response.raise_for_status()
    return response.json()


def mongo_uri(host: str) -> str:
    user = os.environ["MONGO_ROOT_USERNAME"]
    password = os.environ["MONGO_ROOT_PASSWORD"]
    return f"mongodb://{user}:{password}@{host}:27017/admin?authSource=admin"


def seed_mongo(host: str, database_name: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    with pymongo.MongoClient(mongo_uri(host), tz_aware=True, serverSelectionTimeoutMS=5000) as client:
        client.drop_database(database_name)
        database = client[database_name]
        database[ACTIVE_COLLECTION_ONE].insert_many(
            [
                {"orderId": "raw-controls-1", "amount": 10, "updatedAt": now},
                {"orderId": "raw-controls-2", "amount": 20, "updatedAt": now + timedelta(seconds=1)},
            ]
        )
        database[ACTIVE_COLLECTION_TWO].insert_one({"customerId": "raw-controls-a", "updatedAt": now})
        database[INACTIVE_COLLECTION].insert_one({"eventId": "raw-controls-skip", "updatedAt": now})


def seed_large_collection(host: str, database_name: str, rows: int = 2505) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    with pymongo.MongoClient(mongo_uri(host), tz_aware=True, serverSelectionTimeoutMS=5000) as client:
        client.drop_database(database_name)
        database = client[database_name]
        database[LARGE_COLLECTION].insert_many(
            [
                {
                    "orderId": f"large-{index}",
                    "amount": index,
                    "updatedAt": now + timedelta(milliseconds=index),
                }
                for index in range(rows)
            ]
        )


def insert_missing_collection_document(host: str, database_name: str) -> None:
    with pymongo.MongoClient(mongo_uri(host), tz_aware=True, serverSelectionTimeoutMS=5000) as client:
        client[database_name][MISSING_COLLECTION].insert_one(
            {
                "id": "now-present",
                "updatedAt": datetime.now(timezone.utc).replace(microsecond=0),
            }
        )


def insert_incremental_document(host: str, database_name: str) -> None:
    with pymongo.MongoClient(mongo_uri(host), tz_aware=True, serverSelectionTimeoutMS=5000) as client:
        client[database_name][ACTIVE_COLLECTION_ONE].insert_one(
            {
                "orderId": f"raw-controls-incremental-{int(time.time())}",
                "amount": 30,
                "updatedAt": datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=10),
            }
        )


def create_source(
    source_name: str,
    database_name: str,
    host: str,
    *,
    active: bool,
    collections: dict[str, bool],
    collection_counts: dict[str, int] | None = None,
) -> str:
    source_id = new_id()
    secret_reference = f"raw-controls-{source_id}"
    put_secret(secret_reference, {"mongo_uri": mongo_uri(host)})
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO source_connections (
                    id, source_name, source_type, database_name, auth_database,
                    connection_config_json, secret_reference, include_collections_json,
                    exclude_collections_json, cursor_field, ingestion_mode, is_active,
                    last_test_status, last_test_message, last_test_at,
                    last_inventory_status, last_inventory_message, last_inventory_at
                )
                VALUES (%s, %s, 'mongo', %s, 'admin', %s, %s, %s, '[]'::jsonb,
                        'updatedAt', 'python', %s, 'ok', 'validation source created',
                        now(), 'ok', 'validation inventory created', now())
                """,
                (
                    source_id,
                    source_name,
                    database_name,
                    Json({"host": host, "port": 27017, "username": os.environ["MONGO_ROOT_USERNAME"]}),
                    secret_reference,
                    Json([name for name, is_active in collections.items() if is_active] or ["__onov8_no_active_collections_selected__"]),
                    active,
                ),
            )
            for collection_name, is_active in collections.items():
                cursor.execute(
                    """
                    INSERT INTO source_connection_collections (
                        id, source_id, collection_name, is_active, record_count,
                        estimated_size_bytes, last_stats_at, detected_cursor_field,
                        detected_cursor_strategy, sample_schema_json
                    )
                    VALUES (%s, %s, %s, %s, %s, 256, now(), 'updatedAt',
                            'incremental_timestamp', %s)
                    """,
                    (
                        new_id(),
                        source_id,
                        collection_name,
                        is_active,
                        int((collection_counts or {}).get(collection_name, 1)),
                        Json({"sampled": True, "fields": ["_id", "updatedAt"], "top_level": ["_id", "updatedAt"], "nested": []}),
                    ),
                )
    return source_id


def wait_for_run(airflow_run_id: str, timeout_seconds: int = 240, observations: dict[str, Any] | None = None) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last: dict[str, Any] = {}
    while time.time() < deadline:
        try:
            progress = request_json("GET", f"/api/raw/runs/{airflow_run_id}/progress", timeout=20)
            if observations is not None:
                observations["progress_endpoint_ok"] = True
                observations["latest_progress"] = progress
                observations["observed_progress_gt_zero"] = bool(
                    observations.get("observed_progress_gt_zero")
                    or (
                        progress.get("status") in {"requested", "queued", "running", "processing"}
                        and float(progress.get("progress_percent") or 0) > 0
                    )
                )
        except Exception as exc:
            if observations is not None:
                observations["progress_endpoint_error"] = str(exc)
        try:
            active_payload = request_json("GET", "/api/raw/runs/active", timeout=20)
            if observations is not None:
                observations["active_endpoint_ok"] = isinstance(active_payload.get("runs"), list)
                observations["active_seen"] = bool(
                    observations.get("active_seen")
                    or any(
                        run.get("airflow_run_id") == airflow_run_id or run.get("run_id") == airflow_run_id
                        for run in active_payload.get("runs", [])
                    )
                )
        except Exception as exc:
            if observations is not None:
                observations["active_endpoint_error"] = str(exc)
        runs = request_json("GET", "/api/pipelines/runs", timeout=20)
        match = next((run for run in runs if run.get("airflow_run_id") == airflow_run_id), None)
        if match:
            last = match
            if match.get("status") in TERMINAL_RAW_STATUSES:
                result = request_json("GET", f"/api/pipelines/runs/{airflow_run_id}", timeout=20)
                if observations is not None:
                    observations["final_progress"] = request_json("GET", f"/api/raw/runs/{airflow_run_id}/progress", timeout=20)
                return result
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for raw run {airflow_run_id}; last={last}")


def run_raw(path: str, label: str, allowed_statuses: set[str] | None = None) -> dict[str, Any]:
    allowed = allowed_statuses or {"success", "no_new_data", "duplicate_batch_skipped"}
    payload = request_json("POST", path, timeout=30)
    run = payload.get("run") or {}
    airflow_run_id = run.get("airflow_run_id")
    assert_ok(airflow_run_id, f"{label} did not return an Airflow run id")
    observations: dict[str, Any] = {}
    queued_progress = request_json("GET", f"/api/raw/runs/{airflow_run_id}/progress", timeout=20)
    observations["queued_progress"] = queued_progress
    active_payload = request_json("GET", "/api/raw/runs/active", timeout=20)
    observations["active_endpoint_ok"] = isinstance(active_payload.get("runs"), list)
    observations["active_seen"] = any(
        item.get("airflow_run_id") == airflow_run_id or item.get("run_id") == str(run.get("id") or "")
        for item in active_payload.get("runs", [])
    )
    result = wait_for_run(
        airflow_run_id,
        int(os.environ.get("VALIDATE_RAW_COLLECTION_CONTROLS_TIMEOUT_SECONDS", "240")),
        observations,
    )
    assert_ok(result.get("status") in allowed, f"{label} failed: {result}")
    result["_validation_progress"] = observations
    return result


def collection_names(run: dict[str, Any]) -> set[str]:
    return {item["collection_name"] for item in run.get("collection_statuses", [])}


def collection_pairs(run: dict[str, Any]) -> set[tuple[str, str]]:
    return {
        (str(item.get("source_id") or ""), item["collection_name"])
        for item in run.get("collection_statuses", [])
    }


def collection_pairs_for_sources(run: dict[str, Any], source_ids: set[str]) -> set[tuple[str, str]]:
    return {
        pair
        for pair in collection_pairs(run)
        if pair[0] in source_ids
    }


def raw_files_for(source_id: str, collection_name: str | None = None) -> list[dict[str, Any]]:
    params = {"source_id": source_id, "limit": 500}
    if collection_name:
        params["collection_name"] = collection_name
    return request_json("GET", "/api/raw/files", params=params, timeout=30)


def raw_files_query(**params: Any) -> list[dict[str, Any]]:
    params.setdefault("limit", 500)
    return request_json("GET", "/api/raw/files", params=params, timeout=30)


def raw_batches(run_id: str, status: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": 2000}
    if status:
        params["status"] = status
    return request_json("GET", f"/api/raw/runs/{run_id}/batches", params=params, timeout=30).get("batches", [])


def assert_success_progress(run: dict[str, Any], label: str) -> dict[str, Any]:
    observations = run.get("_validation_progress") or {}
    assert_ok(observations.get("active_endpoint_ok"), f"{label} did not validate /api/raw/runs/active")
    assert_ok(observations.get("progress_endpoint_ok"), f"{label} did not validate /api/raw/runs/{{run_id}}/progress")
    progress = observations.get("final_progress") or request_json("GET", f"/api/raw/runs/{run['airflow_run_id']}/progress", timeout=20)
    assert_ok(float(progress.get("progress_percent") or 0) == 100, f"{label} completed without reaching 100% progress: {progress}")
    assert_ok(
        int(progress.get("collections_completed") or 0) >= int(progress.get("collections_total") or 0),
        f"{label} completed with incomplete collection progress: {progress}",
    )
    if int(progress.get("total_batches") or 0):
        assert_ok(
            int(progress.get("completed_batches") or 0) >= int(progress.get("total_batches") or 0),
            f"{label} completed with incomplete batch progress: {progress}",
        )
    return progress


def cleanup_fixtures(host: str, source_ids: list[str], database_names: list[str]) -> None:
    raw_objects: list[tuple[str, str]] = []
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT minio_bucket, object_key
                FROM raw_files
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            raw_objects = [(row[0], row[1]) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT DISTINCT run_id::text
                FROM raw_ingestion_batches
                WHERE (source_id = ANY(%s::uuid[]) OR database_name = ANY(%s))
                  AND run_id IS NOT NULL
                UNION
                SELECT DISTINCT run_id::text
                FROM raw_collection_run_statuses
                WHERE (source_id = ANY(%s::uuid[]) OR database_name = ANY(%s))
                  AND run_id IS NOT NULL
                UNION
                SELECT DISTINCT run_id::text
                FROM raw_files
                WHERE (source_id = ANY(%s::uuid[]) OR database_name = ANY(%s))
                  AND run_id IS NOT NULL
                UNION
                SELECT id::text
                FROM raw_ingestion_runs
                WHERE target_source_id = ANY(%s::uuid[])
                """,
                (source_ids, database_names, source_ids, database_names, source_ids, database_names, source_ids),
            )
            run_ids = [row[0] for row in cursor.fetchall()]
            cursor.execute(
                """
                DELETE FROM raw_ingestion_batches
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            cursor.execute(
                """
                DELETE FROM raw_collection_run_statuses
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            cursor.execute(
                """
                DELETE FROM raw_files
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            cursor.execute(
                """
                DELETE FROM raw_run_database_progress
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            if run_ids:
                cursor.execute("DELETE FROM raw_ingestion_runs WHERE id = ANY(%s::uuid[])", (run_ids,))
            cursor.execute(
                """
                DELETE FROM raw_collection_states
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            cursor.execute(
                """
                DELETE FROM raw_schema_snapshots
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            cursor.execute(
                """
                DELETE FROM raw_batch_fingerprints
                WHERE source_id = ANY(%s::uuid[])
                   OR database_name = ANY(%s)
                """,
                (source_ids, database_names),
            )
            cursor.execute("DELETE FROM source_connections WHERE id = ANY(%s::uuid[])", (source_ids,))
    client = s3_client()
    for bucket, object_key in raw_objects:
        try:
            client.delete_object(Bucket=bucket, Key=object_key)
        except Exception:
            pass
    for source_id in source_ids:
        delete_secret(f"raw-controls-{source_id}")
    with pymongo.MongoClient(mongo_uri(host), tz_aware=True, serverSelectionTimeoutMS=5000) as client:
        for database_name in database_names:
            client.drop_database(database_name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-host", default=os.environ.get("VALIDATE_RAW_COLLECTION_CONTROLS_MONGO_HOST", "mongo-users"))
    parser.add_argument("--keep-fixtures", action="store_true", help="Keep temporary validation sources and Mongo databases for debugging")
    args = parser.parse_args()

    logger = setup_logging("validate_raw_collection_controls")
    suffix = new_id().replace("-", "")[:10]
    active_database = f"raw_controls_active_{suffix}"
    second_active_database = f"raw_controls_second_{suffix}"
    inactive_database = f"raw_controls_inactive_{suffix}"
    missing_database = f"raw_controls_missing_{suffix}"
    large_database = f"raw_controls_large_{suffix}"
    active_source_name = f"raw_controls_active_{suffix}"
    second_active_source_name = f"raw_controls_second_{suffix}"
    inactive_source_name = f"raw_controls_inactive_{suffix}"
    missing_source_name = f"raw_controls_missing_{suffix}"
    large_source_name = f"raw_controls_large_{suffix}"
    missing_source_id: str | None = None
    large_source_id: str | None = None

    init_dashboard_db()
    seed_mongo(args.mongo_host, active_database)
    seed_mongo(args.mongo_host, second_active_database)
    seed_mongo(args.mongo_host, inactive_database)
    seed_large_collection(args.mongo_host, large_database)

    active_source_id = create_source(
        active_source_name,
        active_database,
        args.mongo_host,
        active=True,
        collections={ACTIVE_COLLECTION_ONE: True, ACTIVE_COLLECTION_TWO: True, INACTIVE_COLLECTION: False},
    )
    second_active_source_id = create_source(
        second_active_source_name,
        second_active_database,
        args.mongo_host,
        active=True,
        collections={ACTIVE_COLLECTION_ONE: True, ACTIVE_COLLECTION_TWO: False, INACTIVE_COLLECTION: False},
    )
    inactive_source_id = create_source(
        inactive_source_name,
        inactive_database,
        args.mongo_host,
        active=False,
        collections={INACTIVE_SOURCE_COLLECTION: True},
    )
    logger.info("Created validation sources active=%s inactive=%s", active_source_id, inactive_source_id)

    inactive_response = request_json("POST", f"/api/raw/run/source/{inactive_source_id}", expect_status=400)
    assert_ok("inactive" in str(inactive_response).lower(), "Inactive source run did not report inactive source")
    assert_ok(not raw_files_for(inactive_source_id), "Inactive source wrote raw files")
    active_progress_payload = request_json("GET", "/api/raw/runs/active", timeout=30)
    assert_ok(isinstance(active_progress_payload.get("runs"), list), "Active RAW progress endpoint did not return a runs list")

    source_run = run_raw(f"/api/raw/run/source/{active_source_id}?progress_delay_seconds=0.25", "source-level RAW")
    expected_source_pairs = {
        (active_source_id, ACTIVE_COLLECTION_ONE),
        (active_source_id, ACTIVE_COLLECTION_TWO),
    }
    assert_ok(collection_pairs(source_run) == expected_source_pairs, f"Database-level run processed unexpected collections: {collection_pairs(source_run)}")
    assert_ok(raw_files_for(active_source_id, ACTIVE_COLLECTION_ONE), "Active collection one did not write raw files")
    assert_ok(raw_files_for(active_source_id, ACTIVE_COLLECTION_TWO), "Active collection two did not write raw files")
    assert_ok(not raw_files_for(active_source_id, INACTIVE_COLLECTION), "Inactive collection wrote raw files")
    source_progress = assert_success_progress(source_run, "source-level RAW")
    assert_ok(
        source_run.get("_validation_progress", {}).get("observed_progress_gt_zero"),
        f"Running source-level RAW did not expose progress > 0: {source_run.get('_validation_progress')}",
    )
    request_json("GET", "/api/raw/source-status", timeout=30)
    persisted_progress = request_json("GET", f"/api/raw/runs/{source_run['airflow_run_id']}/progress", timeout=20)
    assert_ok(
        persisted_progress.get("progress_percent") == source_progress.get("progress_percent")
        and persisted_progress.get("collections_completed") == source_progress.get("collections_completed"),
        "RAW progress did not persist across a status refresh",
    )

    all_active_run = run_raw(
        "/api/raw/run",
        "all-active RAW",
        allowed_statuses={"success", "no_new_data", "duplicate_batch_skipped", "failed"},
    )
    fixture_source_ids = {active_source_id, second_active_source_id}
    expected_all_active_pairs = {
        (active_source_id, ACTIVE_COLLECTION_ONE),
        (active_source_id, ACTIVE_COLLECTION_TWO),
        (second_active_source_id, ACTIVE_COLLECTION_ONE),
    }
    actual_fixture_pairs = collection_pairs_for_sources(all_active_run, fixture_source_ids)
    assert_ok(actual_fixture_pairs == expected_all_active_pairs, f"All-active run did not process exactly the active fixture collections: {actual_fixture_pairs}")
    fixture_statuses = [
        item for item in all_active_run.get("collection_statuses", [])
        if str(item.get("source_id") or "") in fixture_source_ids
    ]
    assert_ok(not [item for item in fixture_statuses if item.get("status") == "failed"], f"All-active fixture collections failed: {fixture_statuses}")
    if all_active_run.get("status") != "failed":
        assert_success_progress(all_active_run, "all-active RAW")
    assert_ok(raw_files_for(second_active_source_id, ACTIVE_COLLECTION_ONE), "Second active source did not create RAW files before inactive display check")
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE source_connections SET is_active = false WHERE id = %s", (second_active_source_id,))
        connection.commit()
    inactive_historical_files = raw_files_for(second_active_source_id, ACTIVE_COLLECTION_ONE)
    assert_ok(inactive_historical_files, "Raw Files endpoint hid existing RAW files after source was deactivated")
    grouped_payload = request_json("GET", "/api/raw/files/groups", params={"database_name": second_active_database}, timeout=30)
    matching_database_groups = [
        item for item in grouped_payload.get("databases", [])
        if item.get("database_name") == second_active_database
    ]
    assert_ok(matching_database_groups, "Grouped Raw Files view did not include inactive-source historical database")
    assert_ok(
        int(matching_database_groups[0].get("raw_files_count") or 0) >= len(inactive_historical_files),
        f"Grouped Raw Files database count did not include inactive-source files: {matching_database_groups}",
    )
    collection_groups = request_json("GET", "/api/raw/files/collections", params={"database_name": second_active_database}, timeout=30)
    matching_collection_groups = [
        item for item in collection_groups.get("collections", [])
        if item.get("collection_name") == ACTIVE_COLLECTION_ONE
    ]
    assert_ok(matching_collection_groups, "Expanding grouped Raw Files database did not show collection rows")
    nested_group_files = raw_files_query(database_name=second_active_database, collection_name=ACTIVE_COLLECTION_ONE, limit=2)
    assert_ok(nested_group_files, "Expanding grouped Raw Files collection did not show individual raw files")
    options_payload = request_json("GET", "/api/raw/files/options", params={"database_name": second_active_database}, timeout=30)
    assert_ok(second_active_database in options_payload.get("databases", []), "Raw Files database filter omitted inactive-source database with RAW files")
    assert_ok(ACTIVE_COLLECTION_ONE in options_payload.get("collections", []), "Raw Files collection filter did not depend on selected database RAW files")

    insert_incremental_document(args.mongo_host, active_database)
    collection_run = run_raw(
        f"/api/raw/run/source/{active_source_id}/collection/{ACTIVE_COLLECTION_ONE}",
        "collection-level RAW",
    )
    assert_ok(collection_pairs(collection_run) == {(active_source_id, ACTIVE_COLLECTION_ONE)}, f"Collection-level run processed unexpected collections: {collection_pairs(collection_run)}")
    assert_success_progress(collection_run, "collection-level RAW")

    large_source_id = create_source(
        large_source_name,
        large_database,
        args.mongo_host,
        active=True,
        collections={LARGE_COLLECTION: True},
        collection_counts={LARGE_COLLECTION: 2505},
    )
    large_run = run_raw(
        f"/api/raw/run/source/{large_source_id}/collection/{LARGE_COLLECTION}?raw_batch_size=1000&progress_delay_seconds=0.05",
        "large batched RAW",
    )
    large_batches = raw_batches(large_run["airflow_run_id"])
    assert_ok(len(large_batches) >= 3, f"Large collection did not create multiple RAW batches: {large_batches}")
    assert_ok({batch.get("status") for batch in large_batches} <= {"success", "skipped"}, f"Unexpected large batch statuses: {large_batches}")
    large_progress = assert_success_progress(large_run, "large batched RAW")
    assert_ok(int(large_progress.get("total_batches") or 0) >= 3, f"Progress did not use batch totals: {large_progress}")
    large_file_count = len(raw_files_for(large_source_id, LARGE_COLLECTION))
    large_rerun = run_raw(
        f"/api/raw/run/source/{large_source_id}/collection/{LARGE_COLLECTION}?raw_batch_size=1000",
        "large batched RAW rerun",
        allowed_statuses={"no_new_data", "duplicate_batch_skipped", "success"},
    )
    assert_ok(len(raw_files_for(large_source_id, LARGE_COLLECTION)) == large_file_count, "Rerun repeated successful large RAW batches")

    source_status = request_json("GET", "/api/raw/source-status", timeout=30)
    summary = source_status.get("summary") or {}
    assert_ok("inventory_totals" in summary, "RAW source status summary is missing inventory_totals")
    assert_ok("active_selection_totals" in summary, "RAW source status summary is missing active_selection_totals")
    assert_ok("latest_run_totals" in summary, "RAW source status summary is missing latest_run_totals")
    assert_ok(int(summary.get("total_databases") or 0) >= int(summary.get("active_databases") or 0), "Active database card cannot be rendered as selected/total")
    assert_ok(int(summary.get("total_collections") or 0) >= int(summary.get("active_collections") or 0), "Active collection card cannot be rendered as selected/total")
    assert_ok(int(summary.get("total_records") or 0) >= int(summary.get("selected_records") or 0), "Selected records card cannot be rendered as selected/total")
    assert_ok(int(summary.get("total_size_bytes") or 0) >= int(summary.get("selected_size_bytes") or 0), "Selected size card cannot be rendered as selected/total")
    assert_ok("processed_collections_total" in summary, "Processed collections card is missing latest-run denominator")
    matching_sources = [source for source in source_status.get("sources", []) if source.get("id") == active_source_id]
    assert_ok(matching_sources, "Active validation source is missing from /api/raw/source-status")
    all_source_status = request_json("GET", "/api/raw/source-status", params={"database_status": "all"}, timeout=30)
    inactive_source_status = request_json("GET", "/api/raw/source-status", params={"database_status": "inactive"}, timeout=30)
    assert_ok(
        any(source.get("id") == inactive_source_id for source in all_source_status.get("sources", [])),
        "RAW source status all filter did not include inactive databases",
    )
    assert_ok(
        any(source.get("id") == inactive_source_id for source in inactive_source_status.get("sources", [])),
        "RAW source status inactive filter did not include inactive databases",
    )
    assert_ok(
        not any(source.get("id") == inactive_source_id for source in source_status.get("sources", [])),
        "Default RAW source status leaked inactive databases",
    )
    active_source_status = matching_sources[0]
    matching_collections = active_source_status.get("collections") or []
    status_by_collection = {item["collection_name"]: item for item in matching_collections}
    assert_ok(status_by_collection.get(ACTIVE_COLLECTION_ONE, {}).get("last_raw_status") in {"success", "initial_load"}, "Active collection status is not visible")
    assert_ok(status_by_collection.get(INACTIVE_COLLECTION, {}).get("is_active") is False, "Inactive collection status row is not visible")
    assert_ok(active_source_status.get("total_records") is not None, "Database row is missing total records")
    assert_ok(active_source_status.get("total_size_bytes") is not None, "Database row is missing total size")
    assert_ok(active_source_status.get("latest_run_scope"), "Database row is missing latest run scope")
    active_source_files = raw_files_for(active_source_id)
    active_source_raw_rows = sum(int(item.get("row_count") or 0) for item in active_source_files)
    assert_ok(int(active_source_status.get("total_raw_files") or 0) >= len(active_source_files), "Database row total_raw_files did not count matching RAW files")
    assert_ok(int(active_source_status.get("total_raw_rows") or 0) >= active_source_raw_rows, "Database row total_raw_rows did not count matching RAW rows")
    assert_ok(int(active_source_status.get("latest_rows_ingested") or active_source_status.get("rows_ingested") or 0) > 0, "Database row ROWS stayed zero even though RAW rows exist")
    assert_ok(int(active_source_status.get("latest_files_written") or active_source_status.get("files_written") or 0) > 0, "Database row FILES stayed zero even though RAW files exist")
    assert_ok("latest_rows_ingested" in active_source_status, "Database API response is missing latest_rows_ingested")
    assert_ok("latest_files_written" in active_source_status, "Database API response is missing latest_files_written")
    assert_ok("total_raw_rows" in active_source_status, "Database API response is missing total_raw_rows")
    assert_ok("total_raw_files" in active_source_status, "Database API response is missing total_raw_files")
    active_one_status = status_by_collection.get(ACTIVE_COLLECTION_ONE, {})
    active_one_files = raw_files_for(active_source_id, ACTIVE_COLLECTION_ONE)
    active_one_raw_rows = sum(int(item.get("row_count") or 0) for item in active_one_files)
    assert_ok(int(active_one_status.get("total_raw_files") or 0) >= len(active_one_files), "Collection total_raw_files did not count matching RAW files")
    assert_ok(int(active_one_status.get("total_raw_rows") or 0) >= active_one_raw_rows, "Collection total_raw_rows did not count matching RAW rows")
    assert_ok(int(active_one_status.get("latest_rows_ingested") or active_one_status.get("rows_ingested") or 0) > 0, "Collection ROWS INGESTED stayed zero even though RAW rows exist")
    assert_ok(int(active_one_status.get("latest_files_written") or active_one_status.get("files_written") or 0) > 0, "Collection latest_files_written stayed zero even though RAW files exist")
    assert_ok(active_one_status.get("latest_raw_object_key") or active_one_status.get("last_raw_path"), "Collection latest RAW path/object key is missing")
    assert_ok(active_one_status.get("latest_status") == active_one_status.get("latest_run", {}).get("status"), "Collection latest_status does not match latest run state")
    assert_ok(
        active_one_status.get("latest_status") != "no_new_data" or active_one_status.get("latest_run", {}).get("status") == "no_new_data",
        "Collection status showed No New Data without a latest No New Data run state",
    )

    frontend_source = Path("dashboard/web/src/main.jsx").read_text(encoding="utf-8")
    assert_ok(
        'const [rawControlsFilters, setRawControlsFilters] = useState({ databaseStatus: "active", collectionStatus: "active" });' in frontend_source
        and 'const [showExcludedCollections, setShowExcludedCollections] = useState(false);' in frontend_source,
        "RAW controls filters do not default to active databases, active collections, and hidden excluded collections",
    )
    assert_ok(
        'setSourceStatus(await api("/api/raw/source-status?database_status=all"));' in frontend_source,
        "RAW controls are not loading cached all-database metadata for local filtering",
    )
    assert_ok(
        'const visibleStatusSources = statusSources.filter((source) =>' in frontend_source
        and 'return sourceIsRunnable(source);' in frontend_source
        and 'visibleStatusSources.map((source)' in frontend_source,
        "RAW controls table is not rendering the filtered runnable database list",
    )
    assert_ok(
        'const visibleCollectionsForSource = (source) =>' in frontend_source
        and 'return collections.filter((collection) => collection.is_active);' in frontend_source
        and 'visibleCollections.map((collection)' in frontend_source,
        "RAW controls expanded rows are not hiding excluded collections by default",
    )
    assert_ok(
        'data-validation-id="raw-controls-show-excluded-toggle"' in frontend_source
        and 'toggleShowExcludedCollections(event.target.checked)' in frontend_source,
        "RAW controls missing Show excluded collections toggle",
    )
    assert_ok(
        ') : showDisabledCollectionRun ? (' in frontend_source
        and '<TextButton icon={Play} disabled>Run RAW for this collection</TextButton>' in frontend_source,
        "Excluded RAW collections do not reveal a disabled run button when explicitly shown",
    )
    assert_ok(
        'disabled={!canRun || Boolean(busyKey) || !sourceRunnable}' in frontend_source,
        "Database RAW run button is not disabled when a database has no runnable active collections",
    )
    assert_ok(
        "Nothing selected for RAW ingestion. Go to Source Connections to activate collections." in frontend_source,
        "RAW controls empty state does not guide users to activate collections",
    )
    assert_ok(
        'const [rawFilesView, setRawFilesView] = useState("grouped");' in frontend_source
        and 'data-validation-id="raw-files-grouped-view"' in frontend_source
        and 'Grouped View' in frontend_source
        and 'Flat Files View' in frontend_source,
        "Raw Files tab does not default to grouped view with a flat view toggle",
    )
    assert_ok(
        'api(`/api/raw/files/groups?${params.toString()}`)' in frontend_source
        and 'api(`/api/raw/files/collections?${params.toString()}`)' in frontend_source
        and 'api(`/api/raw/files?${params.toString()}`)' in frontend_source,
        "Raw Files grouped view is not using lazy database, collection, and file metadata calls",
    )
    assert_ok(
        'rawFilesDatabaseOptions.map((database)' in frontend_source
        and 'rawFilesCollectionOptions.map((collection)' in frontend_source
        and 'rawFilesRunOptions.map((run)' in frontend_source,
        "Raw Files filters are not bound to RAW-file-backed dependent options",
    )
    assert_ok(
        "No RAW files found. Run RAW ingestion from Source Collection Controls." in frontend_source,
        "Raw Files empty state does not direct users to RAW ingestion controls",
    )
    assert_ok("function formatRatioBytes(value) {\n  return formatCompactBytes(value);\n}" in frontend_source, "Small collection sizes can still render as misleading 0 MB")
    assert_ok("formatCompactBytes(collection.estimated_size_bytes || 0)" in frontend_source, "Collection size cell is not using compact byte formatting")
    assert_ok("aria-label={path}" in frontend_source and "IconButton icon={Copy}" in frontend_source, "RAW path cell is missing tooltip/copy affordances")

    collection_status = request_json("GET", f"/api/raw/source/{active_source_id}/collections-status", timeout=30)
    assert_ok(collection_status.get("collections"), "Collections-status endpoint returned no rows")

    filtered_files = raw_files_query(database_name=active_database, collection_name=ACTIVE_COLLECTION_ONE)
    assert_ok(filtered_files, "Raw Files filter returned no rows for active collection")
    assert_ok(
        all(item.get("database_name") == active_database and item.get("collection_name") == ACTIVE_COLLECTION_ONE for item in filtered_files),
        f"Raw Files filter leaked other collections: {filtered_files}",
    )
    paged_files = raw_files_query(database_name=active_database, collection_name=ACTIVE_COLLECTION_ONE, limit=1, offset=0)
    assert_ok(len(paged_files) <= 1, f"Raw Files pagination ignored limit=1: {paged_files}")
    assert_ok(
        not paged_files or (
            paged_files[0].get("database_name") == active_database
            and paged_files[0].get("collection_name") == ACTIVE_COLLECTION_ONE
        ),
        f"Raw Files pagination did not preserve filters: {paged_files}",
    )
    preview = request_json(
        "GET",
        "/api/raw/files/preview",
        params={"database_name": active_database, "collection_name": ACTIVE_COLLECTION_ONE, "limit": 5},
        timeout=30,
    )
    assert_ok(preview.get("records"), "Raw Preview returned no records for database/collection filter")
    assert_ok(
        preview.get("file", {}).get("database_name") == active_database
        and preview.get("file", {}).get("collection_name") == ACTIVE_COLLECTION_ONE,
        f"Raw Preview did not honor database/collection filters: {preview.get('file')}",
    )

    missing_source_id = create_source(
        missing_source_name,
        missing_database,
        args.mongo_host,
        active=True,
        collections={MISSING_COLLECTION: True},
    )
    failed_run = run_raw(
        f"/api/raw/run/source/{missing_source_id}?progress_delay_seconds=0.15",
        "failed RAW progress",
        allowed_statuses={"failed"},
    )
    failed_progress = failed_run.get("_validation_progress", {}).get("final_progress") or request_json(
        "GET",
        f"/api/raw/runs/{failed_run['airflow_run_id']}/progress",
        timeout=20,
    )
    assert_ok(failed_progress.get("status") == "failed", f"Failed RAW progress endpoint did not report failed: {failed_progress}")
    assert_ok(int(failed_progress.get("failed_collections") or 0) >= 1, f"Failed RAW progress did not count failed collections: {failed_progress}")
    assert_ok(0 < float(failed_progress.get("progress_percent") or 0) < 100, f"Failed RAW progress was not partial: {failed_progress}")
    failed_batches = raw_batches(failed_run["airflow_run_id"], status="failed")
    assert_ok(failed_batches, f"Failed RAW run did not track a failed batch: {failed_run}")
    insert_missing_collection_document(args.mongo_host, missing_database)
    retry_payload = request_json(
        "POST",
        "/api/raw/batches/retry-failed",
        json={"run_id": failed_run["airflow_run_id"]},
        timeout=30,
    )
    retry_run_id = retry_payload.get("run", {}).get("airflow_run_id")
    assert_ok(retry_run_id, f"Retry failed RAW batches did not queue a new run: {retry_payload}")
    retry_run = wait_for_run(retry_run_id, int(os.environ.get("VALIDATE_RAW_COLLECTION_CONTROLS_TIMEOUT_SECONDS", "240")))
    assert_ok(retry_run.get("status") in {"success", "no_new_data", "duplicate_batch_skipped"}, f"Retry failed RAW batches did not recover: {retry_run}")

    if not args.keep_fixtures:
        cleanup_fixtures(
            args.mongo_host,
            [source_id for source_id in [active_source_id, second_active_source_id, inactive_source_id, missing_source_id, large_source_id] if source_id],
            [active_database, second_active_database, inactive_database, missing_database, large_database],
        )

    logger.info(
        "RAW collection controls validation passed active_source=%s second_active_source=%s inactive_source=%s source_run=%s all_active_run=%s collection_run=%s",
        active_source_id,
        second_active_source_id,
        inactive_source_id,
        source_run.get("airflow_run_id"),
        all_active_run.get("airflow_run_id"),
        collection_run.get("airflow_run_id"),
    )
    print("RAW collection controls validation passed")


if __name__ == "__main__":
    main()
