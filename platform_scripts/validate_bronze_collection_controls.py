from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    return logging.getLogger(name)


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    import requests

    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def read_text(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def validate_processable_api() -> dict[str, Any]:
    payload = request_json("GET", "/api/bronze/processable", timeout=60)
    assert_ok("summary" in payload and "databases" in payload, "Processable API missing summary/databases")
    databases = payload.get("databases") or []
    for database in databases:
        assert_ok(int(database.get("raw_files") or 0) > 0, f"Database without RAW files appeared: {database}")
        assert_ok(database.get("collections"), f"Database has no RAW-backed collections: {database.get('database_name')}")
        for collection in database.get("collections") or []:
            assert_ok(int(collection.get("raw_files") or 0) > 0, f"Collection without RAW files appeared: {collection}")
    return payload


def validate_dependent_collections(processable: dict[str, Any]) -> None:
    databases = request_json("GET", "/api/bronze/processable/databases", timeout=60)
    api_names = {item.get("database_name") for item in databases}
    processable_names = {item.get("database_name") for item in processable.get("databases") or []}
    assert_ok(processable_names.issubset(api_names), "Processable databases endpoint is missing RAW-backed databases")
    for database_name in sorted(processable_names)[:3]:
        collections = request_json("GET", f"/api/bronze/processable/{database_name}/collections", timeout=60)
        collection_names = {item.get("collection_name") for item in collections}
        expected = {
            item.get("collection_name")
            for database in processable.get("databases") or []
            if database.get("database_name") == database_name
            for item in database.get("collections") or []
        }
        assert_ok(expected.issubset(collection_names), f"Dependent collection endpoint mismatch for {database_name}")


def validate_queue_filters(processable: dict[str, Any]) -> None:
    queue = request_json("GET", "/api/bronze/raw-file-queue?limit=100", timeout=60)
    assert_ok(isinstance(queue.get("files"), list), "Raw file queue endpoint did not return files")
    if not processable.get("databases"):
        return
    database_name = processable["databases"][0]["database_name"]
    filtered = request_json("GET", f"/api/bronze/raw-file-queue?database_name={database_name}&limit=100", timeout=60)
    assert_ok(
        all(item.get("database_name") == database_name for item in filtered.get("files") or []),
        "Database queue filter returned another database",
    )
    collections = processable["databases"][0].get("collections") or []
    if collections:
        collection_name = collections[0]["collection_name"]
        scoped = request_json(
            "GET",
            f"/api/bronze/raw-file-queue?database_name={database_name}&collection_name={collection_name}&limit=100",
            timeout=60,
        )
        assert_ok(
            all(item.get("database_name") == database_name and item.get("collection_name") == collection_name for item in scoped.get("files") or []),
            "Collection queue filter returned another collection",
        )


def validate_static_scope_controls() -> None:
    api_script = read_text("dashboard/api/app/main.py")
    processor = read_text("scripts/bronze_raw_to_delta.py")
    dag = read_text("airflow/dags/bronze_processing_pipeline.py")
    ui = read_text("dashboard/web/src/main.jsx")
    for token in [
        "/api/bronze/processable",
        "/api/bronze/raw-file-queue",
        "/api/bronze/run/database/{database_name}",
        "/api/bronze/run/database/{database_name}/collection/{collection_name}",
        "/api/bronze/run/raw-file/{raw_file_id}",
        "/api/bronze/retry-failed/raw-file/{raw_file_id}",
    ]:
        assert_ok(token in api_script, f"API endpoint missing: {token}")
    for token in ["scope=args.scope", "database_name=args.database_name", "collection_name=args.collection_name", "raw_file_id=args.raw_file_id"]:
        assert_ok(token in processor, f"Processor scoped filtering missing: {token}")
    for token in ["BRONZE_SCOPE", "BRONZE_DATABASE_NAME", "BRONZE_COLLECTION_NAME", "BRONZE_RAW_FILE_ID"]:
        assert_ok(token in dag, f"Bronze DAG conf propagation missing: {token}")
    for label in ["Processable RAW Data", "Raw File Queue", "Run All Pending", "Run Bronze for collection"]:
        assert_ok(label in ui, f"Bronze UI label missing: {label}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true", help="Skip live API checks")
    args = parser.parse_args()
    logger = setup_logging("validate_bronze_collection_controls")
    validate_static_scope_controls()
    if not args.static_only:
        processable = validate_processable_api()
        validate_dependent_collections(processable)
        validate_queue_filters(processable)
    logger.info("Bronze collection/file controls validation passed")


if __name__ == "__main__":
    main()
