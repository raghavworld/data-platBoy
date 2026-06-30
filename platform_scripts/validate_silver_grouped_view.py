#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
MAIN_JSX = ROOT / "dashboard" / "web" / "src" / "main.jsx"
STYLES = ROOT / "dashboard" / "web" / "src" / "styles.css"
API_SCRIPT = ROOT / "dashboard" / "api" / "app" / "main.py"
DAG_SCRIPT = ROOT / "airflow" / "dags" / "silver_processing_pipeline.py"
PROCESSOR = ROOT / "scripts" / "silver_bronze_to_delta.py"
API_BASE = os.environ.get("DASHBOARD_API_BASE_URL") or f"http://localhost:{os.environ.get('DASHBOARD_API_PORT', '8001')}"
USERNAME = os.environ.get("ONOV8_ADMIN_USERNAME", "admin")
PASSWORD = os.environ.get("ONOV8_ADMIN_PASSWORD", "admin")


def ok(message: str, details: Any | None = None) -> dict[str, Any]:
    return {"status": "ok", "message": message, "details": details}


def assert_ok(condition: bool, message: str, details: Any | None = None) -> None:
    if not condition:
        raise RuntimeError(f"{message}: {details}" if details is not None else message)


def request_json(path: str, *, method: str = "GET", payload: dict[str, Any] | None = None, token: str | None = None, timeout: int = 20) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(f"{API_BASE.rstrip('/')}{path}", data=body, headers=headers, method=method)
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8") or "{}")


def login() -> str | None:
    try:
        payload = request_json("/api/auth/login", method="POST", payload={"username": USERNAME, "password": PASSWORD}, timeout=20)
    except HTTPError as exc:
        if exc.code in {404, 405}:
            return None
        raise
    token = payload.get("token")
    assert_ok(token or payload.get("auth_required") is False, "Login did not return a token", payload)
    return token


def validate_static_ui() -> list[dict[str, Any]]:
    jsx = MAIN_JSX.read_text(encoding="utf-8")
    css = STYLES.read_text(encoding="utf-8")
    api_script = API_SCRIPT.read_text(encoding="utf-8")
    dag = DAG_SCRIPT.read_text(encoding="utf-8")
    processor = PROCESSOR.read_text(encoding="utf-8")
    checks: list[dict[str, Any]] = []

    required_jsx = [
        'data-validation-id="silver-grouped-view"',
        'data-validation-id="silver-filter-bar"',
        'data-validation-id="silver-database-filter"',
        'data-validation-id="silver-collection-filter"',
        'data-validation-id="silver-table-filter"',
        'data-validation-id="silver-history-browser"',
        'data-validation-id="silver-progress-panel"',
        'data-validation-id="silver-child-tables"',
        "SilverGroupedBrowser",
        "SilverFlatTable",
        "SilverHistoryBrowser",
        "SilverProgressPanel",
        "SilverConceptGrid",
        '"Processable Bronze Data"',
        '"Silver Tables"',
        '"Child Tables"',
        '"Transform Plans"',
        '"Field Profiles"',
        '"Failed Tables"',
        '"Progress"',
        '"Diagnostics"',
        "Grouped View",
        "Flat View",
        'data-validation-id="silver-processable-grouped-actions"',
        "Run Silver for all pending Bronze tables",
        "Run Silver for this database",
        "Run Silver for this collection",
        "Retry Silver",
        "Up to date",
        "Target Silver",
        "Skipped Reason",
        "Bronze Path",
        "silver-processable-detail",
        "silver-processable-reason",
        "Scope type:",
        "Expected target Silver table(s):",
        "/api/silver/run/table/",
    ]
    missing_jsx = [token for token in required_jsx if token not in jsx]
    assert_ok(not missing_jsx, "Silver grouped UX is missing required UI markers", missing_jsx)
    assert_ok(
        'data-validation-id="silver-processable-flat-table-actions"' not in jsx,
        "Silver processable UI still renders a duplicate flat table",
    )
    checks.append(ok("grouped view, history, progress, child tables, and required tabs are present"))

    dependency_tokens = [
        'database: event.target.value, collection: "", table: ""',
        'collection: event.target.value, table: ""',
        'database: event.target.value, collection: "", table_name: "", child_table_name: "", offset: 0',
        'collection: event.target.value, table_name: "", child_table_name: "", offset: 0',
        "previewChildTableOptions",
        "tablesForSelectedRun",
    ]
    missing_dependency = [token for token in dependency_tokens if token not in jsx]
    assert_ok(not missing_dependency, "Silver dependent filters are not wired correctly", missing_dependency)
    checks.append(ok("database -> collection -> table and preview child-table dependencies clear stale selections"))

    grouping_tokens = [
        "collections_count",
        "parent_table_count",
        "child_table_count",
        "readiness_score",
        "raw_json_dependency",
        "governance_status",
        "bi_suitability",
        "silverRunDetails",
        "batch_fingerprints",
        "source_database",
        "source_collection",
    ]
    missing_grouping = [token for token in grouping_tokens if token not in jsx]
    assert_ok(not missing_grouping, "Silver grouped rows are missing operational fields", missing_grouping)
    checks.append(ok("database, collection, table, and run history grouping fields are represented"))

    scope_tokens = [
        "/api/silver/run",
        "/api/silver/run/database/{database_name}",
        "/api/silver/run/database/{database_name}/collection/{collection_name}",
        "/api/silver/run/table/{table_name}",
        'scope="database"',
        'scope="collection"',
        'scope="table"',
        "SILVER_TABLE_NAME",
        "selected_target_names",
        "target.table_name in only_targets",
    ]
    missing_scope = [
        token for token in scope_tokens
        if token not in api_script and token not in dag and token not in processor
    ]
    assert_ok(not missing_scope, "Silver exact-scope API/processor wiring is incomplete", missing_scope)
    checks.append(ok("all-pending, database, collection, and table Silver scopes are wired end to end"))

    required_css = [
        ".silver-grouped-table",
        ".silver-filter-grid",
        ".silver-preview-controls",
        ".silver-concept-grid",
        ".silver-history-tree",
        ".silver-progress-panel",
        ".silver-database-row",
        ".silver-collection-row",
        ".silver-table-row.child",
    ]
    missing_css = [token for token in required_css if token not in css]
    assert_ok(not missing_css, "Silver grouped UX is missing stylesheet support", missing_css)
    checks.append(ok("premium grouped Silver styling is present"))

    return checks


def validate_live_metadata() -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    token = login()
    tables = request_json("/api/silver/tables?limit=500", token=token)
    smart = request_json("/api/silver/smart-flattening?limit=200", token=token)
    runs = request_json("/api/pipelines/silver/runs?limit=20", token=token)

    assert_ok(isinstance(tables, list), "Silver tables endpoint did not return a list")
    assert_ok(isinstance(smart, dict), "Smart Silver endpoint did not return an object")
    assert_ok(isinstance(runs, list), "Silver runs endpoint did not return a list")
    checks.append(ok("Silver metadata APIs are reachable", {"tables": len(tables), "runs": len(runs)}))

    databases = sorted({row.get("source_database") for row in tables if row.get("source_database")})
    collections_by_database: dict[str, set[str]] = {}
    tables_by_collection: dict[str, set[str]] = {}
    for row in tables:
        database = row.get("source_database")
        collection = row.get("source_collection")
        table_name = row.get("silver_table_name")
        if database and collection:
            collections_by_database.setdefault(database, set()).add(collection)
        if database and collection and table_name:
            tables_by_collection.setdefault(f"{database}.{collection}", set()).add(table_name)

    if tables:
        assert_ok(databases, "Silver table metadata has no database values")
        assert_ok(tables_by_collection, "Silver table metadata has no collection/table hierarchy")
    checks.append(ok("Silver database and collection metadata supports dependent filters", {
        "databases_with_silver_tables": len(databases),
        "collection_scopes": len(tables_by_collection),
    }))

    child_tables = smart.get("child_tables") or []
    transform_plans = smart.get("transform_plans") or []
    field_profiles = smart.get("field_profiles") or []
    assert_ok(isinstance(child_tables, list), "Smart Silver child tables payload is not a list")
    assert_ok(isinstance(transform_plans, list), "Smart Silver transform plans payload is not a list")
    assert_ok(isinstance(field_profiles, list), "Smart Silver field profiles payload is not a list")
    checks.append(ok("Smart Silver metadata exposes child tables, transform plans, and field profiles", {
        "child_tables": len(child_tables),
        "transform_plans": len(transform_plans),
        "field_profiles": len(field_profiles),
    }))

    if runs:
        run_id = runs[0].get("id") or runs[0].get("airflow_run_id")
        if run_id:
            detail = request_json(f"/api/pipelines/silver/runs/{run_id}", token=token)
            assert_ok("batch_fingerprints" in detail, "Silver run detail is missing batch fingerprints")
            checks.append(ok("Silver grouped history can load run fingerprints", {
                "latest_run": runs[0].get("airflow_run_id"),
                "fingerprints": len(detail.get("batch_fingerprints") or []),
            }))
    else:
        checks.append(ok("Silver grouped history static UI is present; no live runs recorded yet"))

    return checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true", help="Skip live dashboard API checks")
    args = parser.parse_args()

    checks = validate_static_ui()
    if not args.static_only:
        checks.extend(validate_live_metadata())

    print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
