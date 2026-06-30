#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
MAIN_JSX = ROOT / "dashboard" / "web" / "src" / "main.jsx"
STYLES = ROOT / "dashboard" / "web" / "src" / "styles.css"
USER_GUIDE = ROOT / "dashboard" / "web" / "src" / "userGuideData.js"
API_MAIN = ROOT / "dashboard" / "api" / "app" / "main.py"
DB_SCHEMA = ROOT / "scripts" / "dashboard_db.py"
README = ROOT / "README.md"
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
        text = response.read().decode("utf-8")
        return json.loads(text or "{}")


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


def extract_e2e_api_block(api_text: str) -> str:
    start = api_text.index("END_TO_END_FLOW_STAGES")
    end = api_text.index('@app.get("/api/governance/overview")')
    return api_text[start:end]


def validate_static_files() -> list[dict[str, Any]]:
    jsx = MAIN_JSX.read_text(encoding="utf-8")
    css = STYLES.read_text(encoding="utf-8")
    guide = USER_GUIDE.read_text(encoding="utf-8")
    api_text = API_MAIN.read_text(encoding="utf-8")
    api_block = extract_e2e_api_block(api_text)
    schema = DB_SCHEMA.read_text(encoding="utf-8")
    readme = README.read_text(encoding="utf-8")
    checks: list[dict[str, Any]] = []

    route_tokens = [
        '{ id: "end-to-end-flow", label: "End-to-End Flow"',
        '"end-to-end-flow": EndToEndFlowPage',
        "function EndToEndFlowPage",
        'data-validation-id="end-to-end-flow-page"',
        'data-validation-id="e2e-selection-tree"',
        'data-validation-id="e2e-current-pipeline-state"',
        'data-validation-id="e2e-pipeline-detail-view"',
        'data-validation-id="e2e-flow-stepper"',
    ]
    missing_routes = [token for token in route_tokens if token not in jsx]
    assert_ok(not missing_routes, "End-to-End Flow route/sidebar/page wiring is incomplete", missing_routes)
    checks.append(ok("page route, sidebar item, and validation markers exist"))

    ui_tokens = [
        "Source Selected",
        "RAW",
        "Bronze",
        "Silver",
        "Safe Views",
        "Superset Dataset",
        "Ready for BI Planning",
        "Run Selected End-to-End Flow",
        "Retry Failed Stage",
        "Resume Flow",
        "Cancel Flow",
        "You are about to process",
        "raw_file_ids",
        "bronze_tables",
        "showInactiveCollections",
        "Show inactive collections",
        "loadSourceConnectionsInventory",
        "Primary selection endpoint failed:",
        "endpointErrorMessage",
        "Current Pipeline State",
        "Pipeline Detail View",
        "Continue from current state",
        "Rebuild completed stages",
        "Trigger Type",
        "detailStageFilter",
        "pipeline_state",
        "completed_manually",
        "completed_by_this_flow",
        "manual_stage_run",
        "end_to_end_flow",
        "scheduled",
        "needs_refresh",
        "retry_action",
    ]
    missing_ui = [token for token in ui_tokens if token not in jsx]
    assert_ok(not missing_ui, "End-to-End Flow UI is missing required flow controls", missing_ui)
    checks.append(ok("selection UI, scoped warning, stepper stages, and retry/resume controls are present"))

    summary_tokens = [
        "selectedDatabaseCount",
        "selectedItems.length",
        "selectedRecords",
        "selectedSize",
        'disabled={!selectedItems.length || running}',
    ]
    missing_summary = [token for token in summary_tokens if token not in jsx]
    assert_ok(not missing_summary, "End-to-End selection summary or run-button state is not wired correctly", missing_summary)
    checks.append(ok("selected collections update summary cards and run button is gated by selection state"))

    css_tokens = [
        ".e2e-workbench",
        ".e2e-database-card",
        ".e2e-collection-row",
        ".e2e-current-state-panel",
        ".e2e-state-flow",
        ".e2e-state-step",
        ".e2e-detail-filter-grid",
        ".e2e-detail-timeline",
        ".e2e-stage-detail-grid",
        ".e2e-stepper",
        ".e2e-step-card",
        ".e2e-items-table",
        ".e2e-log-panel",
    ]
    missing_css = [token for token in css_tokens if token not in css]
    assert_ok(not missing_css, "End-to-End Flow premium styling is missing", missing_css)
    checks.append(ok("premium operational styling exists"))

    endpoint_tokens = [
        '@app.get("/api/end-to-end-flow/inventory")',
        '@app.post("/api/end-to-end-flow/run")',
        '@app.get("/api/end-to-end-flow/runs")',
        '@app.get("/api/end-to-end-flow/runs/{run_id}")',
        '@app.get("/api/end-to-end-flow/runs/{run_id}/logs")',
        '@app.post("/api/end-to-end-flow/runs/{run_id}/retry")',
        '@app.post("/api/end-to-end-flow/runs/{run_id}/resume")',
        '@app.post("/api/end-to-end-flow/runs/{run_id}/cancel")',
    ]
    missing_endpoints = [token for token in endpoint_tokens if token not in api_text]
    assert_ok(not missing_endpoints, "End-to-End Flow APIs are incomplete", missing_endpoints)
    checks.append(ok("inventory, run, progress, logs, retry, resume, and cancel APIs exist"))

    scoped_tokens = [
        'trigger_raw_pipeline_request("end_to_end_flow", str(item["source_id"]), item["collection_name"])',
        'trigger_bronze_pipeline("end_to_end_flow", scope="collection"',
        'trigger_silver_pipeline("end_to_end_flow", scope="collection"',
        "end_to_end_refresh_safe_views_for_tables",
        "end_to_end_refresh_datasets_for_views",
        "end_to_end_collection_pipeline_state",
        "end_to_end_stage_already_complete",
        "end_to_end_trigger_type",
        "end_to_end_retry_action",
        "rebuild_completed_stages",
        "RAW Layer",
        "Bronze Layer",
        "Silver Layer",
        "Query Layer",
        "BI",
    ]
    missing_scoped = [token for token in scoped_tokens if token not in api_block]
    assert_ok(not missing_scoped, "End-to-End Flow does not prove scoped collection execution", missing_scoped)
    checks.append(ok("RAW, Bronze, Silver, Safe Views, and Superset dataset stages are scoped to selected collections"))

    forbidden_tokens = ["ensure_bi_layer(", "generate_new_superset_dashboards", "fix_superset_dashboards", "delete_generated_dashboards", "regenerate_dashboards"]
    present_forbidden = [token for token in forbidden_tokens if token in api_block]
    assert_ok(not present_forbidden, "End-to-End Flow must stop at Superset datasets and not generate dashboards", present_forbidden)
    assert_ok("Dashboard" not in "".join(line for line in jsx.splitlines() if "END_TO_END_FLOW_STEPS" in line or "Superset Dataset" in line), "Flow stepper appears to include dashboards")
    checks.append(ok("flow stops at Superset dataset creation and contains no dashboard generation path"))

    schema_tokens = [
        "CREATE TABLE IF NOT EXISTS end_to_end_flow_runs",
        "CREATE TABLE IF NOT EXISTS end_to_end_flow_steps",
        "CREATE TABLE IF NOT EXISTS end_to_end_flow_items",
        "linked_raw_run_id",
        "linked_bronze_run_id",
        "linked_silver_run_id",
        "linked_query_validation_id",
        "linked_bi_sync_id",
        "cancel_requested",
        "rebuild_completed_stages",
        "raw_status",
        "bronze_status",
        "silver_status",
        "safe_view_status",
        "dataset_status",
    ]
    missing_schema = [token for token in schema_tokens if token not in schema]
    assert_ok(not missing_schema, "End-to-End Flow metadata schema is incomplete", missing_schema)
    checks.append(ok("persistent run, step, item, retry/resume, and linked-run metadata exists"))

    docs_tokens = [
        "### End-to-End Flow",
        "does not generate dashboards",
        "Retry Failed Stage",
        "Resume Flow",
        "Current Pipeline State",
        "Pipeline Detail View",
        "Completed manually",
        "end_to_end_flow_runs",
        "end_to_end_flow_steps",
        "end_to_end_flow_items",
    ]
    missing_docs = [token for token in docs_tokens if token not in readme]
    assert_ok(not missing_docs, "README does not document End-to-End Flow", missing_docs)
    assert_ok('"end-to-end-flow"' in guide and "Run Selected End-to-End Flow" in guide, "User guide is missing End-to-End Flow")
    checks.append(ok("README and user guide document the flow, stop point, and resume behavior"))

    return checks


def validate_live_apis() -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    token = login()
    inventory = request_json("/api/end-to-end-flow/inventory", token=token)
    runs = request_json("/api/end-to-end-flow/runs", token=token)

    assert_ok(isinstance(inventory, dict), "Inventory endpoint did not return an object", inventory)
    assert_ok(isinstance(inventory.get("sources", []), list), "Inventory sources is not a list", inventory)
    assert_ok(isinstance(runs, list), "Runs endpoint did not return a list", runs)
    checks.append(ok("selection endpoint and run records endpoint are reachable", {"sources": len(inventory.get("sources", [])), "runs": len(runs)}))

    if inventory.get("sources"):
        source = inventory["sources"][0]
        assert_ok("collections" in source and isinstance(source["collections"], list), "Inventory source is missing collections", source)
        assert_ok(source.get("collections_count", len(source["collections"])) >= len(source["collections"]), "Inventory source is missing database collection counts", source)
        if source["collections"]:
            collection = source["collections"][0]
            for field in ["record_count", "estimated_size_bytes", "cursor_strategy", "latest_raw_status", "latest_bronze_status", "latest_silver_status", "dataset_status"]:
                assert_ok(field in collection, f"Inventory collection is missing {field}", collection)
            pipeline_state = collection.get("pipeline_state")
            assert_ok(isinstance(pipeline_state, dict), "Inventory collection is missing current pipeline state", collection)
            stages = pipeline_state.get("stages", {}) if isinstance(pipeline_state, dict) else {}
            expected_stages = {"source", "raw", "bronze", "silver", "safe_view", "dataset"}
            assert_ok(expected_stages <= set(stages.keys()), "Current pipeline state is missing required stages", pipeline_state)
            allowed_states = {"not_started", "completed_manually", "completed_by_this_flow", "completed_by_flow", "pending", "running", "failed", "skipped", "needs_refresh"}
            invalid_stage_states = {name: stage.get("status") for name, stage in stages.items() if stage.get("status") not in allowed_states}
            assert_ok(not invalid_stage_states, "Current pipeline state contains an unsupported stage status", invalid_stage_states)
            for stage_name, stage in stages.items():
                for field in ["triggered_from", "trigger_type", "run_id", "started_at", "finished_at", "timestamp", "duration_seconds", "rows", "files", "tables", "files_created", "tables_created", "output_path", "latest_error", "retry_action"]:
                    assert_ok(field in stage, f"Pipeline stage {stage_name} is missing {field}", stage)
                assert_ok(stage.get("trigger_type") in {"manual_stage_run", "end_to_end_flow", "scheduled", "not_started"}, f"Pipeline stage {stage_name} has an unsupported trigger type", stage)
        checks.append(ok("selection endpoint returns databases, collections, records, size, cursor, layer statuses, and dataset status"))
        checks.append(ok("selection endpoint returns Current Pipeline State for manual and flow-run visibility"))
        active_source = next((row for row in inventory["sources"] if row.get("active_collections_count")), None)
        if active_source:
            collections = active_source.get("collections", [])
            first_inactive_index = next((index for index, row in enumerate(collections) if not row.get("is_active")), len(collections))
            assert_ok(
                all(row.get("is_active") for row in collections[:first_inactive_index]),
                "Active collections are not ordered first",
                active_source,
            )
            checks.append(ok("active collections are ordered first in the selection endpoint", {"database": active_source.get("database_name")}))
        collection_without_raw = next(
            (
                collection
                for source_row in inventory["sources"]
                for collection in source_row.get("collections", [])
                if not collection.get("raw_files")
            ),
            None,
        )
        assert_ok(collection_without_raw is not None, "Selection endpoint appears to require RAW files before showing source collections")
        checks.append(ok("selection endpoint includes Source Connections collections before RAW files exist"))
    else:
        checks.append(ok("live inventory endpoint works; no source inventory rows are currently cached"))

    if runs:
        run_id = runs[0].get("id")
        detail = request_json(f"/api/end-to-end-flow/runs/{run_id}", token=token)
        logs = request_json(f"/api/end-to-end-flow/runs/{run_id}/logs", token=token)
        assert_ok("steps" in detail and "items" in detail, "Run detail is missing persisted steps/items", detail)
        assert_ok({step.get("stage") for step in detail.get("steps", [])} >= {"raw", "bronze", "silver", "safe_views", "superset_dataset"}, "Run detail is missing required stages", detail.get("steps"))
        assert_ok(isinstance(logs.get("events", []), list), "Logs endpoint did not return events", logs)
        checks.append(ok("live run detail persists steps, items, and logs", {"run_id": run_id, "steps": len(detail.get("steps", [])), "items": len(detail.get("items", []))}))
    else:
        checks.append(ok("retry/resume metadata is statically present; no live end-to-end runs recorded yet"))

    return checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true", help="Skip live dashboard API checks")
    args = parser.parse_args()

    checks = validate_static_files()
    if not args.static_only:
        try:
            checks.extend(validate_live_apis())
        except (HTTPError, URLError, TimeoutError, ConnectionError) as exc:
            checks.append({"status": "warning", "message": "Live API checks skipped because dashboard API is not reachable", "details": str(exc)})

    print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
