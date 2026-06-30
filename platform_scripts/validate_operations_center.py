from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

from common import PROJECT_ROOT, setup_logging


EXPECTED_OPERATION_ENDPOINTS = {
    "/api/operations/health",
    "/api/operations/orchestration",
    "/api/operations/logs",
    "/api/operations/failed-jobs",
    "/api/operations/history",
    "/api/operations/events",
    "/api/operations/replay-jobs",
    "/api/operations/restart-history",
    "/api/operations/validation-runs",
    "/api/operations/layers/{layer}",
    "/api/operations/raw/soft-reset",
    "/api/operations/raw/flush",
    "/api/operations/raw/rebuild",
    "/api/operations/raw/replay",
    "/api/operations/raw/rerun-failed",
    "/api/operations/raw/clear-logs",
    "/api/operations/bronze/soft-reset",
    "/api/operations/bronze/flush",
    "/api/operations/bronze/rebuild",
    "/api/operations/bronze/replay",
    "/api/operations/bronze/reprocess-failed",
    "/api/operations/bronze/clear-logs",
    "/api/operations/silver/soft-reset",
    "/api/operations/silver/flush",
    "/api/operations/silver/rebuild",
    "/api/operations/silver/replay",
    "/api/operations/silver/rerun-failed",
    "/api/operations/silver/clear-logs",
    "/api/operations/query/refresh-metadata",
    "/api/operations/query/restart",
    "/api/operations/query/clear-query-history",
    "/api/operations/query/clear-validation-history",
    "/api/operations/query/validate",
    "/api/operations/query/refresh-catalogs",
    "/api/operations/bi/refresh-metadata",
    "/api/operations/bi/rebuild-datasets",
    "/api/operations/bi/rebuild-dashboards",
    "/api/operations/bi/restart",
    "/api/operations/bi/clear-validation-history",
    "/api/operations/bi/validate-dashboards",
    "/api/operations/governance/sync",
    "/api/operations/governance/refresh-lineage",
    "/api/operations/governance/reapply-pii-tags",
    "/api/operations/governance/restart",
    "/api/operations/governance/clear-history",
    "/api/operations/governance/validate",
    "/api/operations/services/{service_name}/restart",
    "/api/operations/global/validate",
    "/api/operations/global/restart-all",
    "/api/operations/global/replay-downstream",
    "/api/operations/global/clear-all-logs",
    "/api/operations/global/rebuild",
    "/api/operations/global/cleanup",
    "/api/operations/global/health-check",
    "/api/platform/full-wipe-keep-sources/estimate",
    "/api/platform/full-wipe-keep-sources",
}


PREVIOUS_PHASE_COMMANDS = [
    ["python", "/opt/platform/scripts/validate_raw_phase.py"],
    ["python", "/opt/platform/scripts/validate_bronze_phase.py"],
    ["python", "/opt/platform/scripts/validate_silver_phase.py"],
    ["python", "/opt/platform/scripts/validate_query_phase.py", "--skip-make-check"],
    ["python", "/opt/platform/scripts/validate_bi_phase.py", "--skip-make-check"],
    ["python", "/opt/platform/scripts/validate_governance_phase.py", "--skip-sync"],
]


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request_json(method: str, path: str, expect_ok: bool = True, **kwargs: Any) -> tuple[int, Any]:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 180), **kwargs)
    if expect_ok:
        response.raise_for_status()
    try:
        payload = response.json()
    except Exception:
        payload = response.text
    return response.status_code, payload


def validate_endpoint_inventory() -> None:
    _, openapi = request_json("GET", "/openapi.json", timeout=30)
    paths = set(openapi.get("paths", {}))
    missing = sorted(EXPECTED_OPERATION_ENDPOINTS - paths)
    assert_ok(not missing, f"Missing operation endpoints: {missing}")


def validate_layer_summaries() -> dict[str, Any]:
    summaries = {}
    for layer in ["raw", "bronze", "silver", "query", "bi", "governance"]:
        _, payload = request_json("GET", f"/api/operations/layers/{layer}", timeout=180)
        assert_ok(payload.get("layer") == layer, f"{layer} operation summary did not load")
        summaries[layer] = payload
    return summaries


def validate_safe_operation_flows() -> dict[str, Any]:
    _, restart = request_json("POST", "/api/operations/services/trino/restart", json={"dry_run": True}, timeout=90)
    assert_ok(restart.get("status") in {"dry_run", "ok"}, f"Restart dry-run failed: {restart}")

    _, rebuild = request_json(
        "POST",
        "/api/operations/global/rebuild",
        json={"scope": "bronze_silver", "confirmation": "REBUILD BRONZE SILVER", "dry_run": True},
        timeout=90,
    )
    dependency_order = ((rebuild.get("result") or {}).get("dependency_order") or rebuild.get("dependency_order") or [])
    assert_ok(dependency_order[:2] == ["bronze", "silver"], f"Dependency rebuild order is wrong: {rebuild}")

    replay_results = {}
    for layer in ["raw", "bronze", "silver"]:
        _, replay = request_json("POST", f"/api/operations/{layer}/replay", json={"dry_run": True}, timeout=90)
        assert_ok(replay.get("status") == "dry_run", f"{layer} replay dry-run failed: {replay}")
        replay_results[layer] = replay

    _, downstream = request_json("POST", "/api/operations/global/replay-downstream", json={"dry_run": True}, timeout=90)
    assert_ok(downstream.get("status") in {"queued", "dry_run"}, f"Downstream replay dry-run failed: {downstream}")

    raw_flush_status, _ = request_json(
        "POST",
        "/api/operations/raw/flush",
        expect_ok=False,
        json={"confirmation": "WRONG"},
        timeout=30,
    )
    assert_ok(raw_flush_status >= 400, "RAW flush did not enforce typed confirmation")

    clear_logs_status, _ = request_json(
        "POST",
        "/api/operations/global/clear-all-logs",
        expect_ok=False,
        json={"confirmation": "WRONG"},
        timeout=30,
    )
    assert_ok(clear_logs_status >= 400, "Clear all logs did not enforce typed confirmation")

    full_wipe_status, _ = request_json(
        "POST",
        "/api/platform/full-wipe-keep-sources",
        expect_ok=False,
        json={"confirmation": "WRONG"},
        timeout=30,
    )
    assert_ok(full_wipe_status >= 400, "Full wipe keep-sources did not enforce typed confirmation")
    return {"restart": restart, "rebuild": rebuild, "replay": replay_results, "downstream": downstream}


def validate_platform_observability() -> dict[str, Any]:
    _, health = request_json("GET", "/api/operations/health", timeout=180)
    assert_ok(health.get("status") in {"ok", "degraded"}, f"Platform health aggregation failed: {health}")

    _, orchestration = request_json("GET", "/api/operations/orchestration", timeout=180)
    node_ids = {node.get("id") for node in orchestration.get("nodes", [])}
    expected_nodes = {"mongo", "raw", "bronze", "silver", "query", "bi", "governance"}
    assert_ok(expected_nodes <= node_ids, f"Missing orchestration nodes: {sorted(expected_nodes - node_ids)}")

    _, logs = request_json("GET", "/api/operations/logs", timeout=90)
    for key in ["operations", "events", "replay_jobs", "restart_history", "global_validation_runs"]:
        assert_ok(isinstance(logs.get(key), list), f"Operations logs missing list: {key}")

    _, global_validation = request_json("POST", "/api/operations/global/validate", timeout=240)
    assert_ok(global_validation.get("status") in {"ok", "failed"}, f"Global validation did not return a status: {global_validation}")
    assert_ok(global_validation.get("checks"), "Global validation returned no checks")
    return {"health": health, "orchestration": orchestration, "logs": logs, "global_validation": global_validation}


def validate_dashboard_ui_source() -> None:
    source = PROJECT_ROOT / "dashboard" / "web" / "src" / "main.jsx"
    if source.exists():
        content = source.read_text(encoding="utf-8")
    else:
        dashboard_web = os.environ.get("DASHBOARD_WEB_URL", "http://dashboard-web:5173").rstrip("/")
        response = requests.get(f"{dashboard_web}/src/main.jsx", timeout=30)
        response.raise_for_status()
        content = response.text
    for text in ["Operations Center", "Pipeline Orchestration", "Logs & Replay", "Platform Health", "Full Wipe — Keep Source Connections"]:
        assert_ok(text in content, f"Dashboard UI is missing {text}")


def validate_previous_phase_signals() -> dict[str, Any]:
    _, raw = request_json("GET", "/api/raw/overview", timeout=120)
    _, bronze = request_json("GET", "/api/bronze/overview", timeout=120)
    _, silver = request_json("GET", "/api/silver/overview", timeout=120)
    _, query = request_json("GET", "/api/query/overview", timeout=120)
    _, bi = request_json("GET", "/api/bi/overview", timeout=120)
    _, governance = request_json("GET", "/api/governance/overview", timeout=120)
    _, sources = request_json("GET", "/api/sources", timeout=120)
    assert_ok(isinstance(sources, list), "Source Connections page data did not load")
    assert_ok("bronze_tables" in bronze, "Bronze overview did not load")
    assert_ok("silver_tables" in silver, "Silver overview did not load")
    assert_ok("trino_status" in query, "Query overview did not load")
    assert_ok("superset_status" in bi, "BI overview did not load")
    assert_ok("openmetadata_status" in governance, "Governance overview did not load")
    return {"raw": raw, "sources": {"count": len(sources)}, "bronze": bronze, "silver": silver, "query": query, "bi": bi, "governance": governance}


def run_previous_phase_scripts(logger) -> None:
    if not Path("/.dockerenv").exists():
        logger.info("Skipping container-only previous phase scripts outside Docker; run through make platform-validate")
        return
    for command in PREVIOUS_PHASE_COMMANDS:
        logger.info("Running previous phase validation: %s", " ".join(command))
        result = subprocess.run(command, capture_output=True, text=True, timeout=1200)
        if result.returncode != 0:
            logger.info("Retrying previous phase validation once: %s", " ".join(command))
            retry = subprocess.run(command, capture_output=True, text=True, timeout=1200)
            if retry.returncode == 0:
                continue
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr)
            sys.stdout.write(retry.stdout)
            sys.stderr.write(retry.stderr)
            result = retry
        assert_ok(result.returncode == 0, f"Previous phase validation failed: {' '.join(command)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-previous-phase-scripts", action="store_true")
    args = parser.parse_args()

    logger = setup_logging("validate_operations_center")
    logger.info("Validating Phase 7 Operations Center")

    validate_endpoint_inventory()
    summaries = validate_layer_summaries()
    safe_flows = validate_safe_operation_flows()
    observability = validate_platform_observability()
    validate_dashboard_ui_source()
    previous = validate_previous_phase_signals()
    if not args.skip_previous_phase_scripts:
        run_previous_phase_scripts(logger)

    logger.info(
        "Phase 7 Operations Center validation passed: layers=%s safe_flows=%s observability=%s previous=%s",
        sorted(summaries),
        sorted(safe_flows),
        sorted(observability),
        sorted(previous),
    )


if __name__ == "__main__":
    main()
