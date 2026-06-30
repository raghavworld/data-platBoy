from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import requests

from common import setup_logging
from governance_metadata import (
    governance_catalog,
    governance_lineage,
    governance_overview,
    governance_pii,
    openmetadata_health,
    run_metadata_sync,
    validate_governance_state,
    validate_main_pipeline_removed_state,
)
from query_layer import safe_view_name_for_table, silver_tables_detail


EXPECTED_DAGS = {
    "raw_ingestion_pipeline",
    "bronze_processing_pipeline",
    "silver_processing_pipeline",
    "bi_validation_pipeline",
    "governance_metadata_pipeline",
}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def airflow_base() -> str:
    return os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080")


def dashboard_web_urls() -> list[str]:
    configured = os.environ.get("DASHBOARD_WEB_URL")
    if configured:
        return [configured.rstrip("/")]
    if Path("/.dockerenv").exists():
        return ["http://dashboard-web:5173", f"http://localhost:{os.environ.get('DASHBOARD_WEB_PORT', '5173')}"]
    return [f"http://localhost:{os.environ.get('DASHBOARD_WEB_PORT', '5173')}"]


def request_json(method: str, path_or_url: str, *, absolute: bool = False, expect_ok: bool = True, **kwargs: Any) -> tuple[int, Any]:
    url = path_or_url if absolute else f"{api_base()}{path_or_url}"
    response = requests.request(method, url, timeout=kwargs.pop("timeout", 120), **kwargs)
    if expect_ok:
        response.raise_for_status()
    try:
        payload = response.json()
    except Exception:
        payload = response.text
    return response.status_code, payload


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def airflow_dag_ids() -> set[str]:
    _, payload = request_json(
        "GET",
        f"{airflow_base()}/api/v1/dags",
        absolute=True,
        auth=(os.environ.get("AIRFLOW_USER", "admin"), os.environ.get("AIRFLOW_PASSWORD", "admin")),
        params={"limit": 1000},
        timeout=30,
    )
    return {dag["dag_id"] for dag in payload.get("dags", [])}


def validate_governance_endpoints() -> dict[str, Any]:
    endpoints = [
        "/api/governance/overview",
        "/api/governance/catalog",
        "/api/governance/lineage",
        "/api/governance/pii",
        "/api/governance/ownership",
        "/api/governance/sync-runs",
        "/api/governance/openmetadata-health",
    ]
    payloads = {}
    for endpoint in endpoints:
        _, payloads[endpoint] = request_json("GET", endpoint, timeout=90)
    _, validation = request_json("POST", "/api/governance/validate", timeout=180)
    payloads["/api/governance/validate"] = validation
    return payloads


def validate_dashboard_ui_loads() -> str:
    errors = []
    for url in dashboard_web_urls():
        try:
            response = requests.get(url, timeout=20)
            if response.ok and "ONOV8" in response.text:
                return url
            errors.append(f"{url}: HTTP {response.status_code}")
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("Governance dashboard UI did not load; " + "; ".join(errors))


def validate_previous_phase_signals() -> dict[str, Any]:
    _, raw = request_json("GET", "/api/raw/overview", timeout=90)
    _, bronze = request_json("GET", "/api/bronze/overview", timeout=90)
    _, silver = request_json("GET", "/api/silver/overview", timeout=90)
    _, query = request_json("GET", "/api/query/overview", timeout=120)
    _, bi = request_json("GET", "/api/bi/overview", timeout=120)
    assert_ok(raw.get("active_sources", 0) >= 1, "RAW phase signal failed: expected at least one active source")
    assert_ok(bronze.get("bronze_tables", 0) >= 1, "Bronze phase signal failed: expected at least one Bronze table")
    assert_ok(silver.get("silver_tables", 0) >= 1, "Silver phase signal failed: expected at least one Silver table")
    assert_ok((query.get("trino_status") or {}).get("status") == "ok", "Query phase signal failed: Trino is not ok")
    assert_ok(bi.get("datasets_count", 0) >= 1, "BI phase signal failed: expected at least one Superset dataset")
    return {"raw": raw, "bronze": bronze, "silver": silver, "query": query, "bi": bi}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-sync", action="store_true", help="Validate existing governance state without running a metadata sync first")
    args = parser.parse_args()

    logger = setup_logging("validate_governance_phase")
    logger.info("Validating Phase 6 governance and metadata layer")

    main_pipeline = validate_main_pipeline_removed_state()
    assert_ok(main_pipeline["status"] == "ok", f"main_pipeline removal validation failed: {main_pipeline}")

    if not args.skip_sync:
        sync_result = run_metadata_sync()
        assert_ok(sync_result["status"] == "ok", f"Governance sync failed: {sync_result}")

    health = openmetadata_health()
    assert_ok(health["status"] in {"ok", "fallback_active"}, f"OpenMetadata is neither reachable nor in fallback mode: {health}")

    catalog = governance_catalog(limit=1000)
    asset_names = {asset["asset_name"] for asset in catalog}
    silver_names = {name.removeprefix("silver.") for name in asset_names if name.startswith("silver.")}
    view_names = {name.removeprefix("view.") for name in asset_names if name.startswith("view.")}
    expected_silver_tables = {table["table_name"] for table in silver_tables_detail() if table.get("status") == "ok"}
    expected_safe_views = {safe_view_name_for_table(table_name) for table_name in expected_silver_tables}
    assert_ok(expected_silver_tables <= silver_names, f"Missing cataloged Silver tables: {sorted(expected_silver_tables - silver_names)}")
    assert_ok(expected_safe_views <= view_names, f"Missing cataloged safe views: {sorted(expected_safe_views - view_names)}")

    pii = governance_pii(limit=1000)
    assert_ok(pii["tags"], "PII tags are missing")
    assert_ok(not pii["violations"], f"Safe views expose raw PII: {pii['violations']}")

    lineage = governance_lineage()
    assert_ok(lineage["edges"], "Governance lineage is missing")

    endpoint_payloads = validate_governance_endpoints()
    assert_ok(endpoint_payloads["/api/governance/overview"].get("cataloged_views", 0) >= 1, "Governance overview endpoint has missing views")
    assert_ok(endpoint_payloads["/api/governance/validate"].get("status") == "ok", "Governance validation endpoint failed")

    loaded_url = validate_dashboard_ui_loads()
    logger.info("Governance dashboard UI loaded from %s", loaded_url)

    dag_ids = airflow_dag_ids()
    assert_ok("main_pipeline" not in dag_ids, "Airflow still lists main_pipeline")
    assert_ok(EXPECTED_DAGS <= dag_ids, f"Missing expected Airflow DAGs: {sorted(EXPECTED_DAGS - dag_ids)}")

    overview = governance_overview()
    assert_ok(overview["lineage_coverage_percent"] > 0, "Lineage coverage is zero")
    previous = validate_previous_phase_signals()
    validation = validate_governance_state()
    assert_ok(validation["status"] == "ok", f"Governance state validation failed: {validation}")

    logger.info("Phase 6 governance validation passed: overview=%s previous_phase_signals=%s", overview, previous.keys())


if __name__ == "__main__":
    main()
