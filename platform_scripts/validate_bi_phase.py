from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

from bi_layer import dynamic_safe_datasets, ensure_bi_layer, validate_bi_layer
from common import PROJECT_ROOT, setup_logging


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


def running_inside_container() -> bool:
    return Path("/.dockerenv").exists()


def run_make_validation(logger) -> None:
    logger.info("Validating Makefile target: make validate-bi")
    result = subprocess.run(
        ["make", "validate-bi"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=int(os.environ.get("VALIDATE_BI_MAKE_TIMEOUT_SECONDS", "900")),
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
    assert_ok(result.returncode == 0, "make validate-bi did not pass")
    logger.info("make validate-bi passed")


def endpoint_validations() -> None:
    expected_datasets = set(dynamic_safe_datasets())

    _, health = request_json("GET", "/api/bi/superset-health", timeout=60)
    assert_ok(health.get("http", {}).get("status") == "ok", f"Superset is not reachable: {health}")

    _, overview = request_json("GET", "/api/bi/overview", timeout=60)
    assert_ok(overview.get("superset_database_present") is True, f"Superset database is missing: {overview}")
    assert_ok(overview.get("datasets_count", 0) >= len(expected_datasets), f"BI overview has missing datasets: {overview}")
    assert_ok(overview.get("eligible_safe_views_count", 0) >= len(expected_datasets), f"BI eligible safe views mismatch: {overview}")
    assert_ok((overview.get("unsafe_visible_views_count") or 0) == 0, f"Unsafe views are visible to Superset: {overview}")
    assert_ok(overview.get("failed_dashboard_generations", 0) == 0, f"BI overview has failed dashboard generations: {overview}")
    assert_ok(overview.get("failed_chart_generations", 0) == 0, f"BI overview has failed chart generations: {overview}")

    _, datasets = request_json("GET", "/api/bi/datasets", timeout=60)
    dataset_names = {dataset["dataset_name"] for dataset in datasets if dataset.get("status") == "ok"}
    assert_ok(expected_datasets <= dataset_names, f"Missing BI datasets: {sorted(expected_datasets - dataset_names)}")
    for dataset in datasets:
        if dataset["dataset_name"] in expected_datasets:
            assert_ok(dataset.get("row_count", 0) > 0, f"{dataset['dataset_name']} has no rows")
            assert_ok(dataset.get("columns_count", 0) > 0, f"{dataset['dataset_name']} has no columns")

    _, dashboards = request_json("GET", "/api/bi/dashboards", timeout=60)
    for dashboard in dashboards:
        assert_ok(dashboard.get("status") in {"ok", "slow", "unknown"}, f"{dashboard['dashboard_name']} has unexpected status")
        linked_datasets = set(dashboard.get("linked_datasets") or [])
        if linked_datasets:
            assert_ok(bool(linked_datasets & expected_datasets), f"{dashboard['dashboard_name']} is not linked to a current eligible dataset")

    _, charts = request_json("GET", "/api/bi/charts", timeout=60)
    for chart in charts:
        assert_ok(chart.get("status") in {"ok", "unknown"}, f"{chart['chart_name']} has unexpected status")
        if chart.get("dataset_name"):
            assert_ok(chart["dataset_name"] in expected_datasets, f"{chart['chart_name']} points to non-eligible dataset {chart['dataset_name']}")

    _, validation = request_json("GET", "/api/bi/validation", timeout=60)
    latest = validation.get("latest_run") or {}
    assert_ok(latest.get("status") == "ok", f"Latest BI validation is not ok: {validation}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-make-check",
        action="store_true",
        help="Skip the host-side make validate-bi recursion check. Used by the Makefile target itself.",
    )
    args = parser.parse_args()
    logger = setup_logging("validate_bi_phase")
    logger.info("Validating Phase 5 Superset BI layer")

    if not args.skip_make_check and not running_inside_container():
        run_make_validation(logger)
        return

    provision = ensure_bi_layer("metadata")
    assert_ok(provision.get("status") == "ok", f"BI provisioning failed: {provision}")

    validation = validate_bi_layer()
    assert_ok(validation.get("status") == "ok", f"BI validation failed: {validation}")

    endpoint_validations()
    logger.info("Phase 5 BI validation passed")


if __name__ == "__main__":
    main()
