from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

from common import PROJECT_ROOT, setup_logging
from query_layer import is_pii_column, safe_view_name_for_table


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request_json(method: str, path: str, expect_ok: bool = True, **kwargs: Any) -> tuple[int, Any]:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 90), **kwargs)
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
    logger.info("Validating Makefile target: make validate-query")
    result = subprocess.run(
        ["make", "validate-query"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=int(os.environ.get("VALIDATE_QUERY_MAKE_TIMEOUT_SECONDS", "600")),
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
    assert_ok(result.returncode == 0, "make validate-query did not pass")
    logger.info("make validate-query passed")


def dangerous_sql_is_blocked(safe_view_name: str, silver_table_name: str) -> None:
    dangerous_statements = [
        f'DROP TABLE delta.silver."{silver_table_name}"',
        f'DELETE FROM "{safe_view_name}"',
        f'UPDATE "{safe_view_name}" SET country = \'US\'',
        f'INSERT INTO "{safe_view_name}" SELECT * FROM "{safe_view_name}"',
        f'ALTER VIEW "{safe_view_name}" RENAME TO unsafe_query_view',
        f'CREATE TABLE test AS SELECT * FROM "{safe_view_name}"',
        f'SELECT * FROM "{silver_table_name}" LIMIT 1',
    ]
    for sql in dangerous_statements:
        status, payload = request_json(
            "POST",
            "/api/query/run-readonly",
            expect_ok=False,
            json={"sql": sql},
            timeout=30,
        )
        assert_ok(status >= 400, f"Dangerous SQL was not blocked: {sql}; payload={payload}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-make-check",
        action="store_true",
        help="Skip the host-side make validate-query recursion check. Used by the Makefile target itself.",
    )
    args = parser.parse_args()

    logger = setup_logging("validate_query_phase")
    logger.info("Validating Phase 4 Trino query layer")

    if not args.skip_make_check and not running_inside_container():
        run_make_validation(logger)
        return

    _, health = request_json("GET", "/api/health", timeout=30)
    assert_ok(health.get("status") == "ok", "Dashboard API is not healthy")

    _, refresh = request_json("POST", "/api/query/refresh", json={"action": "metadata"}, timeout=120)
    assert_ok(refresh.get("status") == "ok", f"Query layer refresh failed: {refresh}")

    _, overview = request_json("GET", "/api/query/overview", timeout=60)
    trino_status = overview.get("trino_status") or {}
    assert_ok(trino_status.get("status") == "ok", f"Trino API is not healthy: {trino_status}")
    assert_ok("delta" in overview.get("connected_catalogs", []), "Delta catalog is not connected")

    _, silver_tables = request_json("GET", "/api/query/silver-tables", timeout=90)
    silver_names = {table["table_name"] for table in silver_tables}
    assert_ok(silver_names, "No dynamic Silver tables are visible through Trino")

    _, generated = request_json("POST", "/api/query/generate-safe-views", timeout=180)
    assert_ok(generated.get("status") in {"ok", "warning"}, f"Dynamic safe view generation failed: {generated}")

    _, safe_views = request_json("GET", "/api/query/safe-views", timeout=120)
    safe_view_names = {view["view_name"] for view in safe_views if view.get("status") == "ok"}
    expected_view_names = {safe_view_name_for_table(table_name) for table_name in silver_names}
    missing_safe_views = sorted(expected_view_names - safe_view_names)
    assert_ok(safe_view_names, f"No dynamic safe views are available; expected one of {sorted(expected_view_names)}")

    first_safe_view = sorted(safe_view_names)[0]
    first_silver_table = sorted(silver_names)[0]
    for view in safe_views:
        if view["view_name"] not in expected_view_names or view.get("status") != "ok":
            continue
        columns = {column.lower() for column in view.get("columns", [])}
        raw_pii = sorted(column for column in columns if is_pii_column(column))
        assert_ok(not raw_pii, f"{view['view_name']} exposes raw PII columns: {raw_pii}")
        assert_ok(view.get("pii_safe") is True, f"{view['view_name']} is not marked PII safe")

        _, preview = request_json(
            "GET",
            f"/api/query/safe-views/{view['view_name']}/preview",
            params={"limit": 25},
            timeout=60,
        )
        assert_ok(len(preview.get("records", [])) > 0, f"{view['view_name']} preview returned no rows")

    _, validation = request_json("POST", "/api/query/validate", timeout=180)
    assert_ok(validation.get("status") in {"ok", "warning"}, f"Query validation failed: {validation}")
    failed_checks = [check for check in validation.get("checks", []) if check.get("status") == "failed"]
    assert_ok(not failed_checks, f"Query validation has failed checks: {failed_checks}")
    if missing_safe_views:
        logger.warning("Dynamic safe views missing but classified as warning: %s", missing_safe_views)

    _, performance = request_json("GET", "/api/query/performance", timeout=180)
    assert_ok(performance.get("status") == "ok", f"Query performance validation failed: {performance}")
    assert_ok(performance.get("total_checks", 0) >= len(safe_view_names) * 2, "Performance validation did not run count and sample checks")
    assert_ok(performance.get("failed_checks", 0) == 0, f"Performance validation has failed checks: {performance}")

    _, readonly = request_json(
        "POST",
        "/api/query/run-readonly",
        json={"sql": f'SELECT * FROM "{first_safe_view}"'},
        timeout=60,
    )
    assert_ok(readonly.get("status") == "ok", f"Read-only query failed: {readonly}")
    assert_ok(readonly.get("row_count", 0) <= 100, "Read-only query did not enforce the automatic result limit")

    dangerous_sql_is_blocked(first_safe_view, first_silver_table)

    _, history = request_json("GET", "/api/query/history", timeout=60)
    assert_ok(history, "Query history endpoint returned no records")
    assert_ok(any(item.get("selected_view") == first_safe_view for item in history), "Query history did not capture dashboard read-only queries")

    _, slow = request_json("GET", "/api/query/slow", timeout=60)
    assert_ok(isinstance(slow, list), "Slow query endpoint did not return a list")

    logger.info("Phase 4 Query validation passed")


if __name__ == "__main__":
    main()
