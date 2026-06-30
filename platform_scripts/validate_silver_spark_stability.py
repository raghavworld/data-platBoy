from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def function_body(source: str, name: str) -> str:
    pattern = re.compile(rf"^def {re.escape(name)}\(.*?(?=^def |\Z)", re.MULTILINE | re.DOTALL)
    match = pattern.search(source)
    assert_ok(match is not None, f"{name} function is missing")
    return match.group(0)


def main() -> None:
    silver_job = (PROJECT_ROOT / "scripts" / "silver_bronze_to_delta.py").read_text(encoding="utf-8")
    dashboard_api = (PROJECT_ROOT / "dashboard" / "api" / "app" / "main.py").read_text(encoding="utf-8")
    dashboard_db = (PROJECT_ROOT / "scripts" / "dashboard_db.py").read_text(encoding="utf-8")
    silver_page = (PROJECT_ROOT / "dashboard" / "web" / "src" / "main.jsx").read_text(encoding="utf-8")

    delta_exists_body = function_body(silver_job, "delta_exists")
    assert_ok(".collect(" not in delta_exists_body, "delta_exists must not use Spark collect()")
    assert_ok("spark.read.format" not in delta_exists_body, "delta_exists must not trigger Spark reads")
    assert_ok("delta_log_exists" in delta_exists_body, "delta_exists must check the Delta log path")

    assert_ok("SparkJVMCrashed" in silver_job, "Spark JVM crash detection is missing")
    assert_ok("recommended_fix" in silver_job, "Silver failures must include a recommended fix")
    assert_ok("full_stack_trace" in silver_job and "error_message" in silver_job, "Silver failures must store full trace separately from summary")
    assert_ok("possibly_corrupt" in silver_job, "Silver partial/corrupt table detection is missing")
    assert_ok("spark = build_spark_session()" in silver_job and "spark.stop()" in silver_job, "Silver should restart Spark after JVM crash where possible")
    assert_ok("SILVER_SPARK_SHUFFLE_PARTITIONS" in silver_job, "Spark shuffle partition hardening is missing")
    assert_ok("SILVER_GENERIC_FLATTEN_MAX_DEPTH" in silver_job, "Generic flatten depth cap is missing")
    assert_ok("SILVER_GENERIC_FLATTEN_MAX_COLUMNS" in silver_job, "Generic flatten column cap is missing")

    assert_ok("error_type TEXT" in dashboard_db, "silver_batch_fingerprints.error_type column is missing")
    assert_ok("full_stack_trace TEXT" in dashboard_db, "silver_batch_fingerprints.full_stack_trace column is missing")
    assert_ok("recommended_fix TEXT" in dashboard_db, "silver_batch_fingerprints.recommended_fix column is missing")
    assert_ok("failed_table_names_json" in dashboard_db, "silver_processing_runs failed table names are missing")

    for route in [
        '@app.get("/api/silver/failed-tables")',
        '@app.post("/api/silver/retry-failed")',
        '@app.post("/api/silver/tables/{table_name}/flush")',
        '@app.post("/api/silver/tables/{table_name}/rebuild")',
    ]:
        assert_ok(route in dashboard_api, f"Missing API route: {route}")
    assert_ok("target_tables" in dashboard_api, "Silver table-level retry must pass target_tables to Airflow")
    assert_ok("delete_delta_prefix" in dashboard_api, "Silver table flush must delete only the target prefix")

    assert_ok("View full trace" in silver_page, "Dashboard must keep full traces expandable")
    assert_ok("Retry all failed Silver tables" in silver_page, "Dashboard retry-all action is missing")
    assert_ok("Flush failed Silver table" in silver_page, "Dashboard table flush action is missing")
    assert_ok("possibly_corrupt" in silver_page, "Dashboard must show possible corruption state")

    print("Silver Spark stability validation passed")


if __name__ == "__main__":
    main()
