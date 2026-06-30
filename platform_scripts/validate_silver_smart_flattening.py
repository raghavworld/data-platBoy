from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import DictCursor

from common import setup_logging
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db
from validate_silver_transformation_quality import run_silver_transformation_quality_validation


REQUIRED_CHILD_COLUMNS = {
    "parent_record_id",
    "parent_record_hash",
    "source_db",
    "source_collection",
    "source_table",
    "child_path",
    "child_index",
    "silver_ingested_at",
    "lineage_reference",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize_json(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [normalize_json(item) for item in value]
    return value


def parse_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def fetch_rows(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query, params)
            return [as_dict(row) for row in cursor.fetchall()]


def fetch_scalar(query: str, params: tuple[Any, ...] = ()) -> Any:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
    return row[0] if row else None


def latest_schema_fields(table_name: str) -> list[str]:
    rows = fetch_rows(
        """
        SELECT fields_json
        FROM silver_schema_snapshots
        WHERE silver_table_name = %s
        ORDER BY detected_at DESC
        LIMIT 1
        """,
        (table_name,),
    )
    if not rows:
        return []
    fields_json = parse_json(rows[0].get("fields_json") or {})
    fields = fields_json.get("fields") if isinstance(fields_json, dict) else []
    if not isinstance(fields, list):
        return []
    return [str(field.get("name") or field.get("column_name") or "") for field in fields if isinstance(field, dict)]


def child_table_targets(plans: list[dict[str, Any]]) -> list[str]:
    targets: list[str] = []
    for plan in plans:
        generated = parse_json(plan.get("generated_child_tables_json") or [])
        if not isinstance(generated, list):
            continue
        for child in generated:
            if isinstance(child, dict) and child.get("table_name"):
                targets.append(str(child["table_name"]))
            elif isinstance(child, str):
                targets.append(child)
    return sorted(set(targets))


def check(status: str, name: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "message": message,
        "details": details or {},
    }


def run_silver_smart_flattening_validation() -> dict[str, Any]:
    init_dashboard_db()
    quality = run_silver_transformation_quality_validation(persist=True)
    quality_summary = quality.get("summary") or {}

    tables = fetch_rows("SELECT * FROM silver_collection_states ORDER BY silver_table_name")
    plans = fetch_rows("SELECT * FROM silver_transform_plans ORDER BY last_planned_at DESC")
    profile_count = int(fetch_scalar("SELECT count(*) FROM silver_field_profiles") or 0)
    child_tables = [row for row in tables if row.get("is_child_table")]
    planned_child_tables = child_table_targets(plans)
    missing_planned_children = sorted(
        table_name
        for table_name in planned_child_tables
        if table_name not in {row.get("silver_table_name") for row in child_tables}
    )

    child_metadata_gaps = [
        row.get("silver_table_name")
        for row in child_tables
        if not row.get("parent_silver_table_name") or not row.get("child_path") or not row.get("lineage_reference")
    ]
    child_schema_gaps: dict[str, list[str]] = {}
    for row in child_tables:
        table_name = row.get("silver_table_name")
        if not table_name:
            continue
        schema_fields = set(latest_schema_fields(str(table_name)))
        if not schema_fields:
            continue
        missing_columns = sorted(REQUIRED_CHILD_COLUMNS - schema_fields)
        if missing_columns:
            child_schema_gaps[str(table_name)] = missing_columns

    dynamic_config_tables = [
        row.get("silver_table_name")
        for row in tables
        if row.get("table_classification") in {"dynamic_config_like", "operational_reference", "low_analytics_value"}
    ]
    quality_tables = quality.get("tables") or []
    blocking_quality = [row.get("silver_table_name") for row in quality_tables if row.get("blocking")]
    invalid_sql_tables = [
        row.get("silver_table_name")
        for row in quality_tables
        if row.get("invalid_columns") or row.get("duplicate_columns")
    ]
    raw_json_dependency_score = float(quality_summary.get("average_raw_json_dependency_score") or 0)
    readiness_score = float(quality_summary.get("average_readiness_score") or 0)
    flattening_score = float(quality_summary.get("average_flattening_score") or 0)

    checks = [
        check(
            "ok" if tables else "failed",
            "silver_tables_registered",
            f"{len(tables)} Silver table(s) registered.",
            {"table_count": len(tables)},
        ),
        check(
            "ok" if plans else "failed",
            "transform_plans_generated",
            f"{len(plans)} Smart Silver transform plan(s) generated.",
            {"plan_count": len(plans)},
        ),
        check(
            "ok" if profile_count else "failed",
            "field_profiles_generated",
            f"{profile_count} Silver field profile row(s) generated.",
            {"field_profile_count": profile_count},
        ),
        check(
            "ok" if child_tables else "warning" if planned_child_tables else "info",
            "child_tables_generated",
            f"{len(child_tables)} child Silver table(s) registered.",
            {
                "child_table_count": len(child_tables),
                "planned_child_tables": planned_child_tables,
                "missing_planned_children": missing_planned_children,
            },
        ),
        check(
            "failed" if child_metadata_gaps or child_schema_gaps else "ok",
            "child_relationships_preserved",
            "Child table lineage metadata is complete." if not child_metadata_gaps and not child_schema_gaps else "Child table relationship metadata is incomplete.",
            {
                "metadata_gaps": child_metadata_gaps,
                "schema_gaps": child_schema_gaps,
                "required_columns": sorted(REQUIRED_CHILD_COLUMNS),
            },
        ),
        check(
            "ok" if raw_json_dependency_score >= 70 else "warning" if raw_json_dependency_score >= 50 else "failed",
            "raw_json_dependency_controlled",
            f"Average raw_json dependency score is {raw_json_dependency_score}.",
            {"average_raw_json_dependency_score": raw_json_dependency_score},
        ),
        check(
            "failed" if invalid_sql_tables else "ok",
            "sql_readability",
            "Smart Silver schemas are SQL-safe." if not invalid_sql_tables else "Some Smart Silver schemas are not SQL-safe.",
            {"invalid_sql_tables": invalid_sql_tables},
        ),
        check(
            "failed" if blocking_quality or readiness_score < 70 else "warning" if readiness_score < 85 else "ok",
            "analytics_readiness",
            f"Average Smart Silver readiness score is {readiness_score}.",
            {
                "average_readiness_score": readiness_score,
                "average_flattening_score": flattening_score,
                "blocking_quality_tables": blocking_quality,
            },
        ),
        check(
            "ok" if dynamic_config_tables else "info",
            "dynamic_collections_supported",
            f"{len(dynamic_config_tables)} dynamic/config-like Silver table(s) classified safely.",
            {"dynamic_config_tables": dynamic_config_tables},
        ),
    ]

    if any(item["status"] == "failed" for item in checks):
        status = "failed"
    elif any(item["status"] == "warning" for item in checks):
        status = "warning"
    else:
        status = "passed"

    label = "Passed" if status == "passed" else "Warning" if status == "warning" else "Failed"
    return {
        "status": status,
        "message": f"{label}: Smart Silver flattening validation completed.",
        "summary": {
            "silver_tables": len(tables),
            "transform_plans": len(plans),
            "field_profiles": profile_count,
            "child_tables_generated": len(child_tables),
            "average_readiness_score": readiness_score,
            "average_flattening_score": flattening_score,
            "average_raw_json_dependency_score": raw_json_dependency_score,
            "blocking_quality_tables": len(blocking_quality),
        },
        "checks": checks,
        "validated_at": utc_now(),
    }


def main() -> None:
    logger = setup_logging("validate_silver_smart_flattening")
    result = run_silver_smart_flattening_validation()
    logger.info(result["message"])
    print(json.dumps(normalize_json(result), indent=2, sort_keys=True))
    if result["status"] == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
