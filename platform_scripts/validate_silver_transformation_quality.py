from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import DictCursor

from common import bronze_table_name, setup_logging, trino_connection
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db, json_param, new_id
from query_layer import is_pii_column


SQL_SAFE_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DELTA_INVALID_CHARS = set(" ,;{}()\n\t=")
DATE_TIME_SUFFIXES = ("_at", "_date")
NUMERIC_MARKERS = ("amount", "price", "total")
BOOLEAN_PREFIXES = ("is_", "has_")
RAW_JSON_SUFFIX = "_raw_json"
AUDIT_COLUMNS = {
    "source_id",
    "source_name",
    "source_database",
    "source_collection",
    "raw_run_id",
    "raw_file_id",
    "raw_object_key",
    "bronze_record_hash",
    "bronze_ingested_at",
    "silver_record_hash",
    "silver_processed_at",
    "silver_ingestion_date",
}
CHILD_TABLE_CANDIDATES = {
    "items",
    "documents",
    "transactions",
    "attachments",
    "events",
}
CONFIG_LIKE_MARKERS = ("userpreference", "preference", "settings", "setting", "filters", "filter", "configs", "config")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def compact_status(status: str) -> str:
    return "passed" if status == "ok" else status


def status_to_api(status: str) -> str:
    return "ok" if status == "passed" else status


def normalize_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: normalize_json(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [normalize_json(item) for item in value]
    return value


def field_type(field: dict[str, Any]) -> str:
    return str(field.get("type") or field.get("data_type") or "").lower()


def field_name(field: dict[str, Any]) -> str:
    return str(field.get("name") or field.get("column_name") or "")


def top_level_name(name: str) -> str:
    return name.replace("[]", "").split(".", 1)[0]


def is_complex_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return (
        lowered.startswith(("row(", "struct<", "array(", "array<", "map(", "map<"))
        or lowered in {"json", "object"}
        or "array<" in lowered
        or "struct<" in lowered
        or "map<" in lowered
    )


def is_struct_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return lowered.startswith(("row(", "struct<")) or "struct<" in lowered


def is_array_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return lowered.startswith(("array(", "array<")) or "array<" in lowered


def is_map_json_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return lowered.startswith(("map(", "map<")) or lowered in {"json", "object"} or "map<" in lowered


def is_numeric_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return any(
        token in lowered
        for token in ("decimal", "double", "real", "integer", "bigint", "smallint", "tinyint", "number", "numeric")
    )


def is_timestamp_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return "timestamp" in lowered or lowered == "date"


def is_date_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return lowered == "date" or "timestamp" in lowered


def is_boolean_type(data_type: str) -> bool:
    return data_type.lower() in {"boolean", "bool"}


def is_config_like_table(table_name: str, source_collection: str | None = None) -> bool:
    lowered = f"{table_name} {source_collection or ''}".lower()
    return any(marker in lowered for marker in CONFIG_LIKE_MARKERS)


def classify_table(table_name: str, source_collection: str | None, fields: list[dict[str, Any]]) -> str:
    lowered = f"{table_name} {source_collection or ''}".lower()
    if is_config_like_table(table_name, source_collection):
        return "dynamic_config_like"
    if any(marker in lowered for marker in ("log", "audit", "event")):
        return "log_like"
    if any(marker in lowered for marker in ("order", "payment", "transaction", "invoice")):
        return "transactional"
    if any(marker in lowered for marker in ("master", "role", "permission", "team", "tier", "default", "lookup")):
        return "reference_data"
    if any(is_pii_column(field_name(field)) for field in fields):
        return "pii_sensitive"
    return "analytics_ready"


def bi_suitability(table_classification: str, readiness_score: float) -> str:
    if readiness_score >= 85 and table_classification not in {"dynamic_config_like", "log_like", "low_analytics_value"}:
        return "bi_ready"
    if table_classification in {"dynamic_config_like", "log_like", "low_analytics_value"}:
        return "limited"
    if readiness_score < 70:
        return "needs_custom_transform"
    return "usable_with_warnings"


def latest_transform_plan(source_database: str | None, source_collection: str | None) -> dict[str, Any]:
    if not source_database or not source_collection:
        return {}
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM silver_transform_plans
                WHERE source_database = %s AND source_collection = %s
                ORDER BY last_planned_at DESC
                LIMIT 1
                """,
                (source_database, source_collection),
            )
            row = cursor.fetchone()
    return as_dict(row)


def latest_silver_schema(table_name: str) -> list[dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT fields_json
                FROM silver_schema_snapshots
                WHERE silver_table_name = %s
                ORDER BY detected_at DESC
                LIMIT 1
                """,
                (table_name,),
            )
            row = cursor.fetchone()
    if not row:
        return []
    fields_json = row["fields_json"] or {}
    if isinstance(fields_json, str):
        fields_json = json.loads(fields_json)
    fields = fields_json.get("fields") or []
    return fields if isinstance(fields, list) else []


def latest_bronze_schema(database_name: str | None, collection_name: str | None) -> list[dict[str, Any]]:
    if not database_name or not collection_name:
        return []
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT fields_json
                FROM bronze_schema_snapshots
                WHERE database_name = %s AND collection_name = %s
                ORDER BY detected_at DESC
                LIMIT 1
                """,
                (database_name, collection_name),
            )
            row = cursor.fetchone()
    if not row:
        return []
    fields_json = row["fields_json"] or {}
    if isinstance(fields_json, str):
        fields_json = json.loads(fields_json)
    fields = fields_json.get("fields") or []
    return fields if isinstance(fields, list) else []


def describe_trino_table(table_name: str) -> list[dict[str, str]]:
    connection = trino_connection(schema="silver")
    cursor = connection.cursor()
    try:
        cursor.execute(f'SHOW COLUMNS FROM delta.silver."{table_name}"')
        return [{"name": str(row[0]), "type": str(row[1]), "nullable": ""} for row in cursor.fetchall()]
    finally:
        cursor.close()
        connection.close()


def visible_bronze_schema(database_name: str | None, collection_name: str | None) -> list[dict[str, str]]:
    if not database_name or not collection_name:
        return []
    table_name = bronze_table_name(database_name, collection_name)
    connection = trino_connection(schema="bronze")
    cursor = connection.cursor()
    try:
        cursor.execute(f'SHOW COLUMNS FROM delta.bronze."{table_name}"')
        return [{"name": str(row[0]), "type": str(row[1]), "nullable": ""} for row in cursor.fetchall()]
    except Exception:
        return []
    finally:
        cursor.close()
        connection.close()


def table_states() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM silver_collection_states
                ORDER BY silver_table_name
                """
            )
            return [as_dict(row) for row in cursor.fetchall()]


def top_level_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for field in fields:
        name = field_name(field)
        if not name or "." in name or "[]" in name:
            continue
        seen.setdefault(name, field)
    return list(seen.values())


def invalid_column_reason(column_name: str) -> str | None:
    if not column_name:
        return "empty column name"
    if not SQL_SAFE_COLUMN.match(column_name):
        return "column is not a plain SQL identifier"
    if any(char in column_name for char in DELTA_INVALID_CHARS):
        return "column contains a Delta-invalid character"
    return None


def duplicate_columns(columns: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for column in columns:
        lowered = column.lower()
        if lowered in seen:
            duplicates.add(column)
        seen.add(lowered)
    return sorted(duplicates)


def score_from_defects(total: int, defects: int, penalty: int = 30) -> float:
    if total <= 0:
        return 0.0
    return max(0.0, min(100.0, 100.0 - (defects * penalty)))


def type_warnings_for_fields(fields: list[dict[str, Any]]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    for field in fields:
        name = field_name(field)
        data_type = field_type(field)
        if not name or name in AUDIT_COLUMNS or name.lower().endswith(RAW_JSON_SUFFIX):
            continue
        lowered = name.lower()
        tokens = [token for token in lowered.split("_") if token]
        if lowered.endswith("_at") and not is_timestamp_type(data_type):
            warnings.append({"column": name, "type": data_type, "expected": "timestamp", "reason": "*_at fields should be timestamps"})
        elif lowered.endswith("_date") and not is_date_type(data_type):
            warnings.append({"column": name, "type": data_type, "expected": "date/timestamp", "reason": "*_date fields should be dates"})
        numeric_signal = bool(tokens) and (
            tokens[-1] in NUMERIC_MARKERS
            or lowered.endswith(("_amount", "_price", "_total"))
            or "total_amount" in lowered
            or "price_amount" in lowered
        )
        if numeric_signal and not is_numeric_type(data_type):
            warnings.append({"column": name, "type": data_type, "expected": "numeric/decimal", "reason": "amount/price/total fields should be numeric"})
        if lowered.startswith(BOOLEAN_PREFIXES) and not is_boolean_type(data_type):
            warnings.append({"column": name, "type": data_type, "expected": "boolean", "reason": "is_* and has_* fields should be boolean"})
    return warnings


def is_non_sensitive_pii_flag(column_name: str, data_type: str) -> bool:
    lowered = column_name.lower()
    if not is_boolean_type(data_type):
        return False
    return (
        lowered.startswith(("is", "has", "prefers"))
        and any(marker in lowered for marker in ("email", "phone", "sms", "mobile"))
        and any(marker in lowered for marker in ("verified", "verification", "notification", "enabled", "active"))
    )


def pii_warnings_for_fields(fields: list[dict[str, Any]]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    for field in fields:
        name = field_name(field)
        data_type = field_type(field)
        lowered = name.lower()
        if not name or name in AUDIT_COLUMNS or lowered.endswith(RAW_JSON_SUFFIX):
            continue
        if is_non_sensitive_pii_flag(name, data_type):
            continue
        if is_pii_column(name) or any(marker in lowered for marker in ("password", "secret", "token", "credential")):
            warnings.append(
                {
                    "column": name,
                    "severity": "warning",
                    "reason": "Governed PII may remain in Silver, but generated safe analytics views must hash or exclude it.",
                }
            )
    return warnings


def child_table_recommendations(
    bronze_fields: list[dict[str, Any]],
    silver_fields: list[dict[str, Any]],
    table_name: str,
    all_silver_tables: set[str],
) -> list[dict[str, str]]:
    recommendations: list[dict[str, str]] = []
    array_roots: set[str] = set()
    for field in [*bronze_fields, *silver_fields]:
        name = field_name(field)
        root = top_level_name(name)
        lowered = root.lower()
        if not root:
            continue
        if is_array_type(field_type(field)) or lowered in CHILD_TABLE_CANDIDATES:
            if lowered in CHILD_TABLE_CANDIDATES or any(candidate in lowered for candidate in CHILD_TABLE_CANDIDATES):
                array_roots.add(root)

    base = table_name[:-6] if table_name.endswith("_clean") else table_name
    for root in sorted(array_roots):
        root_safe = re.sub(r"[^a-z0-9_]+", "_", root.lower()).strip("_")
        expected_markers = {root_safe, root_safe.rstrip("s")}
        has_child = any(candidate != table_name and candidate.startswith(base) and any(marker in candidate for marker in expected_markers) for candidate in all_silver_tables)
        if not has_child:
            recommendations.append(
                {
                    "field": root,
                    "message": "Potential child table recommended",
                    "recommended_table": f"{base}__{root_safe}_clean",
                }
            )
    return recommendations


def bronze_silver_difference(
    bronze_fields: list[dict[str, Any]],
    silver_fields: list[dict[str, Any]],
) -> dict[str, Any]:
    bronze_top = top_level_fields(bronze_fields)
    silver_top = top_level_fields(silver_fields)
    bronze_names = {field_name(field) for field in bronze_top if field_name(field) not in AUDIT_COLUMNS}
    silver_names = {field_name(field) for field in silver_top if field_name(field) not in AUDIT_COLUMNS}
    bronze_complex = sum(1 for field in bronze_top if is_complex_type(field_type(field)))
    silver_complex = sum(1 for field in silver_top if is_complex_type(field_type(field)))
    audit_added = sorted(name for name in AUDIT_COLUMNS if name in {field_name(field) for field in silver_top})
    overlap_ratio = len(bronze_names & silver_names) / max(len(bronze_names), 1)
    flattened_name_signals = sum(1 for name in silver_names if "_" in name and name not in bronze_names)
    raw_json_fallbacks = [field_name(field) for field in silver_top if field_name(field).endswith(RAW_JSON_SUFFIX)]
    transformation_signals = [
        {"name": "fewer_nested_structures", "present": silver_complex < bronze_complex},
        {"name": "sanitized_or_flattened_names", "present": flattened_name_signals > 0},
        {"name": "raw_json_fallbacks", "present": bool(raw_json_fallbacks)},
        {"name": "clean_metadata_added", "present": {"silver_record_hash", "silver_processed_at", "silver_ingestion_date"}.issubset(set(audit_added))},
    ]
    copy_suspected = bool(bronze_fields) and overlap_ratio > 0.9 and silver_complex >= bronze_complex and not any(signal["present"] for signal in transformation_signals)
    return {
        "bronze_column_count": len(bronze_top),
        "silver_column_count": len(silver_top),
        "bronze_complex_columns": bronze_complex,
        "silver_complex_columns": silver_complex,
        "overlap_ratio": round(overlap_ratio, 3),
        "audit_columns_added": audit_added,
        "transformation_signals": transformation_signals,
        "copy_suspected": copy_suspected,
    }


def evaluate_table(state: dict[str, Any], all_silver_tables: set[str]) -> dict[str, Any]:
    table_name = state["silver_table_name"]
    source_database = state.get("source_database")
    source_collection = state.get("source_collection")
    silver_fields = latest_silver_schema(table_name)
    if not silver_fields:
        silver_fields = describe_trino_table(table_name)
    bronze_fields = latest_bronze_schema(source_database, source_collection) or visible_bronze_schema(source_database, source_collection)
    top_fields = top_level_fields(silver_fields)
    columns = [field_name(field) for field in top_fields]

    nested_struct_columns = [field_name(field) for field in top_fields if is_struct_type(field_type(field))]
    array_columns = [field_name(field) for field in top_fields if is_array_type(field_type(field))]
    map_json_columns = [field_name(field) for field in top_fields if is_map_json_type(field_type(field))]
    raw_json_columns = [name for name in columns if name.endswith(RAW_JSON_SUFFIX)]
    complex_columns = [field_name(field) for field in top_fields if is_complex_type(field_type(field))]
    unapproved_complex = [name for name in complex_columns if not name.endswith(RAW_JSON_SUFFIX)]
    nested_paths = [field_name(field) for field in silver_fields if "." in field_name(field) or "[]" in field_name(field)]
    primitive_flat_columns = [field_name(field) for field in top_fields if not is_complex_type(field_type(field))]

    invalid_columns = [{"column": name, "reason": reason} for name in columns if (reason := invalid_column_reason(name))]
    duplicates = duplicate_columns(columns)
    type_warnings = type_warnings_for_fields(top_fields)
    pii_warnings = pii_warnings_for_fields(top_fields)
    child_recommendations = child_table_recommendations(bronze_fields, silver_fields, table_name, all_silver_tables)
    comparison = bronze_silver_difference(bronze_fields, silver_fields)
    plan = latest_transform_plan(source_database, source_collection)
    table_classification = (
        state.get("table_classification")
        or plan.get("table_classification")
        or classify_table(table_name, source_collection, top_fields)
    )

    total_columns = max(len(columns), 1)
    complex_penalty_units = len(unapproved_complex) * 2 + len(nested_paths)
    raw_json_penalty = 1.5 if table_classification in {"dynamic_config_like", "log_like"} else 3.0
    flattening_score = max(0.0, 100.0 - (complex_penalty_units * 12.0) - (len(raw_json_columns) * raw_json_penalty))
    column_safety_score = score_from_defects(total_columns, len(invalid_columns) + len(duplicates), penalty=25)
    type_quality_score = score_from_defects(total_columns, len(type_warnings), penalty=18)
    pii_safety_score = score_from_defects(total_columns, len(pii_warnings), penalty=8)
    transformation_score = 100.0 if not comparison["copy_suspected"] else 55.0
    raw_json_dependency_score = max(0.0, min(100.0, 100.0 - ((len(raw_json_columns) / total_columns) * 100.0)))
    sql_usability_score = max(
        0.0,
        min(
            100.0,
            column_safety_score
            - (len(unapproved_complex) * 10.0)
            - max(0, len(columns) - 250) * 0.4,
        ),
    )
    bi_readiness_score = round(
        (flattening_score * 0.35)
        + (sql_usability_score * 0.25)
        + (type_quality_score * 0.20)
        + (raw_json_dependency_score * 0.20),
        1,
    )
    analytics_readiness_score = round(
        (flattening_score * 0.30)
        + (sql_usability_score * 0.25)
        + (type_quality_score * 0.20)
        + (pii_safety_score * 0.15)
        + (transformation_score * 0.10),
        1,
    )

    recommendations: list[str] = []
    failed_reasons: list[str] = []
    warning_reasons: list[str] = []
    blocking = False
    if not columns:
        blocking = True
        failed_reasons.append("Silver schema is unavailable or empty.")
        recommendations.append("Rebuild this Silver table and confirm schema snapshots are recorded.")
    if invalid_columns:
        blocking = True
        failed_reasons.append("Invalid SQL/Delta column names remain.")
        recommendations.append("Regenerate Silver using safe_identifier-style column aliases.")
    if duplicates:
        blocking = True
        failed_reasons.append("Duplicate column names remain after case-insensitive normalization.")
        recommendations.append("Deduplicate aliases during Silver flattening.")
    if unapproved_complex:
        warning_reasons.append("Complex columns remain without *_raw_json fallback naming.")
        recommendations.append("Flatten these fields or persist them as documented *_raw_json fallback columns.")
    if len(nested_paths) > max(3, total_columns // 4):
        warning_reasons.append("Too many nested field paths remain in Silver metadata.")
        recommendations.append("Increase flatten depth or split complex structures into child tables.")
    if type_warnings:
        warning_reasons.append("Obvious date, numeric, or boolean fields are not normalized.")
        recommendations.append("Cast *_at/*_date, amount/price/total, and is_/has_ fields during Silver processing.")
    if pii_warnings:
        warning_reasons.append("Raw PII-like columns are present in Silver.")
        recommendations.append("Keep PII governed in Silver and ensure safe analytics views hash or remove raw PII columns.")
    if comparison["copy_suspected"]:
        warning_reasons.append("Silver appears to be copied from Bronze without transformation signals.")
        recommendations.append("Apply flattening, safe aliases, type casts, audit hashes, and child table splits before publishing.")
    if child_recommendations:
        warning_reasons.append("Array-like fields may need child tables.")
        recommendations.extend(item["message"] for item in child_recommendations)
    if analytics_readiness_score < 70 and table_classification not in {"dynamic_config_like", "log_like", "low_analytics_value"}:
        failed_reasons.append("Analytics readiness score is below 70.")
    elif analytics_readiness_score < 85:
        warning_reasons.append("Analytics readiness score is below 85.")
    if table_classification in {"dynamic_config_like", "log_like", "low_analytics_value"}:
        warning_reasons.append(f"Table classified as {table_classification}; lower BI readiness is non-blocking by default.")
        recommendations.append("Create a custom transform only if this operational/config table becomes business-critical for BI.")

    if blocking or failed_reasons:
        status = "failed"
    elif warning_reasons:
        status = "warning"
    else:
        status = "passed"
    severity = "blocking_failure" if blocking else "warning" if status in {"failed", "warning"} else "info"
    recommendation_type = (
        "child_table" if child_recommendations
        else "custom_transform" if analytics_readiness_score < 70
        else "governance" if pii_warnings
        else "optimization" if recommendations
        else None
    )
    governance_status = "governed_pii" if pii_warnings else "safe"

    return {
        "silver_table_name": table_name,
        "source_database": source_database,
        "source_collection": source_collection,
        "status": status,
        "severity": severity,
        "blocking": blocking,
        "bi_suitability": bi_suitability(table_classification, analytics_readiness_score),
        "governance_status": governance_status,
        "recommendation_type": recommendation_type,
        "table_classification": table_classification,
        "is_child_table": bool(state.get("is_child_table")),
        "flattening_score": round(flattening_score, 1),
        "sql_usability_score": round(sql_usability_score, 1),
        "bi_readiness_score": bi_readiness_score,
        "raw_json_dependency_score": round(raw_json_dependency_score, 1),
        "column_safety_score": round(column_safety_score, 1),
        "type_quality_score": round(type_quality_score, 1),
        "pii_safety_score": round(pii_safety_score, 1),
        "analytics_readiness_score": analytics_readiness_score,
        "total_columns": len(columns),
        "nested_struct_columns": len(nested_struct_columns),
        "array_columns": len(array_columns),
        "map_json_columns": len(map_json_columns),
        "nested_columns_remaining": len(nested_paths) + len(unapproved_complex),
        "primitive_flat_columns": len(primitive_flat_columns),
        "raw_json_columns": raw_json_columns,
        "invalid_columns": invalid_columns,
        "duplicate_columns": duplicates,
        "type_warnings": type_warnings,
        "pii_warnings": pii_warnings,
        "child_table_recommendations": child_recommendations,
        "bronze_comparison": comparison,
        "failed_reasons": failed_reasons,
        "warning_reasons": warning_reasons,
        "recommendations": sorted(set(recommendations)),
        "last_validated_at": utc_now(),
    }


def persist_results(results: list[dict[str, Any]]) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for result in results:
                cursor.execute(
                    """
                    INSERT INTO silver_transformation_quality (
                        id, silver_table_name, source_database, source_collection, status,
                        severity, blocking, bi_suitability, governance_status,
                        recommendation_type, table_classification,
                        flattening_score, sql_usability_score, bi_readiness_score,
                        raw_json_dependency_score, column_safety_score, type_quality_score,
                        pii_safety_score, analytics_readiness_score, total_columns,
                        nested_struct_columns, array_columns, map_json_columns,
                        nested_columns_remaining, primitive_flat_columns,
                        raw_json_columns_json, invalid_columns_json, duplicate_columns_json,
                        type_warnings_json, pii_warnings_json, child_table_recommendations_json,
                        bronze_comparison_json, failed_reasons_json, warning_reasons_json,
                        recommendations_json, last_validated_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        now()
                    )
                    ON CONFLICT (silver_table_name)
                    DO UPDATE SET
                        source_database = EXCLUDED.source_database,
                        source_collection = EXCLUDED.source_collection,
                        status = EXCLUDED.status,
                        severity = EXCLUDED.severity,
                        blocking = EXCLUDED.blocking,
                        bi_suitability = EXCLUDED.bi_suitability,
                        governance_status = EXCLUDED.governance_status,
                        recommendation_type = EXCLUDED.recommendation_type,
                        table_classification = EXCLUDED.table_classification,
                        flattening_score = EXCLUDED.flattening_score,
                        sql_usability_score = EXCLUDED.sql_usability_score,
                        bi_readiness_score = EXCLUDED.bi_readiness_score,
                        raw_json_dependency_score = EXCLUDED.raw_json_dependency_score,
                        column_safety_score = EXCLUDED.column_safety_score,
                        type_quality_score = EXCLUDED.type_quality_score,
                        pii_safety_score = EXCLUDED.pii_safety_score,
                        analytics_readiness_score = EXCLUDED.analytics_readiness_score,
                        total_columns = EXCLUDED.total_columns,
                        nested_struct_columns = EXCLUDED.nested_struct_columns,
                        array_columns = EXCLUDED.array_columns,
                        map_json_columns = EXCLUDED.map_json_columns,
                        nested_columns_remaining = EXCLUDED.nested_columns_remaining,
                        primitive_flat_columns = EXCLUDED.primitive_flat_columns,
                        raw_json_columns_json = EXCLUDED.raw_json_columns_json,
                        invalid_columns_json = EXCLUDED.invalid_columns_json,
                        duplicate_columns_json = EXCLUDED.duplicate_columns_json,
                        type_warnings_json = EXCLUDED.type_warnings_json,
                        pii_warnings_json = EXCLUDED.pii_warnings_json,
                        child_table_recommendations_json = EXCLUDED.child_table_recommendations_json,
                        bronze_comparison_json = EXCLUDED.bronze_comparison_json,
                        failed_reasons_json = EXCLUDED.failed_reasons_json,
                        warning_reasons_json = EXCLUDED.warning_reasons_json,
                        recommendations_json = EXCLUDED.recommendations_json,
                        last_validated_at = EXCLUDED.last_validated_at,
                        updated_at = now()
                    """,
                    (
                        new_id(),
                        result["silver_table_name"],
                        result.get("source_database"),
                        result.get("source_collection"),
                        result["status"],
                        result["severity"],
                        result["blocking"],
                        result["bi_suitability"],
                        result["governance_status"],
                        result.get("recommendation_type"),
                        result.get("table_classification"),
                        result["flattening_score"],
                        result["sql_usability_score"],
                        result["bi_readiness_score"],
                        result["raw_json_dependency_score"],
                        result["column_safety_score"],
                        result["type_quality_score"],
                        result["pii_safety_score"],
                        result["analytics_readiness_score"],
                        result["total_columns"],
                        result["nested_struct_columns"],
                        result["array_columns"],
                        result["map_json_columns"],
                        result["nested_columns_remaining"],
                        result["primitive_flat_columns"],
                        json_param(result["raw_json_columns"]),
                        json_param(result["invalid_columns"]),
                        json_param(result["duplicate_columns"]),
                        json_param(result["type_warnings"]),
                        json_param(result["pii_warnings"]),
                        json_param(result["child_table_recommendations"]),
                        json_param(result["bronze_comparison"]),
                        json_param(result["failed_reasons"]),
                        json_param(result["warning_reasons"]),
                        json_param(result["recommendations"]),
                        result["last_validated_at"],
                    ),
                )


def latest_quality_results(limit: int = 500) -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM silver_transformation_quality
                ORDER BY
                    CASE status WHEN 'failed' THEN 1 WHEN 'warning' THEN 2 WHEN 'passed' THEN 3 ELSE 4 END,
                    analytics_readiness_score ASC,
                    silver_table_name ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = [as_dict(row) for row in cursor.fetchall()]
    return rows


def quality_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    if not total:
        return {
            "average_readiness_score": 0.0,
            "average_flattening_score": 0.0,
            "average_raw_json_dependency_score": 0.0,
            "tables_passed": 0,
            "tables_warning": 0,
            "tables_failed": 0,
            "blocking_failures": 0,
            "bi_ready_tables": 0,
            "needs_custom_transform": 0,
            "child_tables_generated": 0,
            "child_table_recommendations": 0,
            "nested_columns_remaining": 0,
            "status": "unknown",
        }
    passed = sum(1 for row in rows if row.get("status") == "passed")
    warning = sum(1 for row in rows if row.get("status") == "warning")
    failed = sum(1 for row in rows if row.get("status") == "failed")
    blocking_failures = sum(1 for row in rows if row.get("blocking"))
    bi_ready = sum(1 for row in rows if row.get("bi_suitability") == "bi_ready")
    needs_custom = sum(1 for row in rows if row.get("bi_suitability") == "needs_custom_transform" or row.get("recommendation_type") == "custom_transform")
    child_tables_generated = sum(1 for row in rows if row.get("is_child_table"))
    child_recommendations = sum(len(row.get("child_table_recommendations") or row.get("child_table_recommendations_json") or []) for row in rows)
    nested_remaining = sum(int(row.get("nested_columns_remaining") or 0) for row in rows)
    avg_score = round(sum(float(row.get("analytics_readiness_score") or 0) for row in rows) / total, 1)
    avg_flattening = round(sum(float(row.get("flattening_score") or 0) for row in rows) / total, 1)
    avg_raw_json_dependency = round(sum(float(row.get("raw_json_dependency_score") or 0) for row in rows) / total, 1)
    if blocking_failures or avg_score < 70:
        status = "failed"
    elif avg_score < 85 or warning or failed:
        status = "warning"
    else:
        status = "passed"
    return {
        "average_readiness_score": avg_score,
        "average_flattening_score": avg_flattening,
        "average_raw_json_dependency_score": avg_raw_json_dependency,
        "tables_passed": passed,
        "tables_warning": warning,
        "tables_failed": failed,
        "blocking_failures": blocking_failures,
        "bi_ready_tables": bi_ready,
        "needs_custom_transform": needs_custom,
        "child_tables_generated": child_tables_generated,
        "child_table_recommendations": child_recommendations,
        "nested_columns_remaining": nested_remaining,
        "status": status,
    }


def run_silver_transformation_quality_validation(persist: bool = True) -> dict[str, Any]:
    init_dashboard_db()
    states = table_states()
    if not states:
        return {
            "status": "failed",
            "message": "Failed: no Silver tables are registered.",
            "summary": quality_summary([]),
            "tables": [],
            "checks": [
                {
                    "name": "silver_tables_registered",
                    "status": "failed",
                    "message": "No Silver tables are registered.",
                    "details": {"recommended_fix": "Run Silver processing from Bronze, then retry deep Silver quality validation."},
                }
            ],
        }

    all_names = {state["silver_table_name"] for state in states}
    results = [evaluate_table(state, all_names) for state in states]
    if persist:
        persist_results(results)
    summary = quality_summary(results)
    status = summary["status"]
    checks = [
        {
            "name": "silver_tables_registered",
            "status": "ok",
            "message": f"{len(results)} Silver table(s) registered.",
            "details": {"table_count": len(results)},
        },
        {
            "name": "silver_flattening_quality",
            "status": "warning" if summary["nested_columns_remaining"] else "ok",
            "message": f"{summary['nested_columns_remaining']} nested or unapproved complex column(s) remain.",
            "details": {
                "affected_tables": [row["silver_table_name"] for row in results if row["nested_columns_remaining"]],
                "recommended_fix": "Flatten nested structs or persist complex fallback columns as documented *_raw_json fields.",
            },
        },
        {
            "name": "silver_column_names_sql_safe",
            "status": "failed" if any(row["invalid_columns"] or row["duplicate_columns"] for row in results) else "ok",
            "message": "Silver column names are SQL-safe." if not any(row["invalid_columns"] or row["duplicate_columns"] for row in results) else "Invalid or duplicate Silver column names found.",
            "details": {
                "affected_tables": [row["silver_table_name"] for row in results if row["invalid_columns"] or row["duplicate_columns"]],
                "recommended_fix": "Regenerate Silver aliases with safe SQL identifiers and deduplicate collisions.",
            },
        },
        {
            "name": "silver_type_normalization",
            "status": "warning" if any(row["type_warnings"] for row in results) else "ok",
            "message": "Obvious dates, amounts, prices, totals, and booleans are typed." if not any(row["type_warnings"] for row in results) else "Some obvious fields need type normalization.",
            "details": {
                "affected_tables": [row["silver_table_name"] for row in results if row["type_warnings"]],
                "recommended_fix": "Cast *_at/*_date fields, amount/price/total fields, and is_/has_ fields during Silver transformation.",
            },
        },
        {
            "name": "silver_pii_safety",
            "status": "warning" if any(row["pii_warnings"] for row in results) else "ok",
            "message": "No obvious PII fields detected in Silver." if not any(row["pii_warnings"] for row in results) else "PII-like fields are present in governed Silver and must be masked by safe analytics views.",
            "details": {
                "affected_tables": [row["silver_table_name"] for row in results if row["pii_warnings"]],
                "recommended_fix": "Verify generated safe analytics views hash or exclude raw PII. Governed PII in Silver is not blocking by itself.",
            },
        },
        {
            "name": "bronze_vs_silver_transformation",
            "status": "warning" if any(row["bronze_comparison"].get("copy_suspected") for row in results) else "ok",
            "message": "Silver shows transformation signals." if not any(row["bronze_comparison"].get("copy_suspected") for row in results) else "Some Silver tables appear copied from Bronze.",
            "details": {
                "affected_tables": [row["silver_table_name"] for row in results if row["bronze_comparison"].get("copy_suspected")],
                "recommended_fix": "Apply flattening, safe aliases, normalized types, audit metadata, and table splits before marking Silver analytics-ready.",
            },
        },
        {
            "name": "child_table_recommendations",
            "status": "warning" if summary["child_table_recommendations"] else "ok",
            "message": f"{summary['child_table_recommendations']} potential child table recommendation(s).",
            "details": {
                "affected_tables": [row["silver_table_name"] for row in results if row["child_table_recommendations"]],
                "recommended_fix": "Split high-cardinality arrays such as items, documents, transactions, attachments, and events into child Silver tables where useful.",
            },
        },
        {
            "name": "analytics_readiness_score",
            "status": "failed" if summary["blocking_failures"] or summary["average_readiness_score"] < 70 else "warning" if summary["average_readiness_score"] < 85 else "ok",
            "message": f"Average Silver readiness score is {summary['average_readiness_score']}.",
            "details": {
                "average_readiness_score": summary["average_readiness_score"],
                "average_flattening_score": summary["average_flattening_score"],
                "average_raw_json_dependency_score": summary["average_raw_json_dependency_score"],
                "tables_passed": summary["tables_passed"],
                "tables_warning": summary["tables_warning"],
                "tables_failed": summary["tables_failed"],
                "blocking_failures": summary["blocking_failures"],
            },
        },
    ]
    label = "Passed" if status == "passed" else "Warning" if status == "warning" else "Failed"
    return {
        "status": status,
        "message": f"{label}: Silver transformation quality validation completed for {len(results)} table(s).",
        "summary": summary,
        "tables": normalize_json(results),
        "checks": checks,
        "validated_at": utc_now().isoformat(),
    }


def main() -> None:
    logger = setup_logging("validate_silver_transformation_quality")
    result = run_silver_transformation_quality_validation(persist=True)
    logger.info(result["message"])
    for table in result["tables"]:
        logger.info(
            "%s status=%s readiness=%s flattening=%s nested=%s recommendations=%s",
            table["silver_table_name"],
            table["status"],
            table["analytics_readiness_score"],
            table["flattening_score"],
            table["nested_columns_remaining"],
            len(table["recommendations"]),
        )
    print(json.dumps(normalize_json(result), indent=2, sort_keys=True))
    if result["status"] == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
