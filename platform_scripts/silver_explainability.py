from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import DictCursor, Json

from common import load_environment, safe_identifier, setup_logging
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db, new_id
from query_layer import is_pii_column, safe_view_name_for_table, table_columns


load_environment()


PII_TYPE_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("email", ("email",)),
    ("phone", ("phone", "mobile")),
    ("emirates_id", ("emirates_id", "emiratesid")),
    ("passport", ("passport",)),
    ("credential", ("password", "secret", "token", "credential")),
    ("account", ("iban", "accountnumber", "account_number", "accountname", "account_name")),
    ("username", ("username", "user_name", "login")),
    ("date_of_birth", ("dateofbirth", "date_of_birth", "birthdate", "birth_date", "dob")),
    ("name", ("fullname", "full_name", "firstname", "first_name", "lastname", "last_name")),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_json(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return value


def column_name_from_path(field_path: str, *, suffix: str | None = None) -> str:
    parts = [part for part in re.split(r"[.\[\]]+", str(field_path)) if part]
    base = "_".join(parts) or str(field_path)
    if suffix:
        base = f"{base}_{suffix}"
    return safe_identifier(base, max_length=110)


def pii_type_for(name: str) -> str:
    lowered = str(name or "").lower().replace(".", "_")
    for pii_type, markers in PII_TYPE_RULES:
        if any(marker in lowered for marker in markers):
            return pii_type
    if is_pii_column(lowered):
        return "personal_identifier"
    return "unknown"


def pii_rule_for(name: str) -> str:
    detected = pii_type_for(name)
    return f"column_name_contains_{detected}" if detected != "unknown" else "silver_profile_pii_flag"


def pii_confidence_for(name: str) -> float:
    lowered = str(name or "").lower().replace(".", "_")
    detected = pii_type_for(lowered)
    if detected == "unknown":
        return 0.7
    if lowered in {"email", "email_address", "phone", "phone_number", "password", "token", "emirates_id", "passport"}:
        return 0.98
    if lowered.endswith(("_email", "_phone", "_name")):
        return 0.9
    return 0.85


def raw_json_reason(value: str | None) -> str:
    lowered = str(value or "").lower()
    if any(marker in lowered for marker in ("depth", "deep", "nested")):
        return "too deeply nested"
    if any(marker in lowered for marker in ("map", "dynamic", "config")):
        return "map/config-like object"
    if any(marker in lowered for marker in ("limit", "max", "cap", "column")):
        return "exceeded max columns"
    if any(marker in lowered for marker in ("low", "rare", "frequency")):
        return "low frequency"
    if any(marker in lowered for marker in ("array", "explode", "unsafe")):
        return "unsafe to explode"
    return "rare/dynamic field"


def child_reason(value: str | None) -> str:
    lowered = str(value or "").lower()
    if any(marker in lowered for marker in ("cardinality", "many")):
        return "high-cardinality nested data"
    if any(marker in lowered for marker in ("repeated", "struct")):
        return "repeated struct"
    if any(marker in lowered for marker in ("explosion", "columns")):
        return "column explosion prevention"
    return "array of objects"


def bi_impact_for_raw_json(reason: str) -> str:
    if reason in {"too deeply nested", "exceeded max columns", "unsafe to explode"}:
        return "custom_transform_recommended"
    if reason == "map/config-like object":
        return "limited_filtering"
    return "minor"


def safe_view_metadata() -> dict[str, dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM query_safe_views")
            rows = [as_dict(row) for row in cursor.fetchall()]
    metadata = {row["view_name"]: row for row in rows if row.get("view_name")}
    for view_name, row in metadata.items():
        if row.get("status") != "ok":
            row["columns"] = []
            continue
        try:
            row["columns"] = table_columns(view_name)
        except Exception:
            row["columns"] = []
    return metadata


def safe_view_status_for_pii(view_row: dict[str, Any] | None, column_name: str) -> tuple[str, str, str]:
    if not view_row:
        return (
            "missing_safe_view",
            "retained_in_governed_silver",
            "Create or refresh the safe analytics view so raw PII is not queryable.",
        )
    if view_row.get("status") != "ok":
        return (
            "missing_safe_view",
            "retained_in_governed_silver",
            view_row.get("error_message") or "Refresh safe view metadata and regenerate the failed safe view.",
        )
    columns = [str(column).lower() for column in view_row.get("columns") or []]
    raw_column = column_name.lower()
    if raw_column in columns:
        return (
            "exposed",
            "retained_in_governed_silver",
            "Block this release until the safe view hashes or excludes the raw PII column.",
        )
    if f"{raw_column}_hash" in columns or any(column.endswith("_hash") and raw_column in column for column in columns):
        return (
            "protected",
            "hashed",
            "Keep raw PII governed in Silver and expose only hashed values through the safe view.",
        )
    if bool(view_row.get("pii_safe")):
        return (
            "protected",
            "excluded_from_safe_view",
            "No action required unless analysts need a hashed join key.",
        )
    return (
        "exposed",
        "retained_in_governed_silver",
        "Review safe view columns; metadata says raw PII may be visible.",
    )


def silver_table_sources() -> dict[str, dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT silver_table_name, source_database, source_collection,
                       primary_key_column, row_count, is_child_table,
                       parent_silver_table_name, child_path, bi_suitability
                FROM silver_collection_states
                """
            )
            return {row["silver_table_name"]: as_dict(row) for row in cursor.fetchall()}


def plan_rows() -> list[dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM silver_transform_plans ORDER BY last_planned_at DESC")
            return [as_dict(row) for row in cursor.fetchall()]


def profile_rows() -> list[dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM silver_field_profiles
                WHERE pii_detected = true OR raw_json_fallback = true
                ORDER BY table_name, field_path
                """
            )
            return [as_dict(row) for row in cursor.fetchall()]


def insert_pii_audits(cursor, profiles: list[dict[str, Any]], plans: list[dict[str, Any]], table_sources: dict[str, dict[str, Any]], view_metadata: dict[str, dict[str, Any]]) -> int:
    records: dict[tuple[str, str, str], dict[str, Any]] = {}
    for profile in profiles:
        if not profile.get("pii_detected"):
            continue
        table_name = profile["table_name"]
        field_path = profile["field_path"]
        column_name = column_name_from_path(field_path)
        source = table_sources.get(table_name, {})
        view_name = safe_view_name_for_table(table_name)
        safe_status, action_taken, recommendation = safe_view_status_for_pii(view_metadata.get(view_name), column_name)
        severity = "blocking" if safe_status == "exposed" else "warning"
        pii_type = pii_type_for(field_path)
        records[(table_name, column_name, pii_type)] = {
            "database_name": profile.get("source_database") or source.get("source_database"),
            "collection_name": profile.get("source_collection") or source.get("source_collection"),
            "silver_table_name": table_name,
            "column_name": column_name,
            "field_path": field_path,
            "detected_pii_type": pii_type,
            "detection_rule": pii_rule_for(field_path),
            "confidence": pii_confidence_for(field_path),
            "action_taken": action_taken,
            "safe_view_name": view_name,
            "safe_view_status": safe_status,
            "severity": severity,
            "recommendation": recommendation,
            "details": {
                "source": "silver_field_profiles",
                "detected_type": profile.get("detected_type"),
                "occurrence_percent": profile.get("occurrence_percent"),
                "flattening_strategy": profile.get("flattening_strategy"),
            },
        }

    for plan in plans:
        source_db = plan.get("source_database")
        source_collection = plan.get("source_collection")
        generated_tables = normalize_json(plan.get("generated_silver_tables_json"), []) or []
        table_name = generated_tables[0] if generated_tables else safe_identifier(f"{source_db}__{source_collection}_clean", max_length=120)
        for field_path in normalize_json(plan.get("pii_fields_json"), []) or []:
            column_name = column_name_from_path(field_path)
            pii_type = pii_type_for(field_path)
            key = (table_name, column_name, pii_type)
            if key in records:
                continue
            view_name = safe_view_name_for_table(table_name)
            safe_status, action_taken, recommendation = safe_view_status_for_pii(view_metadata.get(view_name), column_name)
            records[key] = {
                "database_name": source_db,
                "collection_name": source_collection,
                "silver_table_name": table_name,
                "column_name": column_name,
                "field_path": field_path,
                "detected_pii_type": pii_type,
                "detection_rule": pii_rule_for(field_path),
                "confidence": pii_confidence_for(field_path),
                "action_taken": action_taken,
                "safe_view_name": view_name,
                "safe_view_status": safe_status,
                "severity": "blocking" if safe_status == "exposed" else "warning",
                "recommendation": recommendation,
                "details": {"source": "silver_transform_plans"},
            }

    for record in records.values():
        cursor.execute(
            """
            INSERT INTO silver_pii_audit (
                id, database_name, collection_name, silver_table_name, column_name,
                field_path, detected_pii_type, detection_rule, confidence,
                action_taken, safe_view_name, safe_view_status, severity,
                recommendation, checked_at, details_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (silver_table_name, column_name, detected_pii_type)
            DO UPDATE SET
                database_name = EXCLUDED.database_name,
                collection_name = EXCLUDED.collection_name,
                field_path = EXCLUDED.field_path,
                detection_rule = EXCLUDED.detection_rule,
                confidence = EXCLUDED.confidence,
                action_taken = EXCLUDED.action_taken,
                safe_view_name = EXCLUDED.safe_view_name,
                safe_view_status = EXCLUDED.safe_view_status,
                severity = EXCLUDED.severity,
                recommendation = EXCLUDED.recommendation,
                checked_at = now(),
                details_json = EXCLUDED.details_json
            """,
            (
                new_id(),
                record["database_name"],
                record["collection_name"],
                record["silver_table_name"],
                record["column_name"],
                record["field_path"],
                record["detected_pii_type"],
                record["detection_rule"],
                record["confidence"],
                record["action_taken"],
                record["safe_view_name"],
                record["safe_view_status"],
                record["severity"],
                record["recommendation"],
                Json(record["details"]),
            ),
        )
    return len(records)


def insert_child_audits(cursor, plans: list[dict[str, Any]], table_sources: dict[str, dict[str, Any]]) -> int:
    records: dict[str, dict[str, Any]] = {}
    for table_name, source in table_sources.items():
        if not source.get("is_child_table"):
            continue
        child_path = source.get("child_path")
        child_table = table_name
        records[child_table] = {
            "database_name": source.get("source_database"),
            "collection_name": source.get("source_collection"),
            "parent_table": source.get("parent_silver_table_name") or "-",
            "child_table": child_table,
            "source_nested_path": child_path,
            "reason": child_reason(child_path),
            "parent_key": source.get("primary_key_column") or "silver_row_id",
            "child_index_field": "child_index",
            "rows_generated": int(source.get("row_count") or 0),
            "relationship_confidence": 0.95 if source.get("parent_silver_table_name") and child_path else 0.75,
            "bi_usefulness": source.get("bi_suitability") or "unknown",
            "details": {"source": "silver_collection_states"},
        }

    for plan in plans:
        source_db = plan.get("source_database")
        source_collection = plan.get("source_collection")
        generated_tables = normalize_json(plan.get("generated_silver_tables_json"), []) or []
        parent_table = generated_tables[0] if generated_tables else safe_identifier(f"{source_db}__{source_collection}_clean", max_length=120)
        for child in normalize_json(plan.get("generated_child_tables_json"), []) or []:
            child_table = child.get("table_name")
            if not child_table or child_table in records:
                continue
            source_path = child.get("source_path")
            records[child_table] = {
                "database_name": source_db,
                "collection_name": source_collection,
                "parent_table": parent_table,
                "child_table": child_table,
                "source_nested_path": source_path,
                "reason": child_reason(child.get("reason")),
                "parent_key": "silver_row_id",
                "child_index_field": "child_index",
                "rows_generated": 0,
                "relationship_confidence": 0.85,
                "bi_usefulness": plan.get("bi_suitability") or "unknown",
                "details": {"source": "silver_transform_plans", "plan_child": child},
            }

    for record in records.values():
        cursor.execute(
            """
            INSERT INTO silver_child_table_audit (
                id, database_name, collection_name, parent_table, child_table,
                source_nested_path, reason, parent_key, child_index_field,
                rows_generated, relationship_confidence, bi_usefulness,
                checked_at, details_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (child_table)
            DO UPDATE SET
                database_name = EXCLUDED.database_name,
                collection_name = EXCLUDED.collection_name,
                parent_table = EXCLUDED.parent_table,
                source_nested_path = EXCLUDED.source_nested_path,
                reason = EXCLUDED.reason,
                parent_key = EXCLUDED.parent_key,
                child_index_field = EXCLUDED.child_index_field,
                rows_generated = EXCLUDED.rows_generated,
                relationship_confidence = EXCLUDED.relationship_confidence,
                bi_usefulness = EXCLUDED.bi_usefulness,
                checked_at = now(),
                details_json = EXCLUDED.details_json
            """,
            (
                new_id(),
                record["database_name"],
                record["collection_name"],
                record["parent_table"],
                record["child_table"],
                record["source_nested_path"],
                record["reason"],
                record["parent_key"],
                record["child_index_field"],
                record["rows_generated"],
                record["relationship_confidence"],
                record["bi_usefulness"],
                Json(record["details"]),
            ),
        )
    return len(records)


def insert_raw_json_audits(cursor, profiles: list[dict[str, Any]], plans: list[dict[str, Any]], table_sources: dict[str, dict[str, Any]]) -> int:
    records: dict[tuple[str, str], dict[str, Any]] = {}
    for profile in profiles:
        if not profile.get("raw_json_fallback"):
            continue
        table_name = profile["table_name"]
        field_path = profile["field_path"]
        column_name = column_name_from_path(field_path, suffix="raw_json")
        source = table_sources.get(table_name, {})
        reason = raw_json_reason(profile.get("flattening_strategy"))
        records[(table_name, column_name)] = {
            "database_name": profile.get("source_database") or source.get("source_database"),
            "collection_name": profile.get("source_collection") or source.get("source_collection"),
            "silver_table_name": table_name,
            "column_name": column_name,
            "original_field_path": field_path,
            "reason": reason,
            "needs_custom_transform": reason in {"too deeply nested", "exceeded max columns", "unsafe to explode"},
            "bi_suitability_impact": bi_impact_for_raw_json(reason),
            "details": {
                "source": "silver_field_profiles",
                "detected_type": profile.get("detected_type"),
                "occurrence_percent": profile.get("occurrence_percent"),
            },
        }

    for plan in plans:
        source_db = plan.get("source_database")
        source_collection = plan.get("source_collection")
        generated_tables = normalize_json(plan.get("generated_silver_tables_json"), []) or []
        table_name = generated_tables[0] if generated_tables else safe_identifier(f"{source_db}__{source_collection}_clean", max_length=120)
        for item in normalize_json(plan.get("raw_json_fallback_fields_json"), []) or []:
            field_path = item.get("field_path") or item.get("path")
            if not field_path:
                continue
            column_name = column_name_from_path(field_path, suffix="raw_json")
            reason = raw_json_reason(item.get("reason"))
            records[(table_name, column_name)] = {
                "database_name": source_db,
                "collection_name": source_collection,
                "silver_table_name": table_name,
                "column_name": column_name,
                "original_field_path": field_path,
                "reason": reason,
                "needs_custom_transform": reason in {"too deeply nested", "exceeded max columns", "unsafe to explode"},
                "bi_suitability_impact": bi_impact_for_raw_json(reason),
                "details": {"source": "silver_transform_plans", "plan_fallback": item},
            }

    for record in records.values():
        cursor.execute(
            """
            INSERT INTO silver_raw_json_fallback_audit (
                id, database_name, collection_name, silver_table_name, column_name,
                original_field_path, reason, needs_custom_transform,
                bi_suitability_impact, checked_at, details_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (silver_table_name, column_name)
            DO UPDATE SET
                database_name = EXCLUDED.database_name,
                collection_name = EXCLUDED.collection_name,
                original_field_path = EXCLUDED.original_field_path,
                reason = EXCLUDED.reason,
                needs_custom_transform = EXCLUDED.needs_custom_transform,
                bi_suitability_impact = EXCLUDED.bi_suitability_impact,
                checked_at = now(),
                details_json = EXCLUDED.details_json
            """,
            (
                new_id(),
                record["database_name"],
                record["collection_name"],
                record["silver_table_name"],
                record["column_name"],
                record["original_field_path"],
                record["reason"],
                record["needs_custom_transform"],
                record["bi_suitability_impact"],
                Json(record["details"]),
            ),
        )
    return len(records)


def refresh_silver_explainability_audit() -> dict[str, Any]:
    init_dashboard_db()
    profiles = profile_rows()
    plans = plan_rows()
    table_sources = silver_table_sources()
    view_metadata = safe_view_metadata()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM silver_pii_audit")
            cursor.execute("DELETE FROM silver_child_table_audit")
            cursor.execute("DELETE FROM silver_raw_json_fallback_audit")
            pii_count = insert_pii_audits(cursor, profiles, plans, table_sources, view_metadata)
            child_count = insert_child_audits(cursor, plans, table_sources)
            raw_json_count = insert_raw_json_audits(cursor, profiles, plans, table_sources)
    return {
        "status": "ok",
        "message": "Silver explainability audit refreshed",
        "pii_records": pii_count,
        "child_table_records": child_count,
        "raw_json_fallback_records": raw_json_count,
        "checked_at": utc_now().isoformat(),
    }


def fetch_rows(table: str, filters: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    allowed = {
        "silver_pii_audit": {
            "database_name": "database_name",
            "collection_name": "collection_name",
            "table_name": "silver_table_name",
            "pii_type": "detected_pii_type",
            "action_taken": "action_taken",
            "safe_view_status": "safe_view_status",
        },
        "silver_child_table_audit": {
            "database_name": "database_name",
            "collection_name": "collection_name",
            "table_name": "child_table",
        },
        "silver_raw_json_fallback_audit": {
            "database_name": "database_name",
            "collection_name": "collection_name",
            "table_name": "silver_table_name",
            "needs_custom_transform": "needs_custom_transform",
        },
    }[table]
    clauses = []
    params: list[Any] = []
    for public_name, column in allowed.items():
        value = filters.get(public_name)
        if value in (None, ""):
            continue
        clauses.append(f"{column} = %s")
        if public_name == "needs_custom_transform":
            params.append(str(value).lower() in {"1", "true", "yes"})
        else:
            params.append(value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM {table}
                {where}
                ORDER BY checked_at DESC
                LIMIT %s
                """,
                [*params, max(1, min(int(limit), 2000))],
            )
            return [as_dict(row) for row in cursor.fetchall()]


def silver_explainability_audit(
    *,
    database_name: str | None = None,
    collection_name: str | None = None,
    table_name: str | None = None,
    pii_type: str | None = None,
    action_taken: str | None = None,
    safe_view_status: str | None = None,
    needs_custom_transform: str | bool | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    init_dashboard_db()
    filters = {
        "database_name": database_name,
        "collection_name": collection_name,
        "table_name": table_name,
        "pii_type": pii_type,
        "action_taken": action_taken,
        "safe_view_status": safe_view_status,
        "needs_custom_transform": needs_custom_transform,
    }
    pii = fetch_rows("silver_pii_audit", filters, limit)
    child = fetch_rows("silver_child_table_audit", filters, limit)
    raw_json = fetch_rows("silver_raw_json_fallback_audit", filters, limit)
    return {
        "summary": {
            "pii_records": len(pii),
            "blocking_pii_exposures": len([row for row in pii if row.get("severity") == "blocking"]),
            "child_table_records": len(child),
            "raw_json_fallback_records": len(raw_json),
            "needs_custom_transform": len([row for row in raw_json if row.get("needs_custom_transform")]),
        },
        "pii": pii,
        "child_tables": child,
        "raw_json_fallbacks": raw_json,
        "filters": {key: value for key, value in filters.items() if value not in (None, "")},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh or inspect Silver explainability audit metadata.")
    parser.add_argument("command", choices=["refresh", "show"], nargs="?", default="refresh")
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()
    setup_logging()
    payload = refresh_silver_explainability_audit() if args.command == "refresh" else silver_explainability_audit()
    if args.json_output:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(payload.get("message") or json.dumps(payload, default=str))


if __name__ == "__main__":
    main()
