from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
import hashlib
from datetime import datetime, timezone
from typing import Any

import requests
from psycopg2.extras import DictCursor, Json

from common import load_environment, safe_identifier, sql_string_literal, trino_connection
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db, new_id


load_environment()


TRINO_HTTP_URL = os.environ.get("TRINO_HTTP_URL", "http://trino:8080")
TRINO_CONTAINER_NAME = os.environ.get("TRINO_CONTAINER_NAME", "local-data-platform-trino")
TRINO_READONLY_USER = os.environ.get("QUERY_TRINO_USER", os.environ.get("SUPERSET_TRINO_USER", os.environ.get("TRINO_USER", "trino")))
QUERY_SCHEMA = "silver"
SAFE_QUERY_LIMIT = 100
SLOW_QUERY_THRESHOLD_MS = int(os.environ.get("QUERY_SLOW_THRESHOLD_MS", "3000"))
SILVER_TABLE_SUFFIX = "_clean"
SAFE_VIEW_SUFFIX = "_analytics"
SAFE_VIEW_METADATA_STALE_MINUTES = int(os.environ.get("SAFE_VIEW_METADATA_STALE_MINUTES", "1440"))
QUERY_MAX_SQL_LENGTH = int(os.environ.get("QUERY_MAX_SQL_LENGTH", "8000"))
QUERY_MAX_JOIN_COUNT = int(os.environ.get("QUERY_MAX_JOIN_COUNT", "5"))
QUERY_MAX_RELATION_COUNT = int(os.environ.get("QUERY_MAX_RELATION_COUNT", "6"))
QUERY_MAX_EXECUTION_SECONDS = int(os.environ.get("QUERY_MAX_EXECUTION_SECONDS", "30"))
QUERY_MAX_SCAN_BYTES = int(os.environ.get("QUERY_MAX_SCAN_BYTES", str(512 * 1024 * 1024)))
QUERY_PII_HASH_SALT = os.environ.get("QUERY_PII_HASH_SALT", "local-onov8-query-salt-change-me")
QUERY_PII_HASH_VERSION = os.environ.get("QUERY_PII_HASH_VERSION", "v2_salted_sha256")
QUERY_ALLOW_ORPHAN_SAFE_VIEW_DROP = os.environ.get("QUERY_ALLOW_ORPHAN_SAFE_VIEW_DROP", "false").lower() == "true"
# Kept as an empty compatibility export for older imports. Safe views are now
# generated from discovered Silver metadata instead of a static demo registry.
SAFE_VIEW_DEFINITIONS: dict[str, dict[str, str]] = {}
FORBIDDEN_SQL = {
    "ALTER",
    "CALL",
    "CREATE",
    "DELETE",
    "DROP",
    "EXECUTE",
    "GRANT",
    "INSERT",
    "MERGE",
    "REFRESH",
    "RESET",
    "REVOKE",
    "SET",
    "TRUNCATE",
    "UPDATE",
    "USE",
}
PII_BLOCKED_COLUMNS = {
    "email",
    "email_address",
    "customer_email",
    "phone",
    "phone_number",
    "customer_phone",
    "emirates_id",
    "passport",
    "passport_number",
    "full_name",
    "customer_name",
    "name",
    "password",
    "token",
    "secret",
}
PII_ALLOWED_COLUMNS = {
    "source_name",
    "database_name",
    "collection_name",
    "service_name",
    "product_name",
    "role_name",
    "permission_name",
    "team_name",
}
UNSAFE_JSON_COLUMN_MARKERS = (
    "raw_json",
    "json_blob",
    "payload_json",
    "raw_payload",
    "document_json",
    "profile_json",
    "pii_payload",
)
CERTIFIED_SAFE_JSON_COLUMNS = {
    safe_identifier(value)
    for value in os.environ.get("QUERY_SAFE_JSON_ALLOWED_COLUMNS", "").split(",")
    if str(value).strip()
}
QUERY_HASH_HEX_PREFIX_LENGTH = 24


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_epoch_seconds() -> int:
    return int(time.time())


def fingerprint_sql(sql: str) -> str:
    normalized = re.sub(r"\s+", " ", strip_sql_comments(sql)).strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:QUERY_HASH_HEX_PREFIX_LENGTH]


def is_dangerous_json_column(column_name: str) -> bool:
    normalized = safe_identifier(str(column_name))
    if normalized in CERTIFIED_SAFE_JSON_COLUMNS:
        return False
    if normalized.endswith("_json") and normalized not in CERTIFIED_SAFE_JSON_COLUMNS:
        return True
    return any(marker in normalized for marker in UNSAFE_JSON_COLUMN_MARKERS)


def pii_hash_sql_expression(quoted_column: str) -> str:
    # Deterministic salted hash so analytics joins remain stable while avoiding
    # plain unsalted hashes that are easier to precompute.
    return (
        "lower(to_hex(sha256(to_utf8("
        f"concat({sql_string_literal(QUERY_PII_HASH_SALT)}, '|', CAST({quoted_column} AS varchar))"
        "))))"
    )


def trino_data_size_literal(num_bytes: int) -> str:
    if num_bytes >= 1024 * 1024 * 1024:
        return f"{max(1, round(num_bytes / (1024 * 1024 * 1024)))}GB"
    if num_bytes >= 1024 * 1024:
        return f"{max(1, round(num_bytes / (1024 * 1024)))}MB"
    if num_bytes >= 1024:
        return f"{max(1, round(num_bytes / 1024))}KB"
    return f"{max(1, num_bytes)}B"


def trino_info() -> dict[str, Any]:
    response = requests.get(f"{TRINO_HTTP_URL}/v1/info", timeout=5)
    response.raise_for_status()
    return response.json()


def trino_status() -> dict[str, Any]:
    try:
        info = trino_info()
        node_version = info.get("nodeVersion")
        display_version = node_version.get("version") if isinstance(node_version, dict) else node_version
        return {
            "status": "ok",
            "message": f"Trino {display_version or 'unknown'} reachable",
            "node_version": node_version,
            "starting": info.get("starting", False),
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc), "node_version": None, "starting": None}


def trino_container_running() -> dict[str, Any]:
    docker = shutil.which("docker")
    if docker:
        try:
            result = subprocess.run(
                [docker, "inspect", "-f", "{{.State.Running}}", TRINO_CONTAINER_NAME],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                running = result.stdout.strip().lower() == "true"
                return {
                    "status": "ok" if running else "error",
                    "running": running,
                    "message": f"{TRINO_CONTAINER_NAME} running={running}",
                }
        except Exception as exc:
            return {"status": "unknown", "running": None, "message": str(exc)}
    status = trino_status()
    return {
        "status": status["status"],
        "running": status["status"] == "ok",
        "message": "Docker CLI unavailable; using Trino HTTP health as the container signal",
    }


def stats_value(stats: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in stats and stats[key] is not None:
            return stats[key]
    return None


def bytes_from_stats(stats: dict[str, Any]) -> int | None:
    value = stats_value(stats, "processedBytes", "physicalInputBytes", "rawInputDataSize", "processed_bytes")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def query_id_from_stats(stats: dict[str, Any]) -> str:
    value = stats_value(stats, "queryId", "query_id", "id")
    return str(value) if value else f"dashboard_{new_id()}"


def record_query_history(
    *,
    query_id: str,
    user_source: str,
    sql_text: str,
    selected_view: str | None,
    started_at: datetime,
    finished_at: datetime,
    duration_ms: int,
    row_count: int,
    status: str,
    error_message: str | None = None,
    bytes_scanned: int | None = None,
    actor: str | None = None,
    actor_role: str | None = None,
    session_id: str | None = None,
    request_id: str | None = None,
    client_ip: str | None = None,
    query_fingerprint: str | None = None,
    selected_view_status: str | None = None,
) -> dict[str, Any]:
    init_dashboard_db()
    is_slow = duration_ms >= SLOW_QUERY_THRESHOLD_MS
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO query_history (
                    query_id, user_source, sql_text, selected_view, started_at,
                    finished_at, duration_ms, row_count, status, error_message,
                    bytes_scanned, is_slow, actor, actor_role, session_id,
                    request_id, client_ip, query_fingerprint, selected_view_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (query_id)
                DO UPDATE SET
                    user_source = EXCLUDED.user_source,
                    sql_text = EXCLUDED.sql_text,
                    selected_view = EXCLUDED.selected_view,
                    started_at = EXCLUDED.started_at,
                    finished_at = EXCLUDED.finished_at,
                    duration_ms = EXCLUDED.duration_ms,
                    row_count = EXCLUDED.row_count,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    bytes_scanned = EXCLUDED.bytes_scanned,
                    is_slow = EXCLUDED.is_slow,
                    actor = EXCLUDED.actor,
                    actor_role = EXCLUDED.actor_role,
                    session_id = EXCLUDED.session_id,
                    request_id = EXCLUDED.request_id,
                    client_ip = EXCLUDED.client_ip,
                    query_fingerprint = EXCLUDED.query_fingerprint,
                    selected_view_status = EXCLUDED.selected_view_status
                RETURNING *
                """,
                (
                    query_id,
                    user_source,
                    sql_text,
                    selected_view,
                    started_at,
                    finished_at,
                    duration_ms,
                    row_count,
                    status,
                    error_message,
                    bytes_scanned,
                    is_slow,
                    actor,
                    actor_role,
                    session_id,
                    request_id,
                    client_ip,
                    query_fingerprint,
                    selected_view_status,
                ),
            )
            return as_dict(cursor.fetchone())


def trino_query(
    sql: str,
    schema: str = QUERY_SCHEMA,
    *,
    observe: bool = False,
    user_source: str = "dashboard",
    selected_view: str | None = None,
    trino_user: str | None = None,
    session_properties: dict[str, str] | None = None,
    query_context: dict[str, Any] | None = None,
) -> tuple[list[str], list[list[Any]], dict[str, Any]]:
    connection = trino_connection(schema=schema, user=trino_user, session_properties=session_properties)
    cursor = connection.cursor()
    started_at = utc_now()
    started_timer = time.monotonic()
    stats: dict[str, Any] = {}
    context = query_context or {}
    query_fingerprint = fingerprint_sql(sql)
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in (cursor.description or [])]
        rows = [list(row) for row in cursor.fetchall()]
        stats = dict(getattr(cursor, "stats", {}) or {})
        if observe:
            finished_at = utc_now()
            duration_ms = int((time.monotonic() - started_timer) * 1000)
            record_query_history(
                query_id=query_id_from_stats(stats),
                user_source=user_source,
                sql_text=sql,
                selected_view=selected_view,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
                row_count=len(rows),
                status="ok",
                bytes_scanned=bytes_from_stats(stats),
                actor=context.get("actor"),
                actor_role=context.get("role"),
                session_id=context.get("session_id"),
                request_id=context.get("request_id"),
                client_ip=context.get("client_ip"),
                query_fingerprint=query_fingerprint,
                selected_view_status=context.get("selected_view_status"),
            )
        return columns, rows, stats
    except Exception as exc:
        message = str(exc)
        # Lightweight compatibility fallback when a Trino cluster does not
        # expose one of the configured session properties.
        if session_properties and "session property" in message.lower():
            cursor.close()
            connection.close()
            return trino_query(
                sql,
                schema=schema,
                observe=observe,
                user_source=user_source,
                selected_view=selected_view,
                trino_user=trino_user,
                session_properties=None,
                query_context=query_context,
            )
        stats = dict(getattr(cursor, "stats", {}) or {})
        if observe:
            finished_at = utc_now()
            duration_ms = int((time.monotonic() - started_timer) * 1000)
            record_query_history(
                query_id=query_id_from_stats(stats),
                user_source=user_source,
                sql_text=sql,
                selected_view=selected_view,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
                row_count=0,
                status="failed",
                error_message=str(exc),
                bytes_scanned=bytes_from_stats(stats),
                actor=context.get("actor"),
                actor_role=context.get("role"),
                session_id=context.get("session_id"),
                request_id=context.get("request_id"),
                client_ip=context.get("client_ip"),
                query_fingerprint=query_fingerprint,
                selected_view_status=context.get("selected_view_status"),
            )
        raise
    finally:
        cursor.close()
        connection.close()


def list_catalogs() -> list[str]:
    _, rows, _ = trino_query("SHOW CATALOGS")
    return [str(row[0]) for row in rows]


def list_silver_tables() -> list[str]:
    _, rows, _ = trino_query("SHOW TABLES FROM delta.silver")
    return sorted(str(row[0]) for row in rows)


def list_silver_views() -> list[str]:
    try:
        _, rows, _ = trino_query("SHOW VIEWS FROM delta.silver")
        return sorted(str(row[0]) for row in rows)
    except Exception:
        return sorted(table for table in list_silver_tables() if table.endswith("_analytics"))


def quote_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def is_silver_source_table(table_name: str) -> bool:
    return table_name.endswith(SILVER_TABLE_SUFFIX) and not table_name.endswith(SAFE_VIEW_SUFFIX)


def safe_view_name_for_table(table_name: str) -> str:
    if table_name.endswith(SILVER_TABLE_SUFFIX):
        base_name = table_name[: -len(SILVER_TABLE_SUFFIX)]
    else:
        base_name = table_name
    return safe_identifier(f"{base_name}{SAFE_VIEW_SUFFIX}", max_length=120)


def source_table_for_safe_view(view_name: str) -> str:
    if view_name.endswith(SAFE_VIEW_SUFFIX):
        base_name = view_name[: -len(SAFE_VIEW_SUFFIX)]
        return safe_identifier(f"{base_name}{SILVER_TABLE_SUFFIX}", max_length=120)
    return safe_identifier(f"{view_name}{SILVER_TABLE_SUFFIX}", max_length=120)


def source_scope_from_table(table_name: str | None) -> dict[str, str | None]:
    base_name = str(table_name or "").removesuffix(SILVER_TABLE_SUFFIX).removesuffix(SAFE_VIEW_SUFFIX)
    if "__" in base_name:
        database, collection = base_name.split("__", 1)
        return {"database_name": database or None, "collection_name": collection or None}
    return {"database_name": None, "collection_name": base_name or None}


def table_columns(table_name: str) -> list[str]:
    _, rows, _ = trino_query(f'SHOW COLUMNS FROM delta.silver."{table_name}"')
    return [str(row[0]) for row in rows]


def table_row_count(table_name: str) -> int:
    _, rows, _ = trino_query(f'SELECT count(*) FROM delta.silver."{table_name}"')
    return int(rows[0][0])


def dashboard_silver_table_locations() -> list[dict[str, str]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT COALESCE(trino_table_name, silver_table_name) AS table_name,
                       silver_table_path
                FROM silver_collection_states
                WHERE silver_table_path IS NOT NULL
                ORDER BY silver_table_name
                """
            )
            return [as_dict(row) for row in cursor.fetchall()]


def registered_silver_table_names() -> list[str]:
    return sorted(
        {
            row["table_name"]
            for row in dashboard_silver_table_locations()
            if row.get("table_name") and is_silver_source_table(row["table_name"])
        }
    )


def visible_silver_source_tables() -> list[str]:
    views = set(list_silver_views())
    return sorted(table for table in list_silver_tables() if table not in views and is_silver_source_table(table))


def register_existing_silver_tables() -> list[dict[str, Any]]:
    table_locations = dashboard_silver_table_locations()
    if not table_locations:
        return []
    connection = trino_connection(schema=QUERY_SCHEMA)
    cursor = connection.cursor()
    results: list[dict[str, Any]] = []
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS delta.silver")
        try:
            cursor.execute("SHOW TABLES FROM delta.silver")
            existing = {str(row[0]) for row in cursor.fetchall()}
        except Exception:
            existing = set()
        for row in table_locations:
            table_name = row["table_name"]
            path = row["silver_table_path"]
            if table_name in existing:
                results.append({"table_name": table_name, "status": "already_registered", "table_location": path})
                continue
            registered = False
            for candidate in [path, path.replace("s3a://", "s3://", 1)]:
                try:
                    cursor.execute(
                        f"""
                        CALL delta.system.register_table(
                            schema_name => 'silver',
                            table_name => {sql_string_literal(table_name)},
                            table_location => {sql_string_literal(candidate)}
                        )
                        """
                    )
                    existing.add(table_name)
                    results.append({"table_name": table_name, "status": "registered", "table_location": candidate})
                    registered = True
                    break
                except Exception as exc:
                    message = str(exc)
                    if "already exists" in message.lower() or "already registered" in message.lower():
                        existing.add(table_name)
                        results.append({"table_name": table_name, "status": "already_registered", "table_location": candidate})
                        registered = True
                        break
                    last_error = message
            if not registered:
                results.append({"table_name": table_name, "status": "error", "table_location": path, "error_message": last_error})
        return results
    finally:
        cursor.close()
        connection.close()


def is_pii_column(column_name: str) -> bool:
    normalized = column_name.lower()
    if normalized.endswith("_hash"):
        return False
    if normalized in PII_ALLOWED_COLUMNS:
        return False
    if normalized in PII_BLOCKED_COLUMNS:
        return True
    if any(marker in normalized for marker in ("phone", "mobile", "email", "iban", "accountnumber", "accountname", "idnumber", "emirates_id", "passport", "username")):
        return True
    if any(marker in normalized for marker in ("password", "secret", "token", "credential")):
        return True
    if any(marker in normalized for marker in ("firstname", "lastname", "fullname", "dateofbirth")):
        return True
    if normalized.endswith("_email") or normalized == "customer_email":
        return True
    if normalized.endswith("_name") and normalized not in PII_ALLOWED_COLUMNS:
        return True
    if normalized.endswith(("first_name", "last_name", "full_name")):
        return True
    return False


def unique_output_alias(base_name: str, used_aliases: set[str], reserved_aliases: set[str]) -> str:
    base = safe_identifier(base_name, max_length=110)
    candidate = base
    suffix = 2
    while candidate.lower() in used_aliases or candidate.lower() in reserved_aliases:
        candidate = safe_identifier(f"{base}_{suffix}", max_length=110)
        suffix += 1
    used_aliases.add(candidate.lower())
    return candidate


def dynamic_safe_view_sql(source_table: str, columns: list[str]) -> tuple[str, dict[str, Any]]:
    non_pii_aliases = {column.lower() for column in columns if not is_pii_column(column)}
    used_aliases: set[str] = set()
    select_expressions: list[str] = []
    hashed_columns: list[str] = []
    blocked_columns: list[str] = []
    passthrough_columns: list[str] = []
    for column in columns:
        quoted = quote_identifier(column)
        if is_dangerous_json_column(column):
            blocked_columns.append(column)
            continue
        if is_pii_column(column):
            alias = unique_output_alias(f"{column}_hash", used_aliases, non_pii_aliases)
            select_expressions.append(
                f"""
                CASE
                    WHEN {quoted} IS NULL OR trim(CAST({quoted} AS varchar)) = '' THEN NULL
                    ELSE {pii_hash_sql_expression(quoted)}
                END AS {quote_identifier(alias)}
                """.strip()
            )
            hashed_columns.append(column)
            continue
        used_aliases.add(column.lower())
        passthrough_columns.append(column)
        select_expressions.append(f"{quoted}")

    if not select_expressions:
        select_expressions.append('CAST(NULL AS varchar) AS "safe_view_placeholder"')

    select_sql = ",\n                ".join(select_expressions)
    sql = f"""
            SELECT
                {select_sql}
            FROM delta.silver.{quote_identifier(source_table)}
        """
    return sql, {
        "blocked_columns": blocked_columns,
        "hashed_columns": hashed_columns,
        "passthrough_columns": passthrough_columns,
    }


def dynamic_safe_view_definition(source_table: str) -> dict[str, Any]:
    columns = table_columns(source_table)
    sql, transformation = dynamic_safe_view_sql(source_table, columns)
    return {
        "view_name": safe_view_name_for_table(source_table),
        "source_table": source_table,
        "sql": sql,
        "columns": columns,
        "blocked_columns": transformation["blocked_columns"],
        "hashed_columns": transformation["hashed_columns"],
        "passthrough_columns": transformation["passthrough_columns"],
        "hash_version": QUERY_PII_HASH_VERSION,
        "dynamic": True,
    }


def expected_safe_view_names_for_tables(table_names: set[str] | list[str]) -> set[str]:
    return {safe_view_name_for_table(table) for table in table_names if is_silver_source_table(table)}


def delete_stale_safe_view_metadata(expected_view_names: set[str]) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            if not expected_view_names:
                cursor.execute("DELETE FROM query_safe_views")
                return
            cursor.execute(
                "DELETE FROM query_safe_views WHERE NOT (view_name = ANY(%s))",
                (list(expected_view_names),),
            )


def delete_safe_view_metadata_rows(view_names: list[str]) -> None:
    if not view_names:
        return
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM query_safe_views WHERE view_name = ANY(%s)", (view_names,))


def metadata_is_stale(row: dict[str, Any]) -> bool:
    if not row:
        return True
    validated = row.get("last_validated_at")
    if not isinstance(validated, datetime):
        return True
    age_seconds = max(0, now_epoch_seconds() - int(validated.timestamp()))
    return age_seconds > SAFE_VIEW_METADATA_STALE_MINUTES * 60


def create_or_replace_safe_view(view_name: str, sql: str) -> None:
    try:
        trino_query(f'CREATE OR REPLACE VIEW delta.silver.{quote_identifier(view_name)} AS {sql}')
    except Exception:
        trino_query(f'DROP VIEW IF EXISTS delta.silver.{quote_identifier(view_name)}')
        trino_query(f'CREATE VIEW delta.silver.{quote_identifier(view_name)} AS {sql}')


def safe_view_reconciliation() -> dict[str, Any]:
    init_dashboard_db()
    register_existing_silver_tables()
    visible_tables = set(visible_silver_source_tables())
    registered_tables = set(registered_silver_table_names())
    expected_sources = set(sorted(visible_tables | registered_tables))
    expected_views = expected_safe_view_names_for_tables(expected_sources)
    visible_views = set(list_silver_views())
    analytics_views = {view for view in visible_views if view.endswith(SAFE_VIEW_SUFFIX)}
    metadata_rows = safe_views_metadata()
    metadata_views = {row.get("view_name") for row in metadata_rows if row.get("view_name")}
    metadata_map = metadata_by_view(metadata_rows)
    missing_sources = sorted(registered_tables - visible_tables)
    missing_views = sorted(expected_views - visible_views)
    orphan_metadata = sorted(metadata_views - expected_views)
    orphan_trino_views = sorted(analytics_views - expected_views)
    stale_metadata = sorted(
        view_name
        for view_name, row in metadata_map.items()
        if view_name in expected_views
        and (
            metadata_is_stale(row)
            or row.get("status") not in {"ok"}
            or row.get("source_table") not in visible_tables
            or view_name not in visible_views
        )
    )
    return {
        "visible_source_tables": sorted(visible_tables),
        "registered_source_tables": sorted(registered_tables),
        "expected_source_tables": sorted(expected_sources),
        "expected_safe_views": sorted(expected_views),
        "visible_safe_views": sorted(visible_views),
        "missing_source_tables": missing_sources,
        "missing_safe_views": missing_views,
        "orphan_metadata_views": orphan_metadata,
        "orphan_trino_views": orphan_trino_views,
        "stale_safe_views": stale_metadata,
        "metadata_count": len(metadata_views),
        "expected_count": len(expected_views),
    }


def available_safe_view_definitions() -> dict[str, dict[str, str]]:
    try:
        visible_tables = visible_silver_source_tables()
    except Exception:
        return {}
    return {
        safe_view_name_for_table(source_table): {"source_table": source_table}
        for source_table in visible_tables
    }


def eligible_safe_view_definitions(require_live_check: bool = True) -> dict[str, dict[str, Any]]:
    try:
        metadata_rows = safe_views_metadata()
        visible_views = set(list_silver_views())
        visible_sources = set(visible_silver_source_tables())
    except Exception:
        return {}
    allowed: dict[str, dict[str, Any]] = {}
    for row in metadata_rows:
        view_name = row.get("view_name")
        source_table = row.get("source_table")
        if not view_name or not source_table:
            continue
        if row.get("status") != "ok":
            continue
        if not bool(row.get("pii_safe")):
            continue
        if view_name not in visible_views:
            continue
        if source_table not in visible_sources:
            continue
        if require_live_check:
            try:
                trino_query(f'SELECT 1 FROM delta.silver.{quote_identifier(view_name)} LIMIT 1')
            except Exception:
                continue
        allowed[view_name] = {"source_table": source_table, "status": "ok", "pii_safe": True, "trino_visible": True}
    return allowed


def known_safe_view_names() -> set[str]:
    try:
        names = set(available_safe_view_definitions())
    except Exception:
        names = set()
    try:
        names.update(row["view_name"] for row in safe_views_metadata())
    except Exception:
        pass
    return names


def pii_safe_columns(columns: list[str]) -> tuple[bool, list[str]]:
    blocked = [column for column in columns if is_pii_column(column) or is_dangerous_json_column(column)]
    return not blocked, blocked


def upsert_safe_view_metadata(
    *,
    view_name: str,
    source_table: str,
    column_count: int = 0,
    row_count: int = 0,
    pii_safe: bool = False,
    source_column_count: int = 0,
    blocked_columns: list[str] | None = None,
    hash_version: str | None = None,
    trino_visible: bool = False,
    status: str,
    error_message: str | None = None,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO query_safe_views (
                    view_name, source_table, column_count, row_count, pii_safe,
                    last_validated_at, status, error_message, updated_at,
                    source_column_count, blocked_columns_json, hash_version, trino_visible
                )
                VALUES (%s, %s, %s, %s, %s, now(), %s, %s, now(), %s, %s, %s, %s)
                ON CONFLICT (view_name)
                DO UPDATE SET
                    source_table = EXCLUDED.source_table,
                    column_count = EXCLUDED.column_count,
                    row_count = EXCLUDED.row_count,
                    pii_safe = EXCLUDED.pii_safe,
                    last_validated_at = EXCLUDED.last_validated_at,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    source_column_count = EXCLUDED.source_column_count,
                    blocked_columns_json = EXCLUDED.blocked_columns_json,
                    hash_version = EXCLUDED.hash_version,
                    trino_visible = EXCLUDED.trino_visible,
                    updated_at = now()
                """,
                (
                    view_name,
                    source_table,
                    column_count,
                    row_count,
                    pii_safe,
                    status,
                    error_message,
                    source_column_count,
                    Json(blocked_columns or []),
                    hash_version,
                    trino_visible,
                ),
            )


def refresh_safe_view_metadata(
    view_name: str,
    source_table: str,
    *,
    source_column_count: int = 0,
    blocked_columns: list[str] | None = None,
    hash_version: str | None = None,
) -> dict[str, Any]:
    columns = table_columns(view_name)
    safe, blocked = pii_safe_columns(columns)
    row_count = table_row_count(view_name)
    trino_visible = True
    status = "ok" if safe else "error"
    messages: list[str] = []
    if not safe:
        messages.append(f"Blocked PII columns visible: {', '.join(blocked)}")
    if blocked_columns:
        messages.append(f"Excluded dangerous JSON fallback columns: {', '.join(blocked_columns)}")
    error_message = "; ".join(messages) if messages else None
    upsert_safe_view_metadata(
        view_name=view_name,
        source_table=source_table,
        column_count=len(columns),
        row_count=row_count,
        pii_safe=safe,
        source_column_count=source_column_count,
        blocked_columns=blocked_columns,
        hash_version=hash_version or QUERY_PII_HASH_VERSION,
        trino_visible=trino_visible,
        status=status,
        error_message=error_message,
    )
    return {
        "view_name": view_name,
        "source_table": source_table,
        "columns": columns,
        "column_count": len(columns),
        "source_column_count": source_column_count,
        "row_count": row_count,
        "pii_safe": safe,
        "trino_visible": trino_visible,
        "hash_version": hash_version or QUERY_PII_HASH_VERSION,
        "blocked_columns": blocked_columns or [],
        "status": status,
        "error_message": error_message,
    }


def ensure_safe_views(force: bool = False) -> list[dict[str, Any]]:
    init_dashboard_db()
    try:
        trino_query("CREATE SCHEMA IF NOT EXISTS delta.silver")
        reconciliation = safe_view_reconciliation()
        visible_tables = set(reconciliation["visible_source_tables"])
        registered_tables = set(reconciliation["registered_source_tables"])
        visible_views = set(reconciliation["visible_safe_views"])
        expected_view_names = set(reconciliation["expected_safe_views"])
    except Exception:
        return []
    delete_safe_view_metadata_rows(reconciliation["orphan_metadata_views"])
    if force and QUERY_ALLOW_ORPHAN_SAFE_VIEW_DROP:
        for view_name in reconciliation["orphan_trino_views"]:
            try:
                trino_query(f'DROP VIEW IF EXISTS delta.silver.{quote_identifier(view_name)}')
            except Exception:
                pass
    delete_stale_safe_view_metadata(expected_view_names)
    metadata_by_view = {row["view_name"]: row for row in safe_views_metadata()}

    results: list[dict[str, Any]] = []
    for source_table in sorted(registered_tables - visible_tables):
        view_name = safe_view_name_for_table(source_table)
        message = f"Registered Silver table is not visible in Trino: {source_table}"
        upsert_safe_view_metadata(
            view_name=view_name,
            source_table=source_table,
            trino_visible=False,
            status="missing_source",
            error_message=message,
        )
        results.append(
            {
                "view_name": view_name,
                "source_table": source_table,
                "columns": [],
                "column_count": 0,
                "row_count": 0,
                "pii_safe": False,
                "trino_visible": False,
                "status": "missing_source",
                "error_message": message,
            }
        )

    for source_table in sorted(visible_tables):
        view_name = safe_view_name_for_table(source_table)
        try:
            cached = metadata_by_view.get(view_name)
            definition = dynamic_safe_view_definition(source_table)
            should_rebuild = (
                force
                or view_name not in visible_views
                or not cached
                or cached.get("status") != "ok"
                or cached.get("source_table") != source_table
                or metadata_is_stale(cached)
            )
            if should_rebuild:
                create_or_replace_safe_view(view_name, definition["sql"])
            results.append(
                refresh_safe_view_metadata(
                    view_name,
                    source_table,
                    source_column_count=len(definition["columns"]),
                    blocked_columns=definition.get("blocked_columns"),
                    hash_version=definition.get("hash_version"),
                )
            )
        except Exception as exc:
            upsert_safe_view_metadata(
                view_name=view_name,
                source_table=source_table,
                trino_visible=view_name in visible_views,
                status="error",
                error_message=str(exc),
            )
            results.append(
                {
                    "view_name": view_name,
                    "source_table": source_table,
                    "columns": [],
                    "column_count": 0,
                    "row_count": 0,
                    "pii_safe": False,
                    "trino_visible": view_name in visible_views,
                    "status": "error",
                    "error_message": str(exc),
                }
            )
    for orphan_view in sorted(reconciliation["orphan_trino_views"]):
        results.append(
            {
                "view_name": orphan_view,
                "source_table": source_table_for_safe_view(orphan_view),
                "columns": [],
                "column_count": 0,
                "row_count": 0,
                "pii_safe": False,
                "trino_visible": True,
                "status": "orphan_view",
                "error_message": "Safe view exists in Trino but has no matching active Silver source table metadata.",
            }
        )
    return results


def safe_views_metadata() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM query_safe_views
                ORDER BY view_name
                """
            )
            return [as_dict(row) for row in cursor.fetchall()]


def latest_validation_run() -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM query_validation_runs
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            return as_dict(cursor.fetchone())


def failed_validation_run_count() -> int:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM query_validation_runs WHERE status = 'failed'")
            return int(cursor.fetchone()[0])


def failed_query_count() -> int:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM query_history WHERE status IN ('failed', 'blocked')")
            return int(cursor.fetchone()[0])


def failed_queries(limit: int = 100) -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM query_history
                WHERE status IN ('failed', 'blocked')
                ORDER BY started_at DESC
                LIMIT %s
                """,
                (max(1, min(int(limit), 500)),),
            )
            return [as_dict(row) for row in cursor.fetchall()]


def query_history(limit: int = 100) -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM query_history
                ORDER BY started_at DESC
                LIMIT %s
                """,
                (max(1, min(int(limit), 500)),),
            )
            return [as_dict(row) for row in cursor.fetchall()]


def slow_queries(limit: int = 100) -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM query_history
                WHERE is_slow = true
                ORDER BY duration_ms DESC NULLS LAST, started_at DESC
                LIMIT %s
                """,
                (max(1, min(int(limit), 500)),),
            )
            return [as_dict(row) for row in cursor.fetchall()]


def query_observability_summary() -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT
                    count(*) AS total_queries,
                    COALESCE(avg(duration_ms), 0) AS average_duration_ms,
                    count(*) FILTER (WHERE status IN ('failed', 'blocked')) AS failed_queries,
                    count(*) FILTER (WHERE is_slow = true) AS slow_queries
                FROM query_history
                """
            )
            summary = as_dict(cursor.fetchone())
            cursor.execute(
                """
                SELECT selected_view, count(*) AS query_count
                FROM query_history
                WHERE selected_view IS NOT NULL
                GROUP BY selected_view
                ORDER BY query_count DESC, selected_view
                LIMIT 10
                """
            )
            most_queried = [as_dict(row) for row in cursor.fetchall()]
            cursor.execute("SELECT COALESCE(sum(total_rows_written), 0) AS rows FROM bronze_collection_states")
            bronze_rows = int(cursor.fetchone()["rows"] or 0)
            cursor.execute("SELECT COALESCE(sum(row_count), 0) AS rows FROM silver_collection_states")
            silver_rows = int(cursor.fetchone()["rows"] or 0)
            cursor.execute("SELECT COALESCE(sum(row_count), 0) AS rows FROM query_safe_views WHERE status = 'ok'")
            safe_view_rows = int(cursor.fetchone()["rows"] or 0)
    return {
        "total_queries": int(summary.get("total_queries") or 0),
        "average_duration_ms": float(summary.get("average_duration_ms") or 0),
        "failed_queries": int(summary.get("failed_queries") or 0),
        "slow_queries": int(summary.get("slow_queries") or 0),
        "slow_threshold_ms": SLOW_QUERY_THRESHOLD_MS,
        "most_queried_views": most_queried,
        "row_counts_per_layer": {
            "bronze": bronze_rows,
            "silver": silver_rows,
            "safe_views": safe_view_rows,
        },
    }


def record_validation_run(checks: list[dict[str, Any]], started_at: datetime, error_message: str | None = None) -> dict[str, Any]:
    passed = len([check for check in checks if check["status"] == "ok"])
    failed = len([check for check in checks if check["status"] == "failed"])
    warnings = len([check for check in checks if check["status"] == "warning"])
    status = "failed" if failed or error_message else "warning" if warnings else "ok"
    run_id = new_id()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO query_validation_runs (
                    id, status, started_at, finished_at, total_checks,
                    passed_checks, failed_checks, checks_json, error_message
                )
                VALUES (%s, %s, %s, now(), %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (run_id, status, started_at, len(checks), passed, failed, Json(checks), error_message),
            )
            return as_dict(cursor.fetchone())


def check_result(
    name: str,
    ok: bool,
    message: str,
    details: dict[str, Any] | None = None,
    *,
    status: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": status or ("ok" if ok else "failed"),
        "message": message,
        "details": details or {},
    }


def validate_query_layer() -> dict[str, Any]:
    init_dashboard_db()
    started_at = utc_now()
    checks: list[dict[str, Any]] = []
    error_message = None
    try:
        container = trino_container_running()
        checks.append(check_result("trino_container_running", container["running"] is True, container["message"], container))

        status = trino_status()
        checks.append(check_result("trino_api_responds", status["status"] == "ok", status["message"], status))

        catalogs = list_catalogs()
        checks.append(check_result("delta_catalog_connected", "delta" in catalogs, f"Catalogs: {', '.join(catalogs)}", {"catalogs": catalogs}))

        registrations = register_existing_silver_tables()
        registered_tables = set(registered_silver_table_names())
        silver_tables = set(visible_silver_source_tables())
        expected_sources = registered_tables or silver_tables
        missing_silver_tables = sorted(expected_sources - silver_tables)
        extra_visible_tables = sorted(silver_tables - expected_sources) if registered_tables else []
        silver_ok = bool(silver_tables) and not missing_silver_tables
        checks.append(
            check_result(
                "silver_tables_visible",
                silver_ok,
                f"Visible dynamic Silver tables: {', '.join(sorted(silver_tables)) or 'none'}",
                {
                    "affected_component": "Query Layer",
                    "reason": (
                        "No Silver tables are visible in Trino"
                        if not silver_tables
                        else "Registered dynamic Silver tables are missing from Trino"
                        if missing_silver_tables
                        else "Dynamic Silver table discovery succeeded"
                    ),
                    "recommended_fix": "Refresh Trino metadata, register discovered Silver tables, then retry Query validation.",
                    "expected": sorted(expected_sources),
                    "visible": sorted(silver_tables),
                    "missing": missing_silver_tables,
                    "extra_visible": extra_visible_tables,
                    "registrations": registrations,
                },
            )
        )

        view_results = ensure_safe_views(force=False)
        visible_views = set(list_silver_views())
        expected_views = expected_safe_view_names_for_tables(silver_tables)
        reconciliation = safe_view_reconciliation()
        missing_views = sorted(expected_views - visible_views)
        safe_view_status = "ok" if not missing_views else "warning"
        checks.append(
            check_result(
                "safe_views_exist",
                not missing_views,
                f"Visible dynamic safe views: {', '.join(sorted(visible_views & expected_views)) or 'none'}",
                {
                    "affected_component": "Query Layer",
                    "reason": "Missing safe view for one or more dynamic Silver tables" if missing_views else "Safe views exist for visible dynamic Silver tables",
                    "recommended_fix": "Generate missing safe views or refresh query metadata, then retry Query validation.",
                    "expected": sorted(expected_views),
                    "visible": sorted(visible_views),
                    "missing": missing_views,
                },
                status=safe_view_status,
            )
        )
        checks.append(
            check_result(
                "safe_view_metadata_orphans",
                not reconciliation.get("orphan_metadata_views"),
                "No orphan safe-view metadata rows"
                if not reconciliation.get("orphan_metadata_views")
                else f"Orphan metadata rows: {', '.join(reconciliation.get('orphan_metadata_views', []))}",
                reconciliation,
                status="warning" if reconciliation.get("orphan_metadata_views") else "ok",
            )
        )
        checks.append(
            check_result(
                "safe_view_trino_orphans",
                not reconciliation.get("orphan_trino_views"),
                "No orphan Trino safe views"
                if not reconciliation.get("orphan_trino_views")
                else f"Orphan Trino views: {', '.join(reconciliation.get('orphan_trino_views', []))}",
                reconciliation,
                status="warning" if reconciliation.get("orphan_trino_views") else "ok",
            )
        )
        checks.append(
            check_result(
                "safe_view_freshness",
                not reconciliation.get("stale_safe_views"),
                "Safe-view metadata is fresh"
                if not reconciliation.get("stale_safe_views")
                else f"Stale safe views: {', '.join(reconciliation.get('stale_safe_views', []))}",
                reconciliation,
                status="warning" if reconciliation.get("stale_safe_views") else "ok",
            )
        )

        for view in view_results:
            if view.get("status") not in {"ok"}:
                checks.append(
                    check_result(
                        f"{view['view_name']}_available",
                        False,
                        view.get("error_message") or "Safe view is not available",
                        {
                            **view,
                            "affected_component": "Query Layer",
                            "reason": "A dynamic safe view could not be generated for its Silver table.",
                            "recommended_fix": "Generate missing safe views and refresh Trino metadata. This is a Query validation configuration issue, not a Silver processing failure.",
                        },
                        status="warning",
                    )
                )
                continue

            if view.get("trino_visible") is False:
                checks.append(check_result(f"{view['view_name']}_trino_visible", False, "View metadata not visible in Trino", view, status="warning"))
                continue

            is_readable = view["status"] == "ok"
            checks.append(
                check_result(
                    f"{view['view_name']}_readable",
                    is_readable,
                    f"{view['view_name']} rows={view.get('row_count', 0)}",
                    view,
                )
            )
            checks.append(
                check_result(
                    f"{view['view_name']}_pii_blocked",
                    bool(view.get("pii_safe")),
                    view.get("error_message") or "No raw PII columns exposed",
                    {"columns": view.get("columns", [])},
                )
            )
            try:
                source_count = table_row_count(view["source_table"])
                view_count = int(view.get("row_count") or 0)
                checks.append(
                    check_result(
                        f"{view['view_name']}_count_matches_source",
                        view_count == source_count,
                        f"{view_count} view rows; {source_count} source rows",
                        {"view_count": view_count, "source_count": source_count},
                    )
                )
            except Exception as exc:
                checks.append(check_result(f"{view['view_name']}_count_matches_source", False, str(exc)))

    except Exception as exc:
        error_message = str(exc)
        checks.append(check_result("query_layer_validation_exception", False, error_message))

    run = record_validation_run(checks, started_at, error_message)
    return {"status": run["status"], "run": run, "checks": checks, "safe_views": safe_views_metadata()}


def query_overview() -> dict[str, Any]:
    init_dashboard_db()
    status = trino_status()
    try:
        catalogs = list_catalogs()
    except Exception:
        catalogs = []
    try:
        silver_tables = visible_silver_source_tables()
    except Exception:
        silver_tables = []
    try:
        visible_views = set(list_silver_views())
    except Exception:
        visible_views = set()
    expected_views = expected_safe_view_names_for_tables(silver_tables)
    metadata = safe_views_metadata()
    expected_metadata = [view for view in metadata if view.get("view_name") in expected_views]
    safe_count = len(
        [
            view
            for view in expected_metadata
            if view.get("status") == "ok" and view.get("pii_safe") and view.get("view_name") in visible_views
        ]
    )
    pii_safe = bool(expected_metadata) and all(view.get("pii_safe") for view in expected_metadata if view.get("view_name") in visible_views)
    observability = query_observability_summary()
    missing = [view for view in expected_views if view not in visible_views or not metadata_by_view(metadata).get(view)]
    failed_metadata = [view for view in expected_metadata if view.get("status") not in {"ok"}]
    stale_metadata = [view for view in expected_metadata if metadata_is_stale(view)]
    reconciliation: dict[str, Any] = {}
    try:
        reconciliation = safe_view_reconciliation()
    except Exception:
        reconciliation = {}
    return {
        "trino_status": status,
        "connected_catalogs": catalogs,
        "connected_catalog_count": len(catalogs),
        "silver_tables_visible": silver_tables,
        "silver_tables_visible_count": len(silver_tables),
        "safe_views_available": sorted(view for view in visible_views if view in expected_views),
        "safe_views_available_count": safe_count,
        "safe_views_generated_count": safe_count,
        "missing_safe_views_count": len(missing),
        "failed_safe_views_count": len(failed_metadata),
        "stale_safe_views_count": len(stale_metadata),
        "last_query_validation": latest_validation_run(),
        "failed_queries": failed_query_count(),
        "pii_safe_views_status": "ok" if pii_safe else "unknown",
        "select_only_policy": "SELECT-only queries against safe analytics views",
        "query_guardrails": {
            "max_rows_per_query": SAFE_QUERY_LIMIT,
            "max_execution_seconds": QUERY_MAX_EXECUTION_SECONDS,
            "max_scan_bytes": QUERY_MAX_SCAN_BYTES,
            "max_joins": QUERY_MAX_JOIN_COUNT,
            "max_relations": QUERY_MAX_RELATION_COUNT,
        },
        "pii_hashing": {
            "version": QUERY_PII_HASH_VERSION,
            "salt_configured": bool(QUERY_PII_HASH_SALT and "change-me" not in QUERY_PII_HASH_SALT),
        },
        "safe_view_reconciliation": reconciliation,
        "observability": observability,
    }


def metadata_by_view(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["view_name"]: row for row in rows if row.get("view_name")}


def silver_tables_detail() -> list[dict[str, Any]]:
    details = []
    try:
        tables = visible_silver_source_tables()
    except Exception:
        return []
    for table in tables:
        try:
            columns = table_columns(table)
            row_count = table_row_count(table)
            status = "ok"
            error_message = None
        except Exception as exc:
            columns = []
            row_count = 0
            status = "error"
            error_message = str(exc)
        details.append(
            {
                "table_name": table,
                **source_scope_from_table(table),
                "row_count": row_count,
                "column_count": len(columns),
                "columns": columns,
                "status": status,
                "error_message": error_message,
            }
        )
    return details


def safe_views_detail() -> list[dict[str, Any]]:
    metadata_rows = safe_views_metadata()
    metadata = {row["view_name"]: row for row in metadata_rows}
    rows = []
    for view_name in sorted(metadata):
        row = metadata.get(view_name, {})
        source_table = row.get("source_table") or source_table_for_safe_view(view_name)
        try:
            columns = table_columns(view_name) if row.get("status") == "ok" else []
        except Exception:
            columns = []
        scope = source_scope_from_table(source_table)
        pii_safe = bool(row.get("pii_safe"))
        status = row.get("status", "unknown")
        if status != "ok":
            pii_status = "missing_safe_view" if status in {"missing_source", "unknown"} else "needs_attention"
        else:
            safe, blocked = pii_safe_columns(columns)
            pii_status = "protected" if safe and pii_safe else "exposed"
        rows.append(
            {
                "view_name": view_name,
                "source_table": source_table,
                "silver_table": source_table,
                "safe_analytics_view": view_name,
                **scope,
                "row_count": int(row.get("row_count") or 0),
                "column_count": int(row.get("column_count") or len(columns)),
                "source_column_count": int(row.get("source_column_count") or 0),
                "columns": columns,
                "pii_safe": pii_safe,
                "trino_visible": bool(row.get("trino_visible")) if row.get("trino_visible") is not None else (status == "ok"),
                "hash_version": row.get("hash_version") or QUERY_PII_HASH_VERSION,
                "blocked_columns": list(row.get("blocked_columns_json") or []),
                "pii_protection_status": pii_status,
                "mapping_status": "generated" if status == "ok" else "missing" if status in {"missing_source", "unknown"} else "failed",
                "safe_view_status": "available" if status == "ok" else status,
                "last_validated_at": row.get("last_validated_at"),
                "status": status,
                "error_message": row.get("error_message"),
            }
        )
    return rows


def missing_safe_views() -> list[dict[str, Any]]:
    try:
        expected_tables = set(registered_silver_table_names()) | set(visible_silver_source_tables())
    except Exception:
        expected_tables = set(registered_silver_table_names())
    metadata = metadata_by_view(safe_views_metadata())
    try:
        visible_views = set(list_silver_views())
    except Exception:
        visible_views = set()
    missing = []
    for source_table in sorted(expected_tables):
        view_name = safe_view_name_for_table(source_table)
        row = metadata.get(view_name)
        if view_name in visible_views and row and row.get("status") == "ok":
            continue
        missing.append(
            {
                "view_name": view_name,
                "source_table": source_table,
                "silver_table": source_table,
                "safe_analytics_view": view_name,
                **source_scope_from_table(source_table),
                "status": row.get("status") if row else "missing",
                "error_message": row.get("error_message") if row else None,
                "recommended_fix": "Generate safe views or refresh Query metadata.",
            }
        )
    return missing


def safe_view_groups() -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    for view in safe_views_detail():
        database = view.get("database_name") or "unknown"
        collection = view.get("collection_name") or "unknown"
        key = f"{database}.{collection}"
        group = groups.setdefault(
            key,
            {
                "database_name": database,
                "collection_name": collection,
                "safe_views": [],
                "total_views": 0,
                "protected_views": 0,
                "missing_views": 0,
                "failed_views": 0,
            },
        )
        group["safe_views"].append(view)
        group["total_views"] += 1
        if view.get("pii_protection_status") == "protected":
            group["protected_views"] += 1
        if view.get("mapping_status") == "missing":
            group["missing_views"] += 1
        if view.get("mapping_status") == "failed":
            group["failed_views"] += 1
    missing = missing_safe_views()
    for view in missing:
        database = view.get("database_name") or "unknown"
        collection = view.get("collection_name") or "unknown"
        key = f"{database}.{collection}"
        group = groups.setdefault(
            key,
            {
                "database_name": database,
                "collection_name": collection,
                "safe_views": [],
                "total_views": 0,
                "protected_views": 0,
                "missing_views": 0,
                "failed_views": 0,
            },
        )
        group["missing_views"] += 1
    return {
        "groups": sorted(groups.values(), key=lambda item: (item["database_name"], item["collection_name"])),
        "missing_safe_views": missing,
        "select_only_policy": "Only SELECT statements against safe analytics views are accepted.",
    }


def preview_safe_view(view_name: str, limit: int = 25) -> dict[str, Any]:
    if view_name not in known_safe_view_names():
        raise ValueError(f"Unknown safe view: {view_name}")
    eligible = eligible_safe_view_definitions(require_live_check=True)
    if view_name not in eligible:
        raise ValueError("Preview is only available for live eligible safe views")
    limit = max(1, min(int(limit), 25))
    columns, rows, _ = trino_query(
        f'SELECT * FROM delta.silver."{view_name}" LIMIT {limit}',
        observe=True,
        user_source="dashboard_preview",
        selected_view=view_name,
    )
    return {
        "view_name": view_name,
        "limit": limit,
        "columns": columns,
        "records": [dict(zip(columns, row)) for row in rows],
    }


def strip_sql_comments(sql: str) -> str:
    no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--.*?$", " ", no_block, flags=re.MULTILINE)


def relation_base_name(identifier: str) -> str:
    cleaned = identifier.strip().rstrip(",")
    cleaned = cleaned.replace('"', "")
    parts = [part for part in cleaned.split(".") if part]
    return parts[-1] if parts else cleaned


def selected_view_from_sql(sql: str) -> str | None:
    safe_view_names = known_safe_view_names()
    relations = re.findall(r"(?is)\b(?:from|join)\s+([A-Za-z0-9_\".]+)", strip_sql_comments(sql))
    for relation in relations:
        base_name = relation_base_name(relation)
        if base_name in safe_view_names:
            return base_name
    return None


def sanitized_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", strip_sql_comments(sql)).strip()[:8000]


def validate_readonly_sql(sql: str) -> tuple[str, dict[str, Any]]:
    if not sql or not sql.strip():
        raise ValueError("SQL is required")
    if len(sql) > QUERY_MAX_SQL_LENGTH:
        raise ValueError(f"SQL is too long; max {QUERY_MAX_SQL_LENGTH} characters")
    stripped = strip_sql_comments(sql).strip()
    if ";" in stripped:
        raise ValueError("Only one SELECT statement is allowed")
    if not re.match(r"(?is)^select\b", stripped):
        raise ValueError("Only SELECT statements are allowed")
    tokens = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", stripped.upper()))
    forbidden = sorted(tokens & FORBIDDEN_SQL)
    if forbidden:
        raise ValueError(f"Forbidden SQL keyword: {forbidden[0]}")

    relations = re.findall(r"(?is)\b(?:from|join)\s+([A-Za-z0-9_\".]+)", stripped)
    if not relations:
        raise ValueError("Query must read from a safe analytics view")
    join_count = len(re.findall(r"(?is)\bjoin\b", stripped))
    if join_count > QUERY_MAX_JOIN_COUNT:
        raise ValueError(f"Too many JOINs ({join_count}); limit is {QUERY_MAX_JOIN_COUNT}")
    if len(relations) > QUERY_MAX_RELATION_COUNT:
        raise ValueError(f"Too many relations ({len(relations)}); limit is {QUERY_MAX_RELATION_COUNT}")

    allowed = eligible_safe_view_definitions(require_live_check=True)
    relation_bases: list[str] = []
    for relation in relations:
        base_name = relation_base_name(relation)
        relation_bases.append(base_name)
        if base_name not in allowed:
            raise ValueError(
                "Read-only queries may only use live safe analytics views with status=ok and pii_safe=true; "
                f"blocked relation: {relation}"
            )

    limit_match = re.search(r"(?is)\blimit\s+(\d+)\b", stripped)
    normalized_sql = stripped
    if not limit_match:
        normalized_sql = f"{stripped} LIMIT {SAFE_QUERY_LIMIT}"
    else:
        requested = int(limit_match.group(1))
        if requested <= SAFE_QUERY_LIMIT:
            normalized_sql = stripped
        else:
            normalized_sql = f"{stripped[:limit_match.start(1)]}{SAFE_QUERY_LIMIT}{stripped[limit_match.end(1):]}"

    context = {
        "relation_views": sorted(set(relation_bases)),
        "selected_view_status": "ok",
        "guardrails": {
            "max_rows": SAFE_QUERY_LIMIT,
            "max_execution_seconds": QUERY_MAX_EXECUTION_SECONDS,
            "max_scan_bytes": QUERY_MAX_SCAN_BYTES,
        },
    }
    return normalized_sql, context


def run_readonly_query(sql: str, *, query_context: dict[str, Any] | None = None) -> dict[str, Any]:
    started_at = utc_now()
    started_timer = time.monotonic()
    context = query_context or {}
    try:
        validated_sql, validation_context = validate_readonly_sql(sql)
    except Exception as exc:
        finished_at = utc_now()
        record_query_history(
            query_id=f"blocked_{new_id()}",
            user_source="dashboard_readonly",
            sql_text=sanitized_sql(sql),
            selected_view=selected_view_from_sql(sql),
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=int((time.monotonic() - started_timer) * 1000),
            row_count=0,
            status="blocked",
            error_message=str(exc),
            actor=context.get("actor"),
            actor_role=context.get("role"),
            session_id=context.get("session_id"),
            request_id=context.get("request_id"),
            client_ip=context.get("client_ip"),
            query_fingerprint=fingerprint_sql(sql),
            selected_view_status="blocked",
        )
        raise
    selected_view = selected_view_from_sql(validated_sql)
    merged_context = {
        **context,
        "selected_view_status": validation_context.get("selected_view_status", "ok"),
    }
    session_properties = {
        "query_max_execution_time": f"{QUERY_MAX_EXECUTION_SECONDS}s",
        "query_max_scan_physical_bytes": trino_data_size_literal(QUERY_MAX_SCAN_BYTES),
    }
    columns, rows, stats = trino_query(
        validated_sql,
        observe=True,
        user_source="dashboard_readonly",
        selected_view=selected_view,
        trino_user=TRINO_READONLY_USER,
        session_properties=session_properties,
        query_context=merged_context,
    )
    scanned = bytes_from_stats(stats)
    scan_guardrail_exceeded = bool(scanned is not None and scanned > QUERY_MAX_SCAN_BYTES)
    return {
        "status": "ok",
        "sql": validated_sql,
        "columns": columns,
        "records": [dict(zip(columns, row)) for row in rows],
        "row_count": len(rows),
        "query_guardrails": validation_context.get("guardrails", {}),
        "query_id": query_id_from_stats(stats),
        "bytes_scanned": scanned,
        "scan_guardrail_exceeded": scan_guardrail_exceeded,
        "scan_guardrail_limit_bytes": QUERY_MAX_SCAN_BYTES,
    }


def cancel_query(query_id: str, actor: str = "dashboard") -> dict[str, Any]:
    normalized = (query_id or "").strip()
    if not normalized:
        raise ValueError("query_id is required")
    init_dashboard_db()
    attempts: list[dict[str, Any]] = []
    sql_attempts = [
        f"CALL system.runtime.kill_query(query_id => {sql_string_literal(normalized)}, message => {sql_string_literal(f'Cancelled by {actor}')})",
        f"CALL system.runtime.kill_query({sql_string_literal(normalized)}, {sql_string_literal(f'Cancelled by {actor}')})",
    ]
    for sql in sql_attempts:
        try:
            trino_query(sql)
            with dashboard_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE query_history
                        SET status = 'cancelled',
                            error_message = COALESCE(error_message, %s),
                            finished_at = COALESCE(finished_at, now())
                        WHERE query_id = %s
                        """,
                        (f"Cancelled by {actor}", normalized),
                    )
            return {"status": "ok", "query_id": normalized, "message": "Cancellation requested in Trino", "attempts": attempts}
        except Exception as exc:
            attempts.append({"sql": sql, "status": "failed", "error_message": str(exc)})
    return {
        "status": "error",
        "query_id": normalized,
        "message": "Unable to cancel query with current Trino runtime configuration",
        "attempts": attempts,
    }


def query_performance() -> dict[str, Any]:
    init_dashboard_db()
    views = [view for view in safe_views_detail() if view.get("status") == "ok"]
    checks: list[dict[str, Any]] = []
    for view in views:
        view_name = view["view_name"]
        for check_type, sql in [
            ("count", f'SELECT count(*) AS row_count FROM delta.silver."{view_name}"'),
            ("sample_select", f'SELECT * FROM delta.silver."{view_name}" LIMIT 25'),
        ]:
            started_timer = time.monotonic()
            try:
                columns, rows, _ = trino_query(
                    sql,
                    observe=True,
                    user_source="performance_validation",
                    selected_view=view_name,
                )
                duration_ms = int((time.monotonic() - started_timer) * 1000)
                actual_row_count = int(rows[0][0]) if check_type == "count" and rows else len(rows)
                is_slow = duration_ms >= SLOW_QUERY_THRESHOLD_MS
                checks.append(
                    {
                        "view_name": view_name,
                        "check_type": check_type,
                        "status": "slow" if is_slow else "ok",
                        "duration_ms": duration_ms,
                        "row_count": actual_row_count,
                        "columns": columns,
                        "slow_threshold_ms": SLOW_QUERY_THRESHOLD_MS,
                        "error_message": None,
                    }
                )
            except Exception as exc:
                duration_ms = int((time.monotonic() - started_timer) * 1000)
                checks.append(
                    {
                        "view_name": view_name,
                        "check_type": check_type,
                        "status": "failed",
                        "duration_ms": duration_ms,
                        "row_count": 0,
                        "columns": [],
                        "slow_threshold_ms": SLOW_QUERY_THRESHOLD_MS,
                        "error_message": str(exc),
                    }
                )

    total_duration = sum(int(check.get("duration_ms") or 0) for check in checks)
    return {
        "status": "ok" if all(check["status"] in {"ok", "slow"} for check in checks) else "failed",
        "checks": checks,
        "total_checks": len(checks),
        "slow_checks": len([check for check in checks if check["status"] == "slow"]),
        "failed_checks": len([check for check in checks if check["status"] == "failed"]),
        "average_duration_ms": (total_duration / len(checks)) if checks else 0,
        "observability": query_observability_summary(),
    }


def refresh_query_layer(action: str = "metadata") -> dict[str, Any]:
    refresh_results: list[dict[str, Any]] = []
    if action == "restart_trino":
        docker = shutil.which("docker")
        if not docker:
            return {
                "status": "unavailable",
                "message": "Docker CLI is not available inside the dashboard API container. Use `make trino-refresh` or `docker compose restart trino` on the host.",
            }
        result = subprocess.run([docker, "restart", TRINO_CONTAINER_NAME], capture_output=True, text=True, timeout=60)
        return {
            "status": "ok" if result.returncode == 0 else "error",
            "message": result.stdout.strip() or result.stderr.strip(),
        }

    for sql in [
        "CALL delta.system.flush_metadata_cache()",
        "CALL delta.system.flush_metadata_cache(schema_name => 'silver')",
    ]:
        try:
            trino_query(sql)
            refresh_results.append({"sql": sql, "status": "ok"})
        except Exception as exc:
            refresh_results.append({"sql": sql, "status": "skipped", "message": str(exc)})
    registrations = register_existing_silver_tables()
    try:
        reconciliation = safe_view_reconciliation()
    except Exception:
        reconciliation = {}
    return {
        "status": "ok",
        "message": "Query layer metadata refreshed",
        "refresh_attempts": refresh_results,
        "registered_tables": registrations,
        "safe_views": safe_views_detail(),
        "reconciliation": reconciliation,
    }


def generate_safe_views() -> dict[str, Any]:
    views = ensure_safe_views(force=True)
    failed = [view for view in views if view.get("status") not in {"ok"}]
    reconciliation: dict[str, Any] = {}
    try:
        reconciliation = safe_view_reconciliation()
    except Exception:
        reconciliation = {}
    return {
        "status": "warning" if failed else "ok",
        "message": "Dynamic safe views generated" if not failed else "Some dynamic safe views could not be generated",
        "safe_views": views,
        "generated_count": len([view for view in views if view.get("status") == "ok"]),
        "rebuilt_count": len([view for view in views if view.get("status") == "ok"]),
        "warning_count": len(failed),
        "failed_count": len(failed),
        "warnings": failed,
        "reconciliation": reconciliation,
    }


def pretty_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str, sort_keys=True)
