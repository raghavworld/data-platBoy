#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from psycopg2.extras import DictCursor, Json

from common import load_environment
from dashboard_db import dashboard_connection, init_dashboard_db, new_id


API_BASE = os.environ.get("DASHBOARD_API_BASE_URL") or f"http://localhost:{os.environ.get('DASHBOARD_API_PORT', '8001')}"
USERNAME = os.environ.get("ONOV8_ADMIN_USERNAME", "admin")
PASSWORD = os.environ.get("ONOV8_ADMIN_PASSWORD", "admin")


def ok(message: str, details: Any | None = None) -> dict[str, Any]:
    return {"status": "ok", "message": message, "details": details}


def fail(message: str, details: Any | None = None) -> None:
    raise RuntimeError(f"{message}: {details}" if details is not None else message)


def request_json(
    method: str,
    path: str,
    token: str | None = None,
    payload: dict[str, Any] | None = None,
    *,
    expect: int = 200,
    timeout: int = 30,
) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(f"{API_BASE.rstrip('/')}{path}", data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            status = response.status
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        status = exc.code
    except URLError as exc:
        fail(f"{method} {path} could not connect", str(exc))

    if status != expect:
        fail(f"{method} {path} returned HTTP {status}, expected {expect}", body[:400])
    return json.loads(body or "{}")


def login() -> str:
    payload = request_json("POST", "/api/auth/login", payload={"username": USERNAME, "password": PASSWORD}, timeout=30)
    token = payload.get("token")
    if not token:
        fail("Login did not return a session token")
    return token


def table_exists(table_name: str) -> bool:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = %s
                )
                """,
                (table_name,),
            )
            return bool(cursor.fetchone()[0])


def source_rows() -> list[dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM source_connections ORDER BY source_name")
            return [dict(row) for row in cursor.fetchall()]


def source_timestamp_snapshot() -> dict[str, tuple[Any, Any]]:
    return {
        str(row["id"]): (row.get("last_test_at"), row.get("last_inventory_at"))
        for row in source_rows()
    }


def collection_state_snapshot(source_id: str) -> dict[str, bool]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT collection_name, is_active
                FROM source_connection_collections
                WHERE source_id = %s
                """,
                (source_id,),
            )
            return {row["collection_name"]: bool(row["is_active"]) for row in cursor.fetchall()}


def restore_source_and_collections(source_id: str, is_active: bool, include_collections: Any, exclude_collections: Any, collections: dict[str, bool]) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for collection_name, collection_active in collections.items():
                cursor.execute(
                    """
                    UPDATE source_connection_collections
                    SET is_active = %s,
                        updated_at = now()
                    WHERE source_id = %s
                      AND collection_name = %s
                    """,
                    (collection_active, source_id, collection_name),
                )
            cursor.execute(
                """
                UPDATE source_connections
                SET is_active = %s,
                    include_collections_json = %s,
                    exclude_collections_json = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (is_active, Json(include_collections), Json(exclude_collections), source_id),
            )


def validate_paginated_sources(token: str) -> list[dict[str, Any]]:
    started = time.monotonic()
    payload = request_json("GET", "/api/sources?page=1&page_size=10&active=all&health=all", token, timeout=10)
    elapsed = time.monotonic() - started
    if not isinstance(payload, dict):
        fail("Paginated sources endpoint returned a non-object payload", payload)
    for key in ("items", "page", "page_size", "total", "pages"):
        if key not in payload:
            fail("Paginated sources endpoint is missing a field", key)
    if payload["page_size"] != 10:
        fail("Paginated sources endpoint did not honor page_size=10", payload)
    if elapsed > 2.0:
        fail("Paginated sources endpoint is too slow", {"duration_seconds": round(elapsed, 3)})
    return [ok("sources endpoint paginates", {"total": payload["total"], "duration_seconds": round(elapsed, 3)})]


def validate_summary(token: str) -> list[dict[str, Any]]:
    payload = request_json("GET", "/api/sources/summary", token, timeout=10)
    required = {
        "total_databases",
        "active_databases",
        "inactive_databases",
        "partial_databases",
        "tested_databases",
        "healthy_databases",
        "total_collections",
        "active_collections",
        "total_records",
        "active_records",
        "total_size_bytes",
        "active_size_bytes",
        "total_size_mb",
        "active_size_mb",
    }
    missing = sorted(required - set(payload))
    if missing:
        fail("Summary endpoint is missing fields", missing)
    return [ok("summary endpoint works", payload)]


def validate_normal_load_is_cached(token: str) -> list[dict[str, Any]]:
    before = source_timestamp_snapshot()
    request_json("GET", "/api/sources?page=1&page_size=10", token, timeout=10)
    request_json("GET", "/api/sources/summary", token, timeout=10)
    request_json("GET", "/api/services/status", token, timeout=10)
    after = source_timestamp_snapshot()
    changed = sorted(source_id for source_id, stamp in before.items() if after.get(source_id) != stamp)
    if changed:
        fail("Normal source page load changed source timestamps, which suggests Mongo work ran", changed)
    return [ok("source page metadata load does not mutate test or inventory timestamps")]


def validate_source_active_persistence(token: str) -> list[dict[str, Any]]:
    rows = source_rows()
    if not rows:
        fail("No source connection is available for source active persistence validation")
    source = rows[0]
    source_id = str(source["id"])
    before_active = bool(source.get("is_active"))
    before_include = source.get("include_collections_json") or []
    before_exclude = source.get("exclude_collections_json") or []
    before_collections = collection_state_snapshot(source_id)
    try:
        activated = request_json("POST", f"/api/sources/{source_id}/activate", token, timeout=20)
        if not activated.get("is_active"):
            fail("Source activate endpoint did not persist active=true", activated)
        fetched_active = request_json("GET", f"/api/sources/{source_id}", token, timeout=10)
        if not fetched_active.get("is_active"):
            fail("Source active state did not persist through fetch", fetched_active)

        deactivated = request_json("POST", f"/api/sources/{source_id}/deactivate", token, timeout=20)
        if deactivated.get("is_active"):
            fail("Source deactivate endpoint did not persist active=false", deactivated)
        fetched_inactive = request_json("GET", f"/api/sources/{source_id}", token, timeout=10)
        if fetched_inactive.get("is_active"):
            fail("Source inactive state did not persist through fetch", fetched_inactive)

        return [ok("source active changes persist", {"source": source["source_name"]})]
    finally:
        restore_source_and_collections(source_id, before_active, before_include, before_exclude, before_collections)


def validate_default_inactive(token: str) -> list[dict[str, Any]]:
    source_name = f"inventory_validation_{new_id().replace('-', '')[:10]}"
    created = request_json(
        "POST",
        "/api/sources",
        token,
        {
            "source_name": source_name,
            "source_type": "mongo",
            "host": "example.invalid",
            "port": 27017,
            "database_name": source_name,
            "auth_database": "admin",
            "cursor_field": "AUTO",
            "ingestion_mode": "python",
        },
        timeout=10,
    )
    try:
        if created.get("is_active"):
            fail("New source was active without an explicit user activation", created)
        return [ok("new source connections remain inactive unless explicitly changed", {"source": source_name})]
    finally:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM source_connections WHERE source_name = %s", (source_name,))


def source_in_filtered_results(token: str, source_name: str, active_filter: str) -> bool:
    payload = request_json(
        "GET",
        f"/api/sources?page=1&page_size=10&search={source_name}&active={active_filter}&health=all",
        token,
        timeout=10,
    )
    return any(item.get("source_name") == source_name for item in payload.get("items", []))


def validate_active_partial_selection_model(token: str) -> list[dict[str, Any]]:
    source_id = new_id()
    source_name = f"selection_validation_{source_id.replace('-', '')[:10]}"
    collections = [
        ("active_orders", 11, 4096),
        ("active_customers", 7, 2048),
        ("audit_events", 5, 1024),
    ]
    try:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO source_connections (
                        id, source_name, source_type, database_name, auth_database,
                        connection_config_json, include_collections_json, exclude_collections_json,
                        cursor_field, ingestion_mode, is_active
                    )
                    VALUES (%s, %s, 'mongo', %s, 'admin', %s, '[]'::jsonb, '[]'::jsonb, 'AUTO', 'python', false)
                    """,
                    (source_id, source_name, source_name, Json({"host": "example.invalid", "port": 27017})),
                )
                for collection_name, record_count, estimated_size_bytes in collections:
                    cursor.execute(
                        """
                        INSERT INTO source_connection_collections (
                            id, source_id, collection_name, is_active, record_count, estimated_size_bytes
                        )
                        VALUES (%s, %s, %s, false, %s, %s)
                        """,
                        (new_id(), source_id, collection_name, record_count, estimated_size_bytes),
                    )

        activated = request_json("POST", f"/api/sources/{source_id}/activate", token, timeout=20)
        if not activated.get("is_active"):
            fail("Source toggle ON did not activate the source", activated)
        if int(activated.get("active_collections_count") or 0) != len(collections):
            fail("Source toggle ON did not activate all child collections", activated)
        active_children = request_json("GET", f"/api/sources/{source_id}/collections", token, timeout=10).get("collections") or []
        if len(active_children) != len(collections) or any(not item.get("is_active") for item in active_children):
            fail("Source toggle ON did not persist all child toggles active", active_children)

        deactivated = request_json("POST", f"/api/sources/{source_id}/deactivate", token, timeout=20)
        if deactivated.get("is_active"):
            fail("Source toggle OFF did not deactivate the source", deactivated)
        if int(deactivated.get("active_collections_count") or 0) != 0:
            fail("Source toggle OFF did not reset active collection count", deactivated)
        inactive_children = request_json("GET", f"/api/sources/{source_id}/collections", token, timeout=10).get("collections") or []
        if any(item.get("is_active") for item in inactive_children):
            fail("Source toggle OFF did not deactivate all child collections", inactive_children)

        database_only = request_json("POST", f"/api/sources/{source_id}/activate-database", token, timeout=20)
        if not database_only.get("is_active"):
            fail("Activate database only did not mark the database active", database_only)
        if int(database_only.get("active_collections_count") or 0) != 0:
            fail("Activate database only unexpectedly activated child collections", database_only)
        database_only_children = request_json("GET", f"/api/sources/{source_id}/collections", token, timeout=10).get("collections") or []
        if any(item.get("is_active") for item in database_only_children):
            fail("Activate database only persisted an active child collection", database_only_children)

        selected_all = request_json("POST", f"/api/sources/{source_id}/collections/select-all", token, timeout=20)
        if int(selected_all.get("source", {}).get("active_collections_count") or 0) != len(collections):
            fail("Select All did not activate every child collection", selected_all)
        selected_children = selected_all.get("collections") or []
        if len(selected_children) != len(collections) or any(not item.get("is_active") for item in selected_children):
            fail("Select All did not return all children active", selected_children)

        deselected_all = request_json("POST", f"/api/sources/{source_id}/collections/deselect-all", token, timeout=20)
        if deselected_all.get("source", {}).get("is_active"):
            fail("Deselect All did not mark the parent database inactive", deselected_all)
        if int(deselected_all.get("source", {}).get("active_collections_count") or 0) != 0:
            fail("Deselect All did not reset active child collection count", deselected_all)
        deselected_children = deselected_all.get("collections") or []
        if any(item.get("is_active") for item in deselected_children):
            fail("Deselect All did not return all children inactive", deselected_children)

        baseline_summary = request_json("GET", "/api/sources/summary", token, timeout=10)
        one_active = request_json(
            "POST",
            f"/api/sources/{source_id}/collections/{collections[0][0]}/activate",
            token,
            timeout=20,
        )
        parent = one_active.get("source") or {}
        if not parent.get("is_active"):
            fail("One active collection did not activate the parent source", parent)
        if parent.get("selection_status") != "partial":
            fail("One active collection did not make the parent partially active", parent)
        if int(parent.get("active_collections_count") or 0) != 1:
            fail("Partial parent active collection count is wrong", parent)

        summary_after_one = request_json("GET", "/api/sources/summary", token, timeout=10)
        expected_summary = {
            "active_databases": int(baseline_summary.get("active_databases") or 0) + 1,
            "partial_databases": int(baseline_summary.get("partial_databases") or 0) + 1,
            "inactive_databases": int(baseline_summary.get("inactive_databases") or 0) - 1,
            "active_collections": int(baseline_summary.get("active_collections") or 0) + 1,
            "active_records": int(baseline_summary.get("active_records") or 0) + collections[0][1],
            "active_size_bytes": int(baseline_summary.get("active_size_bytes") or 0) + collections[0][2],
        }
        for key, expected in expected_summary.items():
            if int(summary_after_one.get(key) or 0) != expected:
                fail("Summary card source changed incorrectly after partial activation", {"field": key, "expected": expected, "actual": summary_after_one.get(key)})

        if not source_in_filtered_results(token, source_name, "active"):
            fail("Active database filter did not include a partially active source")
        if not source_in_filtered_results(token, source_name, "partial"):
            fail("Partially Active database filter did not include the partial source")
        if source_in_filtered_results(token, source_name, "inactive"):
            fail("Inactive database filter included a partially active source")

        request_json("POST", f"/api/sources/{source_id}/deactivate", token, timeout=20)
        final_source = request_json("GET", f"/api/sources/{source_id}", token, timeout=10)
        if final_source.get("selection_status") != "inactive":
            fail("Final source cleanup did not return the parent to inactive state", final_source)

        return [
            ok("source toggle ON activates all child collections", {"source": source_name, "collections": len(collections)}),
            ok("source toggle OFF deactivates all child collections", {"source": source_name}),
            ok("activate database only keeps child collections inactive", {"source": source_name}),
            ok("select all activates every child collection", {"source": source_name}),
            ok("deselect all deactivates every child collection", {"source": source_name}),
            ok("one active collection makes parent partially active", {"source": source_name}),
            ok("active filters include partially active databases", {"source": source_name}),
            ok("summary cards update for partial source selection", {"records": collections[0][1], "size_bytes": collections[0][2]}),
        ]
    finally:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM source_connection_collections WHERE source_id = %s", (source_id,))
                cursor.execute("DELETE FROM source_connections WHERE id = %s", (source_id,))


def choose_refreshable_source() -> dict[str, Any] | None:
    for row in source_rows():
        config = row.get("connection_config_json") or {}
        if isinstance(config, str):
            config = json.loads(config)
        if config.get("host") or row.get("secret_reference"):
            return row
    return None


def validate_inventory_refresh_and_selection(token: str) -> list[dict[str, Any]]:
    source = choose_refreshable_source()
    if not source:
        fail("No source connection is available for inventory refresh validation")

    source_id = str(source["id"])
    before_active = bool(source.get("is_active"))
    before_include = source.get("include_collections_json") or []
    before_exclude = source.get("exclude_collections_json") or []
    before_collections = collection_state_snapshot(source_id)
    try:
        refreshed = request_json("POST", f"/api/sources/{source_id}/refresh-inventory", token, timeout=90)
        collections = refreshed.get("collections") or []
        if not collections:
            fail("Inventory refresh did not store any collections", refreshed)

        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM source_connection_collections WHERE source_id = %s", (source_id,))
                cached_count = int(cursor.fetchone()[0])
        if cached_count < len(collections):
            fail("Inventory refresh did not persist all refreshed collections", {"api": len(collections), "db": cached_count})

        collection_name = collections[0]["collection_name"]
        deactivated = request_json(
            "POST",
            f"/api/sources/{source_id}/collections/{collection_name}/deactivate",
            token,
            timeout=20,
        )
        if deactivated.get("collection", {}).get("is_active"):
            fail("Collection deactivate endpoint did not mark collection inactive", deactivated)
        source_after_deactivate = request_json("GET", f"/api/sources/{source_id}", token, timeout=10)
        count_after_deactivate = int(source_after_deactivate.get("active_collections_count") or 0)
        summary_after_deactivate = request_json("GET", "/api/sources/summary", token, timeout=10)

        activated = request_json(
            "POST",
            f"/api/sources/{source_id}/collections/{collection_name}/activate",
            token,
            timeout=20,
        )
        if not activated.get("collection", {}).get("is_active"):
            fail("Collection activate endpoint did not mark collection active", activated)
        source_after_activate = request_json("GET", f"/api/sources/{source_id}", token, timeout=10)
        if int(source_after_activate.get("active_collections_count") or 0) != count_after_deactivate + 1:
            fail(
                "Source active collection count did not update from API",
                {"before": count_after_deactivate, "after": source_after_activate.get("active_collections_count")},
            )
        if int(source_after_activate.get("active_collections_count") or 0) > 0 and not source_after_activate.get("is_active"):
            fail("Collection activation did not mark the parent database active", source_after_activate)
        summary_after_activate = request_json("GET", "/api/sources/summary", token, timeout=10)
        if int(summary_after_activate.get("active_collections") or 0) != int(summary_after_deactivate.get("active_collections") or 0) + 1:
            fail(
                "Summary active collection count did not update from API",
                {
                    "before": summary_after_deactivate.get("active_collections"),
                    "after": summary_after_activate.get("active_collections"),
                },
            )

        return [
            ok("inventory refresh stores collections", {"source": source["source_name"], "collections": len(collections)}),
            ok("collections can be activated and deactivated", {"collection": collection_name}),
            ok("active counts update from API", {"source_active_count": source_after_activate.get("active_collections_count")}),
            ok("collection selection updates parent database state"),
        ]
    finally:
        restore_source_and_collections(source_id, before_active, before_include, before_exclude, before_collections)


def validate_source_code_labels() -> list[dict[str, Any]]:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    main_path = os.path.join(root, "dashboard", "web", "src", "main.jsx")
    styles_path = os.path.join(root, "dashboard", "web", "src", "styles.css")
    with open(main_path, "r", encoding="utf-8") as handle:
        main_source = handle.read()
    with open(styles_path, "r", encoding="utf-8") as handle:
        styles_source = handle.read()
    for label in ("Databases", "Collections", "Records", "Data Size"):
        if label not in main_source:
            fail("Combined summary card label is missing from Source Connections source code", label)
    if "SourceSummaryCard" not in main_source or "source-summary-card" not in styles_source:
        fail("Combined summary card implementation is missing")
    if 'value="partial">Partially Active' not in main_source:
        fail("Database Status filter is missing the Partially Active option")
    if "Active + Partially Active" not in main_source:
        fail("Active filter does not label partial database inclusion")
    if "Some collections are active for ingestion." not in main_source:
        fail("Partial active source toggle tooltip is missing")
    try:
        status_start = main_source.index("function SourceStatusIcon")
        status_end = main_source.index("function activeCollectionTone", status_start)
    except ValueError as exc:
        fail("Source status icon component could not be found", str(exc))
    status_component = main_source[status_start:status_end]
    if "<Badge" in status_component or "state.helper" in status_component:
        fail("Source Connections status column still renders text badges")
    for token in ("source-status-icon", "data-source-status", "CheckCircle2", "PowerOff", "aria-label", "title="):
        if token not in status_component:
            fail("Source Connections icon status implementation is missing", token)
    if "CirclePause" in status_component or 'const Icon = state.status === "active" ? CheckCircle2 : state.status === "partial"' in status_component:
        fail("Partially Active status must reuse the Active icon shape, not pause iconography")
    if 'const Icon = state.status === "inactive" ? PowerOff : CheckCircle2' not in status_component:
        fail("Partially Active status is not using the same icon shape as Active")
    for tooltip in ("All collections active", "No collections active", "Some collections active"):
        if tooltip not in status_component:
            fail("Source Connections status icon tooltip detail is missing", tooltip)
    required_status_styles = {
        ".source-status-icon.active": "#1f6f5b",
        ".source-status-icon.partial": "#b7791f",
        ".source-status-icon.inactive": "#64748b",
        ".source-status-icon:focus-visible": "outline",
    }
    for selector, expected in required_status_styles.items():
        if selector not in styles_source or expected not in styles_source:
            fail("Source Connections icon status styling is missing or has wrong color", {"selector": selector, "expected": expected})
    if "source-active-glow" not in styles_source or "source-partial-pulse" not in styles_source:
        fail("Source Connections icon status animation styling is missing")
    if "const optimisticSource = sourceWithCollectionState(previousSource, previousCollection, nextActive)" not in main_source:
        fail("Partial state recalculation is not wired to collection toggles")
    if "Partially Active" not in main_source or ".source-count-stack.partial strong" not in styles_source:
        fail("Partial active state styling is missing")
    if 'data-validation-id="source-activation-help"' not in main_source:
        fail("Source activation help text is missing")
    if 'data-validation-id="source-activation-confirmation"' not in main_source:
        fail("Source activation confirmation modal is missing")
    for label in ("Activate all collections", "Activate database only", "Select All", "Deselect All"):
        if label not in main_source:
            fail("Source Connections control label is missing", label)
    for label in ("Select All Databases", "Deselect All Databases"):
        if label in main_source:
            fail("Top-level bulk database control is still visible in Source Connections source code", label)
    try:
        table_start = main_source.index('className="table-wrap source-inventory-table"')
        header_end = main_source.index("</thead>", table_start)
    except ValueError as exc:
        fail("Source Connections table header could not be found", str(exc))
    source_header = main_source[table_start:header_end]
    expected_headers = [
        "<th>Database</th>",
        "<th>Status</th>",
        "<th>Schedule</th>",
        "<th>Active Collections</th>",
        "<th>Records</th>",
        "<th>Size</th>",
        "<th>Last Inventory</th>",
        "<th>Last Test</th>",
        "<th>Actions</th>",
    ]
    for header in expected_headers:
        if header not in source_header:
            fail("Source Connections table header is missing", header)
    for removed_header in ("<th>Source</th>", "<th>Host</th>", "<th>Collections</th>", "<th>Active</th>", "<th />"):
        if removed_header in source_header:
            fail("Removed Source Connections table header is still present", removed_header)
    if ".toggle.partial" not in styles_source:
        fail("Partial active source toggle styling is missing")
    if ".source-selection-help" not in styles_source or ".source-activation-choice-grid" not in styles_source:
        fail("Source activation help/confirmation styling is missing")
    return [ok("page source code includes focused Source Connections columns, icon-only status states, and partial active UI")]


def main() -> int:
    load_environment()
    init_dashboard_db()
    if not table_exists("source_connection_collections"):
        fail("source_connection_collections table does not exist")

    token = login()
    checks = [
        *validate_paginated_sources(token),
        *validate_summary(token),
        *validate_normal_load_is_cached(token),
        *validate_default_inactive(token),
        *validate_source_active_persistence(token),
        *validate_active_partial_selection_model(token),
        *validate_inventory_refresh_and_selection(token),
        *validate_source_code_labels(),
    ]
    print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2, sort_keys=True, default=str))
        raise
