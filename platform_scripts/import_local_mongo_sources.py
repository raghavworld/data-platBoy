#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from typing import Any
from urllib.parse import quote_plus

import pymongo
from psycopg2.extras import DictCursor, Json

from common import load_environment, mask_sensitive_text
from dashboard_db import dashboard_connection, init_dashboard_db, new_id
from source_secrets import put_secret


DEFAULT_HOST = "host.docker.internal"
DEFAULT_PORT = 27017
DEFAULT_AUTH_DATABASE = "admin"
EXCLUDED_DATABASES = {"admin", "config", "local"}
DEMO_DATABASES = {"users_service", "orders_service", "products_service", "payments_service"}
TARGET_DATABASES = [
    "access-control",
    "administration",
    "authentication",
    "blockchain",
    "cc-avenue",
    "configurations",
    "content-master",
    "ezms-backend",
    "form-builder",
    "google-auth",
    "hyperverge-service",
    "manage-integration",
    "ni-payment",
    "push-integration",
    "service-management",
    "sms-integration",
    "whatsapp-integration",
    "workflow-builder",
    "worldcheck-service",
    "zoho-integration",
]


def mongo_uri(host: str, port: int, auth_database: str, username: str = "", password: str = "") -> str:
    database = auth_database or "admin"
    if username and password:
        return (
            f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/"
            f"{quote_plus(database)}?authSource={quote_plus(database)}"
        )
    return f"mongodb://{host}:{port}/{quote_plus(database)}"


def masked_uri(host: str, port: int, auth_database: str, username: str = "", password: str = "") -> str:
    return mask_sensitive_text(mongo_uri(host, port, auth_database, username, password))


def list_local_databases(
    host: str,
    port: int,
    auth_database: str,
    username: str,
    password: str,
    timeout_ms: int,
) -> list[str]:
    uri = mongo_uri(host, port, auth_database, username, password)
    with pymongo.MongoClient(
        uri,
        connectTimeoutMS=timeout_ms,
        serverSelectionTimeoutMS=timeout_ms,
        socketTimeoutMS=max(timeout_ms, 5000),
        tz_aware=True,
    ) as client:
        client.admin.command("ping")
        return sorted(client.list_database_names())


def source_payload(
    database_name: str,
    host: str,
    port: int,
    auth_database: str,
    username: str,
    password: str,
    source_id: str | None = None,
) -> dict[str, Any]:
    source_id = source_id or new_id()
    secret_reference = None
    if username and password:
        secret_reference = f"source-{source_id}"
        put_secret(secret_reference, {"mongo_uri": mongo_uri(host, port, auth_database, username, password)})

    config: dict[str, Any] = {
        "host": host,
        "port": port,
        "auth_database": auth_database,
        "database_name": database_name,
        "masked_mongo_uri": masked_uri(host, port, auth_database, username, password),
    }
    if username:
        config["username"] = username

    return {
        "id": source_id,
        "source_name": database_name,
        "source_type": "mongo",
        "database_name": database_name,
        "auth_database": auth_database,
        "connection_config_json": config,
        "secret_reference": secret_reference,
        "include_collections_json": [],
        "exclude_collections_json": [],
        "cursor_field": "AUTO",
        "ingestion_mode": "python",
        "is_active": False,
    }


def upsert_sources(
    database_names: list[str],
    host: str,
    port: int,
    auth_database: str,
    username: str,
    password: str,
    force_inactive_existing: bool,
) -> dict[str, list[str]]:
    inserted: list[str] = []
    already_existed: list[str] = []
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            for database_name in database_names:
                cursor.execute("SELECT id, is_active FROM source_connections WHERE source_name = %s", (database_name,))
                existing = cursor.fetchone()
                existing_id = str(existing["id"]) if existing else None
                payload = source_payload(database_name, host, port, auth_database, username, password, existing_id)
                if existing:
                    if not force_inactive_existing:
                        payload["is_active"] = bool(existing["is_active"])

                cursor.execute(
                    """
                    INSERT INTO source_connections (
                        id, source_name, source_type, database_name, auth_database,
                        connection_config_json, secret_reference, include_collections_json,
                        exclude_collections_json, cursor_field, ingestion_mode, is_active,
                        last_test_status, last_test_message, last_test_at
                    )
                    VALUES (
                        %(id)s, %(source_name)s, %(source_type)s, %(database_name)s, %(auth_database)s,
                        %(connection_config_json)s, %(secret_reference)s, %(include_collections_json)s,
                        %(exclude_collections_json)s, %(cursor_field)s, %(ingestion_mode)s, %(is_active)s,
                        NULL, NULL, NULL
                    )
                    ON CONFLICT (source_name)
                    DO UPDATE SET
                        source_type = EXCLUDED.source_type,
                        database_name = EXCLUDED.database_name,
                        auth_database = EXCLUDED.auth_database,
                        connection_config_json = EXCLUDED.connection_config_json,
                        secret_reference = EXCLUDED.secret_reference,
                        include_collections_json = EXCLUDED.include_collections_json,
                        exclude_collections_json = EXCLUDED.exclude_collections_json,
                        cursor_field = EXCLUDED.cursor_field,
                        ingestion_mode = EXCLUDED.ingestion_mode,
                        is_active = EXCLUDED.is_active,
                        updated_at = now()
                    """,
                    {
                        **payload,
                        "connection_config_json": Json(payload["connection_config_json"]),
                        "include_collections_json": Json(payload["include_collections_json"]),
                        "exclude_collections_json": Json(payload["exclude_collections_json"]),
                    },
                )
                if existing:
                    already_existed.append(database_name)
                else:
                    inserted.append(database_name)
    return {"inserted": inserted, "already_existed": already_existed}


def main() -> int:
    load_environment()
    parser = argparse.ArgumentParser(description="Import real local MongoDB databases as inactive dashboard source connections.")
    parser.add_argument("--host", default=os.environ.get("LOCAL_MONGO_HOST", DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("LOCAL_MONGO_PORT", DEFAULT_PORT)))
    parser.add_argument("--auth-database", default=os.environ.get("LOCAL_MONGO_AUTH_DATABASE", DEFAULT_AUTH_DATABASE))
    parser.add_argument("--username", default=os.environ.get("LOCAL_MONGO_USERNAME", ""))
    parser.add_argument("--password", default=os.environ.get("LOCAL_MONGO_PASSWORD", ""))
    parser.add_argument("--timeout-ms", type=int, default=int(os.environ.get("LOCAL_MONGO_TIMEOUT_MS", "3000")))
    parser.add_argument("--import-all", action="store_true", help="Import every non-system database instead of the requested real database list.")
    parser.add_argument("--force-inactive-existing", action="store_true", help="Mark existing matching sources inactive while refreshing them.")
    args = parser.parse_args()

    databases_found = list_local_databases(
        args.host,
        args.port,
        args.auth_database,
        args.username,
        args.password,
        args.timeout_ms,
    )

    requested = set(TARGET_DATABASES)
    skipped: list[dict[str, str]] = []
    importable: list[str] = []
    for database_name in databases_found:
        if database_name in EXCLUDED_DATABASES or database_name.startswith("system."):
            skipped.append({"database": database_name, "reason": "system database"})
            continue
        if database_name in DEMO_DATABASES:
            skipped.append({"database": database_name, "reason": "demo database"})
            continue
        if not args.import_all and database_name not in requested:
            skipped.append({"database": database_name, "reason": "not in requested local database list"})
            continue
        importable.append(database_name)

    missing_requested = sorted(requested - set(databases_found))
    result = upsert_sources(
        sorted(importable),
        args.host,
        args.port,
        args.auth_database,
        args.username,
        args.password,
        args.force_inactive_existing,
    )

    summary = {
        "status": "ok",
        "host": args.host,
        "port": args.port,
        "auth_database": args.auth_database,
        "credentials": "configured" if args.username or args.password else "empty",
        "databases_found": databases_found,
        "databases_skipped": skipped,
        "requested_databases_missing": missing_requested,
        "sources_inserted": result["inserted"],
        "sources_already_existed": result["already_existed"],
        "inactive_by_default": True,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
