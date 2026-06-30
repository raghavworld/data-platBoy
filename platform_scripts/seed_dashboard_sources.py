from __future__ import annotations

import argparse
import os
from typing import Any

from common import load_environment, setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db, new_id
from source_secrets import put_secret


DEMO_SEED_DISABLED_MESSAGE = (
    "Demo source seeding is disabled. Use scripts/import_local_mongo_sources.py "
    "to import real local MongoDB databases as inactive source connections."
)

DEMO_SOURCES = (
    ("users_service", "mongo-users", "users"),
    ("orders_service", "mongo-orders", "orders"),
    ("products_service", "mongo-products", "products"),
    ("payments_service", "mongo-payments", "payments"),
)


def masked_uri(username: str, host: str) -> str:
    return f"mongodb://{username}:***@{host}:27017/admin?authSource=admin"


def demo_source_payload(source_name: str, host: str, collection_name: str) -> dict[str, Any]:
    username = os.environ["MONGO_ROOT_USERNAME"]
    password = os.environ["MONGO_ROOT_PASSWORD"]
    uri = f"mongodb://{username}:{password}@{host}:27017/admin?authSource=admin"
    secret_reference = f"demo-{source_name}"
    put_secret(secret_reference, {"mongo_uri": uri})
    return {
        "id": new_id(),
        "source_name": source_name,
        "source_type": "mongo",
        "database_name": source_name,
        "auth_database": "admin",
        "connection_config_json": {
            "host": host,
            "port": 27017,
            "username": username,
            "masked_mongo_uri": masked_uri(username, host),
        },
        "secret_reference": secret_reference,
        "include_collections_json": [collection_name],
        "exclude_collections_json": [],
        "cursor_field": "updatedAt",
        "ingestion_mode": "python",
        "is_active": True,
    }


def seed_demo_sources() -> int:
    load_environment()
    init_dashboard_db()
    return 0


def active_source_count() -> int:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM source_connections WHERE is_active = true")
            return int(cursor.fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-active", action="store_true", help="Fail when no active sources exist")
    parser.add_argument("--ensure-schema", action="store_true", help="Only ensure dashboard tables exist")
    args = parser.parse_args()

    logger = setup_logging("seed_dashboard_sources")
    if args.ensure_schema:
        init_dashboard_db()
        logger.info("Dashboard schema is ready")
    else:
        seeded = seed_demo_sources()
        logger.info("%s Seeded %s demo source connections.", DEMO_SEED_DISABLED_MESSAGE, seeded)

    if args.check_active:
        count = active_source_count()
        logger.info("Active source connections: %s", count)
        if count == 0:
            raise RuntimeError("No active source connections configured")


if __name__ == "__main__":
    main()
