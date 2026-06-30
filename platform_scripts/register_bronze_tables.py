from __future__ import annotations

import os
from typing import Any

from psycopg2.extras import DictCursor

from common import bronze_table_name, setup_logging, sql_string_literal, trino_connection
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db


def scoped_collection_clause() -> tuple[str, list[Any]]:
    database_name = (
        os.environ.get("BRONZE_DATABASE_NAME")
        or os.environ.get("SILVER_DATABASE_NAME")
        or ""
    ).strip()
    source_id = (os.environ.get("BRONZE_SOURCE_ID") or "").strip()
    collection_name = (
        os.environ.get("BRONZE_COLLECTION_NAME")
        or os.environ.get("SILVER_COLLECTION_NAME")
        or ""
    ).strip()
    clauses: list[str] = ["bronze_table_path IS NOT NULL", "bronze_table_path <> ''"]
    params: list[Any] = []
    if source_id:
        clauses.append("source_id::text = %s")
        params.append(source_id)
    if database_name:
        clauses.append("database_name = %s")
        params.append(database_name)
    if collection_name:
        clauses.append("collection_name = %s")
        params.append(collection_name)
    return f"WHERE {' AND '.join(clauses)}", params


def bronze_table_locations() -> list[dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            where, params = scoped_collection_clause()
            cursor.execute(
                f"""
                SELECT database_name, collection_name, bronze_table_path, trino_table_name, updated_at
                FROM bronze_collection_states
                {where}
                ORDER BY updated_at DESC
                """,
                params,
            )
            return [as_dict(row) for row in cursor.fetchall()]


def path_candidates(path: str) -> list[str]:
    candidates = [path]
    s3_path = path.replace("s3a://", "s3://", 1)
    if s3_path != path:
        candidates.append(s3_path)
    return candidates


def register_bronze_tables() -> list[dict[str, str]]:
    init_dashboard_db()
    logger = setup_logging("register_bronze_tables")
    table_locations = bronze_table_locations()
    if not table_locations:
        logger.info("No Bronze table states found for registration scope")
        print("No Bronze table states found for registration scope")
        return []

    connection = trino_connection(schema="bronze")
    cursor = connection.cursor()
    results: list[dict[str, str]] = []
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS delta.bronze")
        print("schema bronze: created_or_already_present")
        logger.info("Ensured Trino Delta schema delta.bronze exists")
        try:
            cursor.execute("SHOW TABLES FROM delta.bronze")
            existing = {str(row[0]) for row in cursor.fetchall()}
        except Exception:
            existing = set()

        for row in table_locations:
            table_name = row.get("trino_table_name") or bronze_table_name(row["database_name"], row["collection_name"])
            table_location = row["bronze_table_path"]
            if table_name in existing:
                result = {"table_name": table_name, "status": "already_present", "table_location": table_location}
                results.append(result)
                print(f"table {table_name}: already_present at {table_location}")
                continue

            last_error = ""
            registered = False
            for candidate in path_candidates(table_location):
                try:
                    cursor.execute(
                        f"""
                        CALL delta.system.register_table(
                            schema_name => 'bronze',
                            table_name => {sql_string_literal(table_name)},
                            table_location => {sql_string_literal(candidate)}
                        )
                        """
                    )
                    existing.add(table_name)
                    registered = True
                    result = {"table_name": table_name, "status": "registered", "table_location": candidate}
                    results.append(result)
                    print(f"table {table_name}: registered at {candidate}")
                    logger.info("Registered Delta table delta.bronze.%s at %s", table_name, candidate)
                    break
                except Exception as exc:
                    last_error = str(exc)
                    lowered = last_error.lower()
                    if "already exists" in lowered or "already registered" in lowered:
                        existing.add(table_name)
                        registered = True
                        result = {"table_name": table_name, "status": "already_present", "table_location": candidate}
                        results.append(result)
                        print(f"table {table_name}: already_present at {candidate}")
                        break

            if not registered:
                result = {"table_name": table_name, "status": "error", "table_location": table_location, "error": last_error}
                results.append(result)
                print(f"table {table_name}: error at {table_location}: {last_error}")
                logger.error("Could not register delta.bronze.%s at %s: %s", table_name, table_location, last_error)

        errors = [result for result in results if result["status"] == "error"]
        if errors:
            raise RuntimeError(f"Failed to register {len(errors)} Bronze table(s)")
        return results
    finally:
        cursor.close()
        connection.close()


def main() -> None:
    register_bronze_tables()


if __name__ == "__main__":
    main()
