from __future__ import annotations

import os
from typing import Any

from psycopg2.extras import DictCursor

from common import bronze_table_name, s3_client, setup_logging, trino_connection
from dashboard_db import as_dict, dashboard_connection, init_dashboard_db


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def parse_s3_table_path(table_path: str | None) -> tuple[str, str]:
    if not table_path:
        raise ValueError("Bronze table path is empty")
    normalized = table_path
    for scheme in ("s3a://", "s3://"):
        if normalized.startswith(scheme):
            normalized = normalized[len(scheme):]
            break
    bucket_name, separator, prefix = normalized.partition("/")
    if not bucket_name or not separator:
        raise ValueError(f"Invalid Bronze table path: {table_path}")
    return bucket_name, prefix.rstrip("/") + "/"


def prefix_stats(bucket_name: str, prefix: str) -> dict[str, int]:
    client = s3_client()
    object_count = 0
    delta_log_objects = 0
    parquet_objects = 0
    storage_bytes = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            object_count += 1
            storage_bytes += int(item.get("Size") or 0)
            if "/_delta_log/" in key:
                delta_log_objects += 1
            if key.endswith(".parquet") and "/_delta_log/" not in key:
                parquet_objects += 1
    return {
        "object_count": object_count,
        "delta_log_objects": delta_log_objects,
        "parquet_objects": parquet_objects,
        "storage_bytes": storage_bytes,
    }


def bronze_object_count() -> int:
    return prefix_stats(os.environ["MINIO_BUCKET_DELTA"], "bronze/")["object_count"]


def scoped_run(cursor) -> dict[str, Any] | None:
    dashboard_run_id = os.environ.get("BRONZE_DASHBOARD_RUN_ID") or ""
    airflow_run_id = os.environ.get("AIRFLOW_BRONZE_RUN_ID") or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or ""
    if not dashboard_run_id and not airflow_run_id:
        return None
    cursor.execute(
        """
        SELECT *
        FROM bronze_processing_runs
        WHERE (%s <> '' AND id::text = %s)
           OR (%s <> '' AND airflow_run_id = %s)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (dashboard_run_id, dashboard_run_id, airflow_run_id, airflow_run_id),
    )
    row = cursor.fetchone()
    return as_dict(row) if row else None


def scoped_collection_clause(run: dict[str, Any] | None) -> tuple[str, list[Any]]:
    source_id = str(os.environ.get("BRONZE_SOURCE_ID") or (run or {}).get("source_id") or "").strip()
    database_name = (
        os.environ.get("BRONZE_DATABASE_NAME")
        or os.environ.get("SILVER_DATABASE_NAME")
        or (run or {}).get("database_name")
        or ""
    ).strip()
    collection_name = (
        os.environ.get("BRONZE_COLLECTION_NAME")
        or os.environ.get("SILVER_COLLECTION_NAME")
        or (run or {}).get("collection_name")
        or ""
    ).strip()
    clauses: list[str] = []
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
    return (f"WHERE {' AND '.join(clauses)}" if clauses else "", params)


def successful_file_states(cursor, run: dict[str, Any] | None) -> list[dict[str, Any]]:
    raw_file_id = (os.environ.get("BRONZE_RAW_FILE_ID") or (run or {}).get("raw_file_id") or "").strip()
    raw_object_key = (os.environ.get("BRONZE_RAW_OBJECT_KEY") or (run or {}).get("raw_object_key") or "").strip()
    clauses = ["status = 'success'"]
    params: list[Any] = []
    if run and run.get("id"):
        clauses.append("bronze_run_id = %s")
        params.append(run["id"])
    where, scope_params = scoped_collection_clause(run)
    if where:
        clauses.extend(where.removeprefix("WHERE ").split(" AND "))
        params.extend(scope_params)
    if raw_file_id:
        clauses.append("raw_file_id::text = %s")
        params.append(raw_file_id)
    if raw_object_key:
        clauses.append("raw_object_key = %s")
        params.append(raw_object_key)
    cursor.execute(
        f"""
        SELECT *
        FROM bronze_file_states
        WHERE {' AND '.join(clauses)}
        ORDER BY finished_at DESC NULLS LAST, updated_at DESC
        """,
        params,
    )
    return [as_dict(row) for row in cursor.fetchall()]


def collection_states(cursor, run: dict[str, Any] | None, success_files: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if run and success_files:
        keys = sorted(
            {
                (
                    str(item.get("source_id") or ""),
                    str(item.get("database_name") or ""),
                    str(item.get("collection_name") or ""),
                )
                for item in success_files
                if item.get("database_name") and item.get("collection_name")
            }
        )
        clauses: list[str] = []
        params: list[Any] = []
        for source_id, database_name, collection_name in keys:
            if source_id:
                clauses.append("(source_id::text = %s AND database_name = %s AND collection_name = %s)")
                params.extend([source_id, database_name, collection_name])
            else:
                clauses.append("(source_id IS NULL AND database_name = %s AND collection_name = %s)")
                params.extend([database_name, collection_name])
        if clauses:
            cursor.execute(
                f"""
                SELECT *
                FROM bronze_collection_states
                WHERE {' OR '.join(clauses)}
                ORDER BY updated_at DESC
                """,
                params,
            )
            rows = [as_dict(row) for row in cursor.fetchall()]
            if rows:
                return rows

    where, params = scoped_collection_clause(run)
    cursor.execute(
        f"""
        SELECT *
        FROM bronze_collection_states
        {where}
        ORDER BY updated_at DESC
        """,
        params,
    )
    return [as_dict(row) for row in cursor.fetchall()]


def trino_rows(table_name: str) -> int | None:
    connection = None
    cursor = None
    try:
        connection = trino_connection(schema="bronze")
        cursor = connection.cursor()
        cursor.execute(f'SELECT count(*) FROM delta.bronze."{table_name}"')
        return int(cursor.fetchone()[0])
    except Exception:
        return None
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def main() -> None:
    logger = setup_logging("validate_bronze_tables")
    init_dashboard_db()
    demo_mode = str(os.environ.get("BRONZE_DEMO_MODE") or "").strip().lower() in {"1", "true", "yes"}

    buckets = {bucket["Name"] for bucket in s3_client().list_buckets().get("Buckets", [])}
    assert_ok(os.environ["MINIO_BUCKET_DELTA"] in buckets, "Delta bucket is missing")

    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            run = scoped_run(cursor)
            if run:
                logger.info("Validating Bronze run %s status=%s scope=%s", run.get("airflow_run_id"), run.get("status"), run.get("scope"))
                assert_ok(run.get("status") in {"success", "no_new_data", "running"}, f"Bronze run is not validatable: {run}")
                if int(run.get("total_raw_files_found") or run.get("total_raw_files") or 0) == 0 and run.get("status") == "no_new_data":
                    logger.info("Bronze scoped run had no RAW files to process; validation passed for no_new_data run")
                    return

            success_files = successful_file_states(cursor, run)
            tables = collection_states(cursor, run, success_files)
            if tables:
                schema_clauses = []
                schema_params: list[Any] = []
                for table in tables:
                    source_id = str(table.get("source_id") or "")
                    if source_id:
                        schema_clauses.append("(source_id::text = %s AND database_name = %s AND collection_name = %s)")
                        schema_params.extend([source_id, table["database_name"], table["collection_name"]])
                    else:
                        schema_clauses.append("(source_id IS NULL AND database_name = %s AND collection_name = %s)")
                        schema_params.extend([table["database_name"], table["collection_name"]])
                cursor.execute(
                    f"SELECT count(*) FROM bronze_schema_snapshots WHERE {' OR '.join(schema_clauses)}",
                    schema_params,
                )
            else:
                cursor.execute("SELECT count(*) FROM bronze_schema_snapshots")
            schema_count = int(cursor.fetchone()[0])

    assert_ok(bronze_object_count() > 0, "No Bronze Delta objects found under edp-delta/bronze/")
    assert_ok(success_files, "No successful Bronze file states found for validation scope")
    assert_ok(tables, "No Bronze collection states found for validation scope")
    assert_ok(schema_count > 0, "bronze_schema_snapshots has no entries")

    checked_paths: set[str] = set()
    total_rows_written = 0
    for state in success_files:
        raw_rows = int(state.get("raw_row_count") or 0)
        rows_written = int(state.get("rows_written") or 0)
        total_rows_written += rows_written
        assert_ok(state.get("bronze_table_path"), f"Successful Bronze file state has no table path: {state.get('raw_object_key')}")
        assert_ok(rows_written <= raw_rows or raw_rows == 0, f"Bronze rows exceed RAW rows for {state.get('raw_object_key')}")

    for table in tables:
        table_path = table.get("bronze_table_path")
        bucket_name, prefix = parse_s3_table_path(table_path)
        stats = prefix_stats(bucket_name, prefix)
        checked_paths.add(prefix)
        assert_ok(stats["object_count"] > 0, f"Bronze table path has no objects: {table_path}")
        assert_ok(stats["delta_log_objects"] > 0, f"Bronze table path has no Delta transaction log: {table_path}")
        assert_ok(stats["parquet_objects"] > 0, f"Bronze table path has no Parquet data files: {table_path}")
        table_name = table.get("trino_table_name") or bronze_table_name(table["database_name"], table["collection_name"])
        if demo_mode:
            logger.info("Demo scoped Bronze validation skipped Trino row count for delta.bronze.%s", table_name)
            continue
        row_count = trino_rows(table_name)
        assert_ok(row_count is not None, f"Bronze table is not readable through Trino/Hive metastore: delta.bronze.{table_name}")
        metadata_rows = int(table.get("total_rows_written") or 0)
        assert_ok(row_count >= 0, f"Invalid Trino row count for delta.bronze.{table_name}")
        if metadata_rows:
            assert_ok(row_count <= metadata_rows or row_count >= total_rows_written, f"Trino row count looks inconsistent for delta.bronze.{table_name}")

    logger.info(
        "Bronze validation passed: files=%s tables=%s schemas=%s rows_written=%s paths=%s",
        len(success_files),
        len(tables),
        schema_count,
        total_rows_written,
        len(checked_paths),
    )


if __name__ == "__main__":
    main()
