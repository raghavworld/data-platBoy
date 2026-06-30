from __future__ import annotations

import os

from common import s3_client, setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def silver_object_count() -> int:
    client = s3_client()
    bucket_name = os.environ["MINIO_BUCKET_DELTA"]
    count = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix="silver/"):
        count += len(page.get("Contents", []))
    return count


def main() -> None:
    logger = setup_logging("validate_silver_tables")
    init_dashboard_db()

    buckets = {bucket["Name"] for bucket in s3_client().list_buckets().get("Buckets", [])}
    assert_ok(os.environ["MINIO_BUCKET_DELTA"] in buckets, "Delta bucket is missing")
    assert_ok(silver_object_count() > 0, "No Silver Delta objects found under edp-delta/silver/")

    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM silver_collection_states")
            table_count = int(cursor.fetchone()[0])
            cursor.execute("SELECT count(*) FROM silver_schema_snapshots")
            schema_count = int(cursor.fetchone()[0])
            cursor.execute("SELECT count(*) FROM silver_batch_fingerprints WHERE status = 'success'")
            batch_count = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT
                    COALESCE(sum(row_count) FILTER (WHERE is_child_table = false), 0) AS parent_rows,
                    COALESCE(sum(row_count) FILTER (WHERE is_child_table = true), 0) AS child_rows,
                    COALESCE(sum(row_count), 0) AS total_normalized_rows
                FROM silver_collection_states
                """
            )
            parent_rows, child_rows, row_count = [int(value or 0) for value in cursor.fetchone()]
            cursor.execute("SELECT count(*) FROM silver_quality_metrics")
            quality_count = int(cursor.fetchone()[0])

    assert_ok(table_count > 0, "silver_collection_states has no table entries")
    assert_ok(schema_count > 0, "silver_schema_snapshots has no entries")
    assert_ok(batch_count > 0, "silver_batch_fingerprints has no success entries")
    assert_ok(row_count > 0, "Silver tables have no normalized rows")
    assert_ok(quality_count > 0, "silver_quality_metrics has no entries")
    logger.info(
        "Silver validation passed: tables=%s schemas=%s batches=%s parent_rows=%s child_rows=%s normalized_rows=%s quality_metrics=%s",
        table_count,
        schema_count,
        batch_count,
        parent_rows,
        child_rows,
        row_count,
        quality_count,
    )


if __name__ == "__main__":
    main()
