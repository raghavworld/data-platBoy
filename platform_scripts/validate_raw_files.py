from __future__ import annotations

import gzip
import json
import os

from common import s3_client, setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db


RAW_VALIDATION_OBJECT_LIMIT = int(os.environ.get("RAW_VALIDATION_OBJECT_LIMIT", "25"))
RAW_RECONCILE_OBJECT_LIMIT = int(os.environ.get("RAW_RECONCILE_OBJECT_LIMIT", "5000"))


def count_valid_json_rows(client, bucket_name: str, object_key: str) -> int:
    response = client.get_object(Bucket=bucket_name, Key=object_key)
    rows = 0
    with gzip.GzipFile(fileobj=response["Body"], mode="rb") as gz_file:
        for line_number, raw_line in enumerate(gz_file, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                json.loads(line.decode("utf-8"))
            except Exception as exc:
                raise RuntimeError(f"{object_key} has invalid JSON at line {line_number}: {exc}") from exc
            rows += 1
    return rows


def main() -> None:
    logger = setup_logging("validate_raw_files")
    init_dashboard_db()
    bucket_name = os.environ["MINIO_BUCKET_RAW"]
    client = s3_client()
    buckets = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if bucket_name not in buckets:
        raise RuntimeError(f"Raw bucket missing: {bucket_name}")

    response = client.list_objects_v2(Bucket=bucket_name, Prefix="python/")
    if response.get("KeyCount", 0) == 0:
        raise RuntimeError("No raw objects exist under edp-raw/python/")

    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM raw_files")
            raw_file_count = int(cursor.fetchone()[0])
            cursor.execute("SELECT count(*) FROM raw_collection_states")
            state_count = int(cursor.fetchone()[0])
            cursor.execute("SELECT count(*) FROM raw_schema_snapshots")
            schema_count = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT minio_bucket, object_key, row_count, file_size_bytes
                FROM raw_files
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (RAW_RECONCILE_OBJECT_LIMIT,),
            )
            raw_rows = [
                {
                    "bucket": row[0] or bucket_name,
                    "object_key": row[1],
                    "row_count": int(row[2] or 0),
                    "file_size_bytes": int(row[3] or 0),
                }
                for row in cursor.fetchall()
            ]

    if raw_file_count == 0:
        raise RuntimeError("raw_files table is empty")
    if state_count == 0:
        raise RuntimeError("raw_collection_states table is empty")
    if schema_count == 0:
        raise RuntimeError("raw_schema_snapshots table is empty")

    metadata_keys = {(row["bucket"], row["object_key"]) for row in raw_rows}
    minio_keys: dict[tuple[str, str], int] = {}
    paginator = client.get_paginator("list_objects_v2")
    listed = 0
    for page in paginator.paginate(Bucket=bucket_name, Prefix="python/"):
        for item in page.get("Contents", []):
            minio_keys[(bucket_name, item["Key"])] = int(item.get("Size") or 0)
            listed += 1
            if listed >= RAW_RECONCILE_OBJECT_LIMIT:
                break
        if listed >= RAW_RECONCILE_OBJECT_LIMIT:
            break

    missing = sorted(key for key in metadata_keys if key[0] == bucket_name and key not in minio_keys)
    if missing:
        raise RuntimeError(f"{len(missing)} raw_files metadata object(s) are missing from MinIO; first missing={missing[0][1]}")

    orphans = sorted(key for key in minio_keys if key not in metadata_keys)
    if orphans:
        logger.warning("Found %s MinIO RAW object(s) without raw_files metadata; first=%s", len(orphans), orphans[0][1])

    for row in raw_rows[:RAW_VALIDATION_OBJECT_LIMIT]:
        head = client.head_object(Bucket=row["bucket"], Key=row["object_key"])
        actual_size = int(head.get("ContentLength") or 0)
        if actual_size != row["file_size_bytes"]:
            raise RuntimeError(
                f"{row['object_key']} size mismatch: metadata={row['file_size_bytes']} minio={actual_size}"
            )
        actual_rows = count_valid_json_rows(client, row["bucket"], row["object_key"])
        if actual_rows != row["row_count"]:
            raise RuntimeError(
                f"{row['object_key']} row_count mismatch: metadata={row['row_count']} json_rows={actual_rows}"
            )

    logger.info(
        "Raw validation succeeded: raw_files=%s states=%s schema_snapshots=%s sampled_objects=%s orphan_objects=%s",
        raw_file_count,
        state_count,
        schema_count,
        min(len(raw_rows), RAW_VALIDATION_OBJECT_LIMIT),
        len(orphans),
    )


if __name__ == "__main__":
    main()
