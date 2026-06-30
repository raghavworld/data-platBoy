from __future__ import annotations

import json
import os
import random
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import DictCursor, Json

from common import load_environment


DASHBOARD_SCHEMA_LOCK_ID = 80260801
BRONZE_METADATA_DEADLOCK_RETRIES = 3
BRONZE_DEADLOCK_SQLSTATE = "40P01"
_DASHBOARD_DB_INITIALIZED = False


def dashboard_db_config() -> dict[str, Any]:
    load_environment()
    return {
        "host": os.environ.get("DASHBOARD_POSTGRES_HOST", "dashboard-postgres"),
        "port": int(os.environ.get("DASHBOARD_POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("DASHBOARD_POSTGRES_DB", "onov8_dashboard"),
        "user": os.environ.get("DASHBOARD_POSTGRES_USER", "dashboard"),
        "password": os.environ.get("DASHBOARD_POSTGRES_PASSWORD", "dashboard"),
        "connect_timeout": int(os.environ.get("DASHBOARD_POSTGRES_CONNECT_TIMEOUT", "10")),
    }


@contextmanager
def dashboard_connection() -> Iterator[psycopg2.extensions.connection]:
    connection = psycopg2.connect(**dashboard_db_config())
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


@contextmanager
def dashboard_schema_lock() -> Iterator[None]:
    connection = psycopg2.connect(**dashboard_db_config())
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", (DASHBOARD_SCHEMA_LOCK_ID,))
        yield
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (DASHBOARD_SCHEMA_LOCK_ID,))
        finally:
            connection.close()


def as_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def new_id() -> str:
    return str(uuid.uuid4())


def json_param(value: Any) -> Json:
    return Json(value if value is not None else {})


def is_deadlock_error(exc: Exception) -> bool:
    if isinstance(exc, psycopg2.errors.DeadlockDetected):
        return True
    return getattr(exc, "pgcode", None) == BRONZE_DEADLOCK_SQLSTATE or "deadlock detected" in str(exc).lower()


def with_bronze_metadata_retry(operation: str, callback):
    for attempt in range(BRONZE_METADATA_DEADLOCK_RETRIES + 1):
        try:
            return callback()
        except Exception as exc:
            if not is_deadlock_error(exc) or attempt >= BRONZE_METADATA_DEADLOCK_RETRIES:
                raise
            delay = min(2.0, 0.2 * (2**attempt)) + random.uniform(0, 0.15)
            time.sleep(delay)

    raise RuntimeError(f"Bronze metadata operation {operation} exhausted deadlock retries")


def init_dashboard_db() -> None:
    global _DASHBOARD_DB_INITIALIZED
    if _DASHBOARD_DB_INITIALIZED:
        return
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (DASHBOARD_SCHEMA_LOCK_ID,))
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS source_connections (
                    id UUID PRIMARY KEY,
                    source_name TEXT NOT NULL UNIQUE,
                    source_type TEXT NOT NULL DEFAULT 'mongo',
                    database_name TEXT NOT NULL,
                    auth_database TEXT DEFAULT 'admin',
                    connection_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    secret_reference TEXT,
                    include_collections_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    exclude_collections_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    cursor_field TEXT NOT NULL DEFAULT 'AUTO',
                    ingestion_mode TEXT NOT NULL DEFAULT 'python',
                    is_active BOOLEAN NOT NULL DEFAULT false,
                    ingestion_schedule_type TEXT NOT NULL DEFAULT 'manual_only',
                    ingestion_schedule_cron TEXT,
                    last_scheduled_run_at TIMESTAMPTZ,
                    next_scheduled_run_at TIMESTAMPTZ,
                    schedule_enabled BOOLEAN NOT NULL DEFAULT false,
                    last_test_status TEXT,
                    last_test_message TEXT,
                    last_test_at TIMESTAMPTZ,
                    last_inventory_status TEXT,
                    last_inventory_message TEXT,
                    last_inventory_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS source_connection_collections (
                    id UUID PRIMARY KEY,
                    source_id UUID REFERENCES source_connections(id) ON DELETE CASCADE,
                    collection_name TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT false,
                    record_count BIGINT NOT NULL DEFAULT 0,
                    estimated_size_bytes BIGINT NOT NULL DEFAULT 0,
                    avg_object_size DOUBLE PRECISION,
                    last_stats_at TIMESTAMPTZ,
                    detected_cursor_field TEXT,
                    detected_cursor_strategy TEXT,
                    sample_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    last_error TEXT,
                    ingestion_schedule_type TEXT NOT NULL DEFAULT 'manual_only',
                    ingestion_schedule_cron TEXT,
                    last_scheduled_run_at TIMESTAMPTZ,
                    next_scheduled_run_at TIMESTAMPTZ,
                    schedule_enabled BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(source_id, collection_name)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_ingestion_runs (
                    id UUID PRIMARY KEY,
                    airflow_run_id TEXT UNIQUE,
                    status TEXT NOT NULL,
                    queued_at TIMESTAMPTZ,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    total_rows BIGINT NOT NULL DEFAULT 0,
                    total_files BIGINT NOT NULL DEFAULT 0,
                    total_rows_found BIGINT NOT NULL DEFAULT 0,
                    total_rows_written BIGINT NOT NULL DEFAULT 0,
                    no_new_data_collections BIGINT NOT NULL DEFAULT 0,
                    duplicate_batches_skipped BIGINT NOT NULL DEFAULT 0,
                    progress_phase TEXT NOT NULL DEFAULT 'queued',
                    progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_databases BIGINT NOT NULL DEFAULT 0,
                    completed_databases BIGINT NOT NULL DEFAULT 0,
                    total_collections BIGINT NOT NULL DEFAULT 0,
                    completed_collections BIGINT NOT NULL DEFAULT 0,
                    total_estimated_records BIGINT NOT NULL DEFAULT 0,
                    processed_records BIGINT NOT NULL DEFAULT 0,
                    failed_collections BIGINT NOT NULL DEFAULT 0,
                    total_batches BIGINT NOT NULL DEFAULT 0,
                    completed_batches BIGINT NOT NULL DEFAULT 0,
                    failed_batches BIGINT NOT NULL DEFAULT 0,
                    skipped_batches BIGINT NOT NULL DEFAULT 0,
                    current_batch_number BIGINT,
                    raw_batch_size BIGINT NOT NULL DEFAULT 1000,
                    raw_parallel_collections BIGINT NOT NULL DEFAULT 1,
                    raw_parallel_batches BIGINT NOT NULL DEFAULT 1,
                    current_database TEXT,
                    current_collection TEXT,
                    progress_started_at TIMESTAMPTZ,
                    progress_updated_at TIMESTAMPTZ,
                    estimated_completion_at TIMESTAMPTZ,
                    estimated_remaining_seconds DOUBLE PRECISION,
                    progress_message TEXT,
                    progress_details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    target_source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    target_collection_name TEXT,
                    retry_of_run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE SET NULL,
                    retry_reason TEXT,
                    triggered_by TEXT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_run_events (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    database_name TEXT,
                    collection_name TEXT,
                    batch_id UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_run_timings (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    level TEXT NOT NULL,
                    database_name TEXT,
                    collection_name TEXT,
                    batch_id UUID,
                    source_connect_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    query_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    file_write_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    metadata_update_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    warning_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_run_database_progress (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    total_collections BIGINT NOT NULL DEFAULT 0,
                    completed_collections BIGINT NOT NULL DEFAULT 0,
                    estimated_records BIGINT NOT NULL DEFAULT 0,
                    processed_records BIGINT NOT NULL DEFAULT 0,
                    current_collection TEXT,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    error_message TEXT,
                    UNIQUE(run_id, source_id, database_name)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_ingestion_batches (
                    batch_id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    batch_number BIGINT NOT NULL,
                    total_batches BIGINT NOT NULL DEFAULT 0,
                    cursor_start TEXT,
                    cursor_end TEXT,
                    estimated_rows BIGINT NOT NULL DEFAULT 0,
                    actual_rows BIGINT NOT NULL DEFAULT 0,
                    processed_rows BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'queued',
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    error_type TEXT,
                    error_message TEXT,
                    retryable BOOLEAN NOT NULL DEFAULT true,
                    recommended_fix TEXT,
                    retry_count BIGINT NOT NULL DEFAULT 0,
                    raw_object_key TEXT,
                    cursor_strategy TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(run_id, source_id, database_name, collection_name, batch_number)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_collection_states (
                    id UUID PRIMARY KEY,
                    source_id UUID REFERENCES source_connections(id) ON DELETE CASCADE,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    cursor_field TEXT NOT NULL,
                    configured_cursor_field TEXT,
                    detected_cursor_field TEXT,
                    ingestion_strategy TEXT NOT NULL DEFAULT 'incremental_timestamp',
                    cursor_warning TEXT,
                    last_cursor_value TEXT,
                    last_snapshot_fingerprint TEXT,
                    last_success_at TIMESTAMPTZ,
                    last_row_count BIGINT NOT NULL DEFAULT 0,
                    last_raw_path TEXT,
                    latest_source_document_at TIMESTAMPTZ,
                    estimated_ingestion_lag_seconds DOUBLE PRECISION,
                    records_since_last_run BIGINT NOT NULL DEFAULT 0,
                    freshness_status TEXT,
                    freshness_checked_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(source_id, database_name, collection_name)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_schema_snapshots (
                    id UUID PRIMARY KEY,
                    source_id UUID REFERENCES source_connections(id) ON DELETE CASCADE,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    schema_hash TEXT NOT NULL,
                    fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    new_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    removed_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    change_type TEXT NOT NULL,
                    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_files (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE SET NULL,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    minio_bucket TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    row_count BIGINT NOT NULL DEFAULT 0,
                    file_size_bytes BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(minio_bucket, object_key)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_batch_fingerprints (
                    id UUID PRIMARY KEY,
                    source_id UUID REFERENCES source_connections(id) ON DELETE CASCADE,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    cursor_field TEXT NOT NULL,
                    min_cursor_value TEXT NOT NULL,
                    max_cursor_value TEXT NOT NULL,
                    row_count BIGINT NOT NULL,
                    batch_checksum TEXT NOT NULL,
                    raw_object_key TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_collection_run_statuses (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    rows_found BIGINT NOT NULL DEFAULT 0,
                    rows_written BIGINT NOT NULL DEFAULT 0,
                    raw_object_key TEXT,
                    previous_cursor_value TEXT,
                    new_cursor_value TEXT,
                    cursor_strategy TEXT,
                    error_type TEXT,
                    message TEXT,
                    recommended_fix TEXT,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_maintenance_events (
                    id UUID PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_processing_runs (
                    id UUID PRIMARY KEY,
                    airflow_run_id TEXT UNIQUE,
                    status TEXT NOT NULL,
                    scope TEXT NOT NULL DEFAULT 'all_pending',
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT,
                    collection_name TEXT,
                    raw_file_id UUID,
                    raw_object_key TEXT,
                    retry_failed BOOLEAN NOT NULL DEFAULT false,
                    current_database_name TEXT,
                    current_collection_name TEXT,
                    current_raw_file_id UUID,
                    current_raw_object_key TEXT,
                    current_phase TEXT NOT NULL DEFAULT 'queued',
                    total_databases BIGINT NOT NULL DEFAULT 0,
                    completed_databases BIGINT NOT NULL DEFAULT 0,
                    total_collections BIGINT NOT NULL DEFAULT 0,
                    completed_collections BIGINT NOT NULL DEFAULT 0,
                    total_raw_files BIGINT NOT NULL DEFAULT 0,
                    completed_raw_files BIGINT NOT NULL DEFAULT 0,
                    total_estimated_rows BIGINT NOT NULL DEFAULT 0,
                    processed_rows BIGINT NOT NULL DEFAULT 0,
                    read_raw_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    parse_json_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    add_audit_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    write_delta_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    metadata_update_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    validation_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_timing_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    cancel_requested BOOLEAN NOT NULL DEFAULT false,
                    progress_updated_at TIMESTAMPTZ,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    total_raw_files_found BIGINT NOT NULL DEFAULT 0,
                    total_files_processed BIGINT NOT NULL DEFAULT 0,
                    total_files_skipped BIGINT NOT NULL DEFAULT 0,
                    total_rows_written BIGINT NOT NULL DEFAULT 0,
                    failed_files BIGINT NOT NULL DEFAULT 0,
                    triggered_by TEXT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_run_events (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES bronze_processing_runs(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    database_name TEXT,
                    collection_name TEXT,
                    raw_file_id UUID,
                    raw_object_key TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_file_states (
                    id UUID PRIMARY KEY,
                    bronze_run_id UUID REFERENCES bronze_processing_runs(id) ON DELETE SET NULL,
                    raw_file_id UUID REFERENCES raw_files(id) ON DELETE SET NULL,
                    raw_object_key TEXT NOT NULL,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    raw_row_count BIGINT NOT NULL DEFAULT 0,
                    raw_file_size_bytes BIGINT NOT NULL DEFAULT 0,
                    raw_file_checksum TEXT,
                    status TEXT NOT NULL,
                    bronze_table_path TEXT,
                    rows_written BIGINT NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    failed_step TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    stack_trace_summary TEXT,
                    retryable BOOLEAN NOT NULL DEFAULT false,
                    recommended_fix TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(raw_file_id),
                    UNIQUE(raw_object_key)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_file_attempts (
                    id UUID PRIMARY KEY,
                    bronze_run_id UUID REFERENCES bronze_processing_runs(id) ON DELETE SET NULL,
                    raw_file_id UUID REFERENCES raw_files(id) ON DELETE SET NULL,
                    raw_object_key TEXT NOT NULL,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    raw_row_count BIGINT NOT NULL DEFAULT 0,
                    raw_file_size_bytes BIGINT NOT NULL DEFAULT 0,
                    raw_file_checksum TEXT,
                    status TEXT NOT NULL,
                    bronze_table_path TEXT,
                    rows_written BIGINT NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    failed_step TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    stack_trace_summary TEXT,
                    retryable BOOLEAN NOT NULL DEFAULT false,
                    recommended_fix TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_schema_snapshots (
                    id UUID PRIMARY KEY,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    bronze_table_path TEXT NOT NULL,
                    schema_hash TEXT NOT NULL,
                    fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    new_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    removed_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    change_type TEXT NOT NULL,
                    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_column_mappings (
                    id UUID PRIMARY KEY,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    raw_object_key TEXT,
                    raw_field_path TEXT NOT NULL,
                    bronze_field_path TEXT NOT NULL,
                    reason TEXT,
                    mapping_version TEXT,
                    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_collection_states (
                    id UUID PRIMARY KEY,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    bronze_table_path TEXT NOT NULL,
                    trino_table_name TEXT,
                    last_success_at TIMESTAMPTZ,
                    last_rows_written BIGINT NOT NULL DEFAULT 0,
                    total_rows_written BIGINT NOT NULL DEFAULT 0,
                    last_schema_hash TEXT,
                    last_processed_raw_object_key TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(source_id, database_name, collection_name)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_maintenance_events (
                    id UUID PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_processing_runs (
                    id UUID PRIMARY KEY,
                    airflow_run_id TEXT UNIQUE,
                    status TEXT NOT NULL,
                    scope TEXT NOT NULL DEFAULT 'all_pending',
                    database_name TEXT,
                    collection_name TEXT,
                    bronze_table TEXT,
                    bronze_file TEXT,
                    retry_failed BOOLEAN NOT NULL DEFAULT false,
                    current_database_name TEXT,
                    current_collection_name TEXT,
                    current_bronze_table TEXT,
                    current_bronze_file TEXT,
                    current_phase TEXT NOT NULL DEFAULT 'queued',
                    total_databases BIGINT NOT NULL DEFAULT 0,
                    completed_databases BIGINT NOT NULL DEFAULT 0,
                    total_collections BIGINT NOT NULL DEFAULT 0,
                    completed_collections BIGINT NOT NULL DEFAULT 0,
                    total_bronze_tables BIGINT NOT NULL DEFAULT 0,
                    completed_bronze_tables BIGINT NOT NULL DEFAULT 0,
                    total_batches BIGINT NOT NULL DEFAULT 0,
                    completed_batches BIGINT NOT NULL DEFAULT 0,
                    processed_rows BIGINT NOT NULL DEFAULT 0,
                    bronze_read_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    flattening_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    child_table_generation_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    delta_write_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    metadata_update_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    profiling_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_timing_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    cancel_requested BOOLEAN NOT NULL DEFAULT false,
                    progress_updated_at TIMESTAMPTZ,
                    silver_batch_size BIGINT NOT NULL DEFAULT 10,
                    silver_parallel_collections BIGINT NOT NULL DEFAULT 1,
                    silver_parallel_tables BIGINT NOT NULL DEFAULT 1,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    total_bronze_batches_found BIGINT NOT NULL DEFAULT 0,
                    total_batches_processed BIGINT NOT NULL DEFAULT 0,
                    total_batches_skipped BIGINT NOT NULL DEFAULT 0,
                    total_rows_written BIGINT NOT NULL DEFAULT 0,
                    failed_batches BIGINT NOT NULL DEFAULT 0,
                    processed_tables BIGINT NOT NULL DEFAULT 0,
                    failed_tables BIGINT NOT NULL DEFAULT 0,
                    failed_table_names_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    triggered_by TEXT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_run_events (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES silver_processing_runs(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    database_name TEXT,
                    collection_name TEXT,
                    bronze_table TEXT,
                    bronze_file TEXT,
                    batch_id UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_processing_batches (
                    batch_id UUID PRIMARY KEY,
                    run_id UUID REFERENCES silver_processing_runs(id) ON DELETE CASCADE,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    bronze_table TEXT,
                    bronze_file TEXT,
                    batch_number BIGINT NOT NULL,
                    total_batches BIGINT NOT NULL DEFAULT 0,
                    rows_processed BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'queued',
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    retry_count BIGINT NOT NULL DEFAULT 0,
                    failed_step TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    retryable BOOLEAN NOT NULL DEFAULT true,
                    recommended_fix TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(run_id, bronze_table, bronze_file, batch_number)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_collection_states (
                    id UUID PRIMARY KEY,
                    silver_table_name TEXT NOT NULL UNIQUE,
                    source_database TEXT,
                    source_collection TEXT,
                    silver_table_path TEXT NOT NULL,
                    trino_table_name TEXT,
                    primary_key_column TEXT,
                    row_count BIGINT NOT NULL DEFAULT 0,
                    last_rows_written BIGINT NOT NULL DEFAULT 0,
                    total_rows_written BIGINT NOT NULL DEFAULT 0,
                    last_success_at TIMESTAMPTZ,
                    last_schema_hash TEXT,
                    flattened_field_count BIGINT NOT NULL DEFAULT 0,
                    partition_info TEXT,
                    last_processed_bronze_batch_id TEXT,
                    is_child_table BOOLEAN NOT NULL DEFAULT false,
                    parent_silver_table_name TEXT,
                    child_path TEXT,
                    table_classification TEXT NOT NULL DEFAULT 'analytics_ready',
                    bi_suitability TEXT NOT NULL DEFAULT 'unknown',
                    governance_status TEXT NOT NULL DEFAULT 'unknown',
                    lineage_reference TEXT,
                    transform_strategy TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_batch_fingerprints (
                    id UUID PRIMARY KEY,
                    silver_table_name TEXT NOT NULL,
                    source_database TEXT NOT NULL,
                    source_collection TEXT NOT NULL,
                    bronze_batch_id TEXT NOT NULL,
                    bronze_raw_object_key TEXT,
                    input_row_count BIGINT NOT NULL DEFAULT 0,
                    input_checksum TEXT,
                    status TEXT NOT NULL,
                    rows_written BIGINT NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    error_message TEXT,
                    error_type TEXT,
                    failed_step TEXT,
                    recommended_fix TEXT,
                    full_stack_trace TEXT,
                    possibly_corrupt BOOLEAN NOT NULL DEFAULT false,
                    retry_count BIGINT NOT NULL DEFAULT 0,
                    retryable BOOLEAN NOT NULL DEFAULT true,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(silver_table_name, bronze_batch_id)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_schema_snapshots (
                    id UUID PRIMARY KEY,
                    silver_table_name TEXT NOT NULL,
                    schema_hash TEXT NOT NULL,
                    fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    new_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    removed_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    change_type TEXT NOT NULL DEFAULT 'unchanged',
                    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_quality_metrics (
                    id UUID PRIMARY KEY,
                    silver_table_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value DOUBLE PRECISION NOT NULL DEFAULT 0,
                    severity TEXT NOT NULL DEFAULT 'info',
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    measured_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_transformation_quality (
                    id UUID PRIMARY KEY,
                    silver_table_name TEXT NOT NULL UNIQUE,
                    source_database TEXT,
                    source_collection TEXT,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    severity TEXT NOT NULL DEFAULT 'info',
                    blocking BOOLEAN NOT NULL DEFAULT false,
                    bi_suitability TEXT NOT NULL DEFAULT 'unknown',
                    governance_status TEXT NOT NULL DEFAULT 'unknown',
                    recommendation_type TEXT,
                    table_classification TEXT,
                    flattening_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    sql_usability_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    bi_readiness_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    raw_json_dependency_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    column_safety_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    type_quality_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    pii_safety_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    analytics_readiness_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_columns BIGINT NOT NULL DEFAULT 0,
                    nested_struct_columns BIGINT NOT NULL DEFAULT 0,
                    array_columns BIGINT NOT NULL DEFAULT 0,
                    map_json_columns BIGINT NOT NULL DEFAULT 0,
                    nested_columns_remaining BIGINT NOT NULL DEFAULT 0,
                    primitive_flat_columns BIGINT NOT NULL DEFAULT 0,
                    raw_json_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    invalid_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    duplicate_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    type_warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    pii_warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    child_table_recommendations_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    bronze_comparison_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    failed_reasons_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    warning_reasons_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    recommendations_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    last_validated_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_field_profiles (
                    id UUID PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    source_database TEXT,
                    source_collection TEXT,
                    field_path TEXT NOT NULL,
                    detected_type TEXT NOT NULL DEFAULT 'unknown',
                    occurrence_count BIGINT NOT NULL DEFAULT 0,
                    occurrence_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
                    extracted_as_column BOOLEAN NOT NULL DEFAULT false,
                    extracted_table TEXT,
                    raw_json_fallback BOOLEAN NOT NULL DEFAULT false,
                    flattening_strategy TEXT NOT NULL DEFAULT 'unknown',
                    pii_detected BOOLEAN NOT NULL DEFAULT false,
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    last_profiled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(table_name, field_path)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_transform_plans (
                    id UUID PRIMARY KEY,
                    source_bronze_table TEXT NOT NULL,
                    source_database TEXT NOT NULL,
                    source_collection TEXT NOT NULL,
                    generated_silver_tables_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    extracted_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    generated_child_tables_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    raw_json_fallback_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    ignored_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    reasons_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    pii_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    recommendations_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    complexity_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    estimated_analytics_readiness DOUBLE PRECISION NOT NULL DEFAULT 0,
                    table_classification TEXT NOT NULL DEFAULT 'analytics_ready',
                    status TEXT NOT NULL DEFAULT 'planned',
                    plan_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    last_planned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(source_database, source_collection)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_pii_audit (
                    id UUID PRIMARY KEY,
                    database_name TEXT,
                    collection_name TEXT,
                    silver_table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    field_path TEXT,
                    detected_pii_type TEXT NOT NULL DEFAULT 'unknown',
                    detection_rule TEXT NOT NULL DEFAULT 'column_name_rule',
                    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                    action_taken TEXT NOT NULL DEFAULT 'retained_in_governed_silver',
                    safe_view_name TEXT,
                    safe_view_status TEXT NOT NULL DEFAULT 'missing_safe_view',
                    severity TEXT NOT NULL DEFAULT 'warning',
                    recommendation TEXT,
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE(silver_table_name, column_name, detected_pii_type)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_child_table_audit (
                    id UUID PRIMARY KEY,
                    database_name TEXT,
                    collection_name TEXT,
                    parent_table TEXT NOT NULL,
                    child_table TEXT NOT NULL,
                    source_nested_path TEXT,
                    reason TEXT NOT NULL DEFAULT 'array of objects',
                    parent_key TEXT,
                    child_index_field TEXT NOT NULL DEFAULT 'child_index',
                    rows_generated BIGINT NOT NULL DEFAULT 0,
                    relationship_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                    bi_usefulness TEXT NOT NULL DEFAULT 'unknown',
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE(child_table)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_raw_json_fallback_audit (
                    id UUID PRIMARY KEY,
                    database_name TEXT,
                    collection_name TEXT,
                    silver_table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    original_field_path TEXT,
                    reason TEXT NOT NULL DEFAULT 'rare/dynamic field',
                    needs_custom_transform BOOLEAN NOT NULL DEFAULT false,
                    bi_suitability_impact TEXT NOT NULL DEFAULT 'warning',
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE(silver_table_name, column_name)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_maintenance_events (
                    id UUID PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS query_safe_views (
                    view_name TEXT PRIMARY KEY,
                    source_table TEXT NOT NULL,
                    column_count BIGINT NOT NULL DEFAULT 0,
                    source_column_count BIGINT NOT NULL DEFAULT 0,
                    row_count BIGINT NOT NULL DEFAULT 0,
                    pii_safe BOOLEAN NOT NULL DEFAULT false,
                    blocked_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    hash_version TEXT,
                    trino_visible BOOLEAN NOT NULL DEFAULT false,
                    last_validated_at TIMESTAMPTZ,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    error_message TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS query_validation_runs (
                    id UUID PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    total_checks BIGINT NOT NULL DEFAULT 0,
                    passed_checks BIGINT NOT NULL DEFAULT 0,
                    failed_checks BIGINT NOT NULL DEFAULT 0,
                    checks_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS query_history (
                    query_id TEXT PRIMARY KEY,
                    user_source TEXT NOT NULL DEFAULT 'dashboard',
                    actor TEXT,
                    actor_role TEXT,
                    session_id TEXT,
                    request_id TEXT,
                    client_ip TEXT,
                    sql_text TEXT NOT NULL,
                    query_fingerprint TEXT,
                    selected_view TEXT,
                    selected_view_status TEXT,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    duration_ms BIGINT,
                    row_count BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    bytes_scanned BIGINT,
                    is_slow BOOLEAN NOT NULL DEFAULT false,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bi_datasets (
                    dataset_name TEXT PRIMARY KEY,
                    source_safe_view TEXT NOT NULL,
                    superset_dataset_id BIGINT,
                    row_count BIGINT NOT NULL DEFAULT 0,
                    columns_count BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    last_refreshed TIMESTAMPTZ,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bi_dashboards (
                    dashboard_name TEXT PRIMARY KEY,
                    superset_dashboard_id BIGINT,
                    slug TEXT,
                    url_path TEXT,
                    chart_count BIGINT NOT NULL DEFAULT 0,
                    linked_datasets_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    generated_chart_names_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    generation_time_ms BIGINT,
                    generated_by TEXT,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    refresh_status TEXT NOT NULL DEFAULT 'unknown',
                    last_viewed_at TIMESTAMPTZ,
                    last_refresh_at TIMESTAMPTZ,
                    last_validation_at TIMESTAMPTZ,
                    load_duration_ms BIGINT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bi_charts (
                    chart_name TEXT PRIMARY KEY,
                    superset_chart_id BIGINT,
                    dashboard_name TEXT,
                    dataset_name TEXT NOT NULL,
                    viz_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    generation_time_ms BIGINT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bi_validation_runs (
                    id UUID PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    total_checks BIGINT NOT NULL DEFAULT 0,
                    passed_checks BIGINT NOT NULL DEFAULT 0,
                    failed_checks BIGINT NOT NULL DEFAULT 0,
                    checks_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bi_query_failures (
                    id UUID PRIMARY KEY,
                    dashboard_name TEXT,
                    dataset_name TEXT,
                    chart_name TEXT,
                    status TEXT NOT NULL DEFAULT 'failed',
                    query_text TEXT,
                    load_duration_ms BIGINT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bi_dashboard_loads (
                    id UUID PRIMARY KEY,
                    dashboard_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    load_duration_ms BIGINT,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS service_health_checks (
                    id UUID PRIMARY KEY,
                    service_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS governance_assets (
                    id UUID PRIMARY KEY,
                    asset_name TEXT NOT NULL UNIQUE,
                    asset_type TEXT NOT NULL,
                    layer TEXT NOT NULL,
                    owner TEXT,
                    domain TEXT,
                    pii_status TEXT NOT NULL DEFAULT 'unknown',
                    row_count BIGINT,
                    source_system TEXT,
                    openmetadata_fqn TEXT,
                    openmetadata_url TEXT,
                    last_synced_at TIMESTAMPTZ,
                    status TEXT NOT NULL DEFAULT 'unknown'
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS governance_lineage_edges (
                    id UUID PRIMARY KEY,
                    upstream_asset TEXT NOT NULL,
                    downstream_asset TEXT NOT NULL,
                    relationship_type TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(upstream_asset, downstream_asset, relationship_type)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS governance_pii_tags (
                    id UUID PRIMARY KEY,
                    asset_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    pii_tag TEXT NOT NULL,
                    detection_method TEXT NOT NULL,
                    validated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(asset_name, column_name, pii_tag)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS governance_sync_runs (
                    id UUID PRIMARY KEY,
                    airflow_run_id TEXT UNIQUE,
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    assets_synced BIGINT NOT NULL DEFAULT 0,
                    tags_applied BIGINT NOT NULL DEFAULT 0,
                    lineage_edges_created BIGINT NOT NULL DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS governance_ownership (
                    id UUID PRIMARY KEY,
                    asset_name TEXT NOT NULL UNIQUE,
                    asset_owner TEXT NOT NULL,
                    team TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    status TEXT NOT NULL DEFAULT 'assigned'
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS operations_history (
                    id UUID PRIMARY KEY,
                    operation_type TEXT NOT NULL,
                    target_layer TEXT NOT NULL,
                    triggered_by TEXT NOT NULL DEFAULT 'dashboard',
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    affected_rows BIGINT NOT NULL DEFAULT 0,
                    affected_files BIGINT NOT NULL DEFAULT 0,
                    error_message TEXT,
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS end_to_end_flow_runs (
                    id UUID PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'queued',
                    current_stage TEXT NOT NULL DEFAULT 'source_selected',
                    selected_databases_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    selected_collections_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    selected_raw_files_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    selected_bronze_tables_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    total_collections BIGINT NOT NULL DEFAULT 0,
                    total_records BIGINT NOT NULL DEFAULT 0,
                    total_size_bytes BIGINT NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    error_message TEXT,
                    cancel_requested BOOLEAN NOT NULL DEFAULT false,
                    rebuild_completed_stages BOOLEAN NOT NULL DEFAULT false,
                    linked_raw_run_id UUID,
                    linked_bronze_run_id UUID,
                    linked_silver_run_id UUID,
                    linked_query_validation_id UUID,
                    linked_bi_sync_id UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS end_to_end_flow_steps (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES end_to_end_flow_runs(id) ON DELETE CASCADE,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
                    current_database TEXT,
                    current_collection TEXT,
                    current_item TEXT,
                    rows_processed BIGINT NOT NULL DEFAULT 0,
                    files_processed BIGINT NOT NULL DEFAULT 0,
                    tables_processed BIGINT NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    error_message TEXT,
                    linked_run_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    events_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(run_id, stage)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS end_to_end_flow_items (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES end_to_end_flow_runs(id) ON DELETE CASCADE,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    record_count BIGINT NOT NULL DEFAULT 0,
                    estimated_size_bytes BIGINT NOT NULL DEFAULT 0,
                    cursor_strategy TEXT,
                    raw_file_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    bronze_tables_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    raw_status TEXT NOT NULL DEFAULT 'pending',
                    bronze_status TEXT NOT NULL DEFAULT 'pending',
                    silver_status TEXT NOT NULL DEFAULT 'pending',
                    safe_view_status TEXT NOT NULL DEFAULT 'pending',
                    dataset_status TEXT NOT NULL DEFAULT 'pending',
                    raw_run_id UUID,
                    bronze_run_id UUID,
                    silver_run_id UUID,
                    safe_view_name TEXT,
                    dataset_name TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_events (
                    id UUID PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    severity TEXT NOT NULL DEFAULT 'info',
                    target_layer TEXT,
                    message TEXT NOT NULL,
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_alerts (
                    id UUID PRIMARY KEY,
                    severity TEXT NOT NULL,
                    source TEXT NOT NULL,
                    message TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    resolved_at TIMESTAMPTZ
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id UUID PRIMARY KEY,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_users (
                    username TEXT PRIMARY KEY,
                    display_name TEXT,
                    email TEXT,
                    role TEXT NOT NULL DEFAULT 'viewer',
                    status TEXT NOT NULL DEFAULT 'active',
                    password_hash TEXT NOT NULL,
                    password_changed_at TIMESTAMPTZ,
                    last_login_at TIMESTAMPTZ,
                    last_login_ip TEXT,
                    source TEXT NOT NULL DEFAULT 'local',
                    created_by TEXT NOT NULL DEFAULT 'system',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_teams (
                    id UUID PRIMARY KEY,
                    team_name TEXT NOT NULL UNIQUE,
                    domain TEXT NOT NULL DEFAULT 'platform',
                    owner_username TEXT,
                    description TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_team_memberships (
                    team_id UUID REFERENCES platform_teams(id) ON DELETE CASCADE,
                    username TEXT REFERENCES platform_users(username) ON DELETE CASCADE,
                    role_in_team TEXT NOT NULL DEFAULT 'member',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (team_id, username)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_api_keys (
                    id UUID PRIMARY KEY,
                    key_name TEXT NOT NULL,
                    key_prefix TEXT NOT NULL UNIQUE,
                    key_hash TEXT NOT NULL,
                    owner_username TEXT REFERENCES platform_users(username) ON DELETE SET NULL,
                    role TEXT NOT NULL DEFAULT 'viewer',
                    status TEXT NOT NULL DEFAULT 'active',
                    created_by TEXT NOT NULL DEFAULT 'system',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at TIMESTAMPTZ,
                    last_used_at TIMESTAMPTZ,
                    revoked_at TIMESTAMPTZ
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_settings (
                    setting_key TEXT PRIMARY KEY,
                    category TEXT NOT NULL DEFAULT 'platform',
                    setting_value_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    description TEXT,
                    updated_by TEXT NOT NULL DEFAULT 'system',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id UUID PRIMARY KEY,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ,
                    revoked_at TIMESTAMPTZ,
                    client_label TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS backup_exports (
                    id UUID PRIMARY KEY,
                    backup_type TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_by TEXT NOT NULL DEFAULT 'system',
                    size_bytes BIGINT NOT NULL DEFAULT 0,
                    checksum_sha256 TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS replay_jobs (
                    id UUID PRIMARY KEY,
                    replay_type TEXT NOT NULL,
                    target_layer TEXT NOT NULL,
                    target_ref TEXT,
                    triggered_by TEXT NOT NULL DEFAULT 'dashboard',
                    status TEXT NOT NULL,
                    queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    operation_id UUID REFERENCES operations_history(id) ON DELETE SET NULL,
                    error_message TEXT,
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS service_restart_history (
                    id UUID PRIMARY KEY,
                    service_name TEXT NOT NULL,
                    triggered_by TEXT NOT NULL DEFAULT 'dashboard',
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    health_status TEXT,
                    health_message TEXT,
                    logs_excerpt TEXT,
                    error_message TEXT,
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS global_validation_runs (
                    id UUID PRIMARY KEY,
                    triggered_by TEXT NOT NULL DEFAULT 'dashboard',
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    checks_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS total_rows_found BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS total_rows_written BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS no_new_data_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS duplicate_batches_skipped BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS progress_phase TEXT NOT NULL DEFAULT 'queued'")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS total_databases BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS completed_databases BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS total_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS completed_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS total_estimated_records BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS processed_records BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS failed_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS total_batches BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS completed_batches BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS failed_batches BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS skipped_batches BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS current_batch_number BIGINT")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS raw_batch_size BIGINT NOT NULL DEFAULT 1000")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS raw_parallel_collections BIGINT NOT NULL DEFAULT 1")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS raw_parallel_batches BIGINT NOT NULL DEFAULT 1")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS current_database TEXT")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS current_collection TEXT")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS progress_started_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS progress_updated_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS estimated_completion_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS estimated_remaining_seconds DOUBLE PRECISION")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS progress_message TEXT")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS progress_details_json JSONB NOT NULL DEFAULT '{}'::jsonb")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS target_source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS target_collection_name TEXT")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS retry_of_run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE SET NULL")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS retry_reason TEXT")
            cursor.execute("ALTER TABLE raw_ingestion_runs ADD COLUMN IF NOT EXISTS queued_at TIMESTAMPTZ")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_ingestion_batches (
                    batch_id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    batch_number BIGINT NOT NULL,
                    total_batches BIGINT NOT NULL DEFAULT 0,
                    cursor_start TEXT,
                    cursor_end TEXT,
                    estimated_rows BIGINT NOT NULL DEFAULT 0,
                    actual_rows BIGINT NOT NULL DEFAULT 0,
                    processed_rows BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'queued',
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    error_type TEXT,
                    error_message TEXT,
                    retryable BOOLEAN NOT NULL DEFAULT true,
                    recommended_fix TEXT,
                    retry_count BIGINT NOT NULL DEFAULT 0,
                    raw_object_key TEXT,
                    cursor_strategy TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(run_id, source_id, database_name, collection_name, batch_number)
                )
                """
            )
            cursor.execute("ALTER TABLE raw_ingestion_batches ADD COLUMN IF NOT EXISTS total_batches BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_batches ADD COLUMN IF NOT EXISTS processed_rows BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_ingestion_batches ADD COLUMN IF NOT EXISTS error_type TEXT")
            cursor.execute("ALTER TABLE raw_ingestion_batches ADD COLUMN IF NOT EXISTS retryable BOOLEAN NOT NULL DEFAULT true")
            cursor.execute("ALTER TABLE raw_ingestion_batches ADD COLUMN IF NOT EXISTS recommended_fix TEXT")
            cursor.execute("ALTER TABLE raw_ingestion_batches ADD COLUMN IF NOT EXISTS cursor_strategy TEXT")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_run_events (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    database_name TEXT,
                    collection_name TEXT,
                    batch_id UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_run_timings (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    level TEXT NOT NULL,
                    database_name TEXT,
                    collection_name TEXT,
                    batch_id UUID,
                    source_connect_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    query_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    file_write_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    metadata_update_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    total_duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    warning_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_run_database_progress (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES raw_ingestion_runs(id) ON DELETE CASCADE,
                    source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL,
                    database_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    total_collections BIGINT NOT NULL DEFAULT 0,
                    completed_collections BIGINT NOT NULL DEFAULT 0,
                    estimated_records BIGINT NOT NULL DEFAULT 0,
                    processed_records BIGINT NOT NULL DEFAULT 0,
                    current_collection TEXT,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    error_message TEXT,
                    UNIQUE(run_id, source_id, database_name)
                )
                """
            )
            cursor.execute("ALTER TABLE source_connections ALTER COLUMN cursor_field SET DEFAULT 'AUTO'")
            cursor.execute("ALTER TABLE source_connections ALTER COLUMN is_active SET DEFAULT false")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS last_inventory_status TEXT")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS last_inventory_message TEXT")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS last_inventory_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS ingestion_schedule_type TEXT NOT NULL DEFAULT 'manual_only'")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS ingestion_schedule_cron TEXT")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS last_scheduled_run_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS next_scheduled_run_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE source_connections ADD COLUMN IF NOT EXISTS schedule_enabled BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE source_connection_collections ADD COLUMN IF NOT EXISTS ingestion_schedule_type TEXT NOT NULL DEFAULT 'manual_only'")
            cursor.execute("ALTER TABLE source_connection_collections ADD COLUMN IF NOT EXISTS ingestion_schedule_cron TEXT")
            cursor.execute("ALTER TABLE source_connection_collections ADD COLUMN IF NOT EXISTS last_scheduled_run_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE source_connection_collections ADD COLUMN IF NOT EXISTS next_scheduled_run_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE source_connection_collections ADD COLUMN IF NOT EXISTS schedule_enabled BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE end_to_end_flow_runs ADD COLUMN IF NOT EXISTS rebuild_completed_stages BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE raw_collection_run_statuses ADD COLUMN IF NOT EXISTS cursor_strategy TEXT")
            cursor.execute("ALTER TABLE raw_collection_run_statuses ADD COLUMN IF NOT EXISTS error_type TEXT")
            cursor.execute("ALTER TABLE raw_collection_run_statuses ADD COLUMN IF NOT EXISTS recommended_fix TEXT")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS configured_cursor_field TEXT")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS detected_cursor_field TEXT")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS ingestion_strategy TEXT NOT NULL DEFAULT 'incremental_timestamp'")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS cursor_warning TEXT")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS last_snapshot_fingerprint TEXT")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS latest_source_document_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS estimated_ingestion_lag_seconds DOUBLE PRECISION")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS records_since_last_run BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS freshness_status TEXT")
            cursor.execute("ALTER TABLE raw_collection_states ADD COLUMN IF NOT EXISTS freshness_checked_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE bronze_file_states ADD COLUMN IF NOT EXISTS bronze_run_id UUID REFERENCES bronze_processing_runs(id) ON DELETE SET NULL")
            cursor.execute("ALTER TABLE bronze_file_states ADD COLUMN IF NOT EXISTS duration_seconds DOUBLE PRECISION")
            cursor.execute("ALTER TABLE bronze_file_states ADD COLUMN IF NOT EXISTS failed_step TEXT")
            cursor.execute("ALTER TABLE bronze_file_states ADD COLUMN IF NOT EXISTS error_type TEXT")
            cursor.execute("ALTER TABLE bronze_file_states ADD COLUMN IF NOT EXISTS stack_trace_summary TEXT")
            cursor.execute("ALTER TABLE bronze_file_states ADD COLUMN IF NOT EXISTS retryable BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE bronze_file_states ADD COLUMN IF NOT EXISTS recommended_fix TEXT")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'all_pending'")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS source_id UUID REFERENCES source_connections(id) ON DELETE SET NULL")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS database_name TEXT")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS collection_name TEXT")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS raw_file_id UUID")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS raw_object_key TEXT")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS retry_failed BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS current_database_name TEXT")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS current_collection_name TEXT")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS current_raw_file_id UUID")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS current_raw_object_key TEXT")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS current_phase TEXT NOT NULL DEFAULT 'queued'")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS total_databases BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS completed_databases BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS total_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS completed_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS total_raw_files BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS completed_raw_files BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS total_estimated_rows BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS processed_rows BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS read_raw_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS parse_json_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS add_audit_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS write_delta_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS metadata_update_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS validation_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS total_timing_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE bronze_processing_runs ADD COLUMN IF NOT EXISTS progress_updated_at TIMESTAMPTZ")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bronze_run_events (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES bronze_processing_runs(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    database_name TEXT,
                    collection_name TEXT,
                    raw_file_id UUID,
                    raw_object_key TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_attempts_run ON bronze_file_attempts(bronze_run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_attempts_status ON bronze_file_attempts(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_attempts_collection ON bronze_file_attempts(source_id, database_name, collection_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_connections_active ON source_connections(is_active)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_files_created_at ON raw_files(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_files_source_collection ON raw_files(source_id, collection_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_runs_created_at ON raw_ingestion_runs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_runs_progress_status ON raw_ingestion_runs(status, progress_updated_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_run_database_progress_run ON raw_run_database_progress(run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_run_events_run ON raw_run_events(run_id, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_run_timings_run ON raw_run_timings(run_id, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_ingestion_batches_run ON raw_ingestion_batches(run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_ingestion_batches_status ON raw_ingestion_batches(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_ingestion_batches_collection ON raw_ingestion_batches(source_id, database_name, collection_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_schema_detected_at ON raw_schema_snapshots(detected_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_collection_run_statuses_run ON raw_collection_run_statuses(run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_collection_run_statuses_status ON raw_collection_run_statuses(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_fingerprints_collection ON raw_batch_fingerprints(source_id, database_name, collection_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_connection_collections_source ON source_connection_collections(source_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_connection_collections_active ON source_connection_collections(source_id, is_active)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_connection_collections_stats ON source_connection_collections(last_stats_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_connections_schedule_due ON source_connections(schedule_enabled, next_scheduled_run_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_connection_collections_schedule_due ON source_connection_collections(source_id, schedule_enabled, next_scheduled_run_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_collection_states_freshness ON raw_collection_states(freshness_status, last_success_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_runs_created_at ON bronze_processing_runs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_states_run ON bronze_file_states(bronze_run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_states_status ON bronze_file_states(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_states_retryable ON bronze_file_states(retryable)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_states_failed_step ON bronze_file_states(failed_step)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_file_states_collection ON bronze_file_states(source_id, database_name, collection_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_runs_status ON bronze_processing_runs(status, progress_updated_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_run_events_run ON bronze_run_events(run_id, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_e2e_flow_runs_created ON end_to_end_flow_runs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_e2e_flow_runs_status ON end_to_end_flow_runs(status, updated_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_e2e_flow_steps_run ON end_to_end_flow_steps(run_id, stage)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_e2e_flow_items_run ON end_to_end_flow_items(run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_e2e_flow_items_scope ON end_to_end_flow_items(source_id, database_name, collection_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_schema_detected_at ON bronze_schema_snapshots(detected_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_column_mappings_detected_at ON bronze_column_mappings(detected_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_column_mappings_collection ON bronze_column_mappings(source_id, database_name, collection_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_column_mappings_object ON bronze_column_mappings(raw_object_key)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_collection_states_updated_at ON bronze_collection_states(updated_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_runs_created_at ON silver_processing_runs(created_at DESC)")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'all_pending'")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS database_name TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS collection_name TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS bronze_table TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS bronze_file TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS retry_failed BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS current_database_name TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS current_collection_name TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS current_bronze_table TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS current_bronze_file TEXT")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS current_phase TEXT NOT NULL DEFAULT 'queued'")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS total_databases BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS completed_databases BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS total_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS completed_collections BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS total_bronze_tables BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS completed_bronze_tables BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS total_batches BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS completed_batches BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS processed_rows BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS bronze_read_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS flattening_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS child_table_generation_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS delta_write_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS metadata_update_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS profiling_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS total_timing_seconds DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS progress_updated_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS silver_batch_size BIGINT NOT NULL DEFAULT 10")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS silver_parallel_collections BIGINT NOT NULL DEFAULT 1")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS silver_parallel_tables BIGINT NOT NULL DEFAULT 1")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS processed_tables BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS failed_tables BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_processing_runs ADD COLUMN IF NOT EXISTS failed_table_names_json JSONB NOT NULL DEFAULT '[]'::jsonb")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_run_events (
                    id UUID PRIMARY KEY,
                    run_id UUID REFERENCES silver_processing_runs(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    database_name TEXT,
                    collection_name TEXT,
                    bronze_table TEXT,
                    bronze_file TEXT,
                    batch_id UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_processing_batches (
                    batch_id UUID PRIMARY KEY,
                    run_id UUID REFERENCES silver_processing_runs(id) ON DELETE CASCADE,
                    database_name TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    bronze_table TEXT,
                    bronze_file TEXT,
                    batch_number BIGINT NOT NULL,
                    total_batches BIGINT NOT NULL DEFAULT 0,
                    rows_processed BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'queued',
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    duration_seconds DOUBLE PRECISION,
                    retry_count BIGINT NOT NULL DEFAULT 0,
                    failed_step TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    retryable BOOLEAN NOT NULL DEFAULT true,
                    recommended_fix TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(run_id, bronze_table, bronze_file, batch_number)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_pii_audit (
                    id UUID PRIMARY KEY,
                    database_name TEXT,
                    collection_name TEXT,
                    silver_table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    field_path TEXT,
                    detected_pii_type TEXT NOT NULL DEFAULT 'unknown',
                    detection_rule TEXT NOT NULL DEFAULT 'column_name_rule',
                    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                    action_taken TEXT NOT NULL DEFAULT 'retained_in_governed_silver',
                    safe_view_name TEXT,
                    safe_view_status TEXT NOT NULL DEFAULT 'missing_safe_view',
                    severity TEXT NOT NULL DEFAULT 'warning',
                    recommendation TEXT,
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE(silver_table_name, column_name, detected_pii_type)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_child_table_audit (
                    id UUID PRIMARY KEY,
                    database_name TEXT,
                    collection_name TEXT,
                    parent_table TEXT NOT NULL,
                    child_table TEXT NOT NULL,
                    source_nested_path TEXT,
                    reason TEXT NOT NULL DEFAULT 'array of objects',
                    parent_key TEXT,
                    child_index_field TEXT NOT NULL DEFAULT 'child_index',
                    rows_generated BIGINT NOT NULL DEFAULT 0,
                    relationship_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                    bi_usefulness TEXT NOT NULL DEFAULT 'unknown',
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE(child_table)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_raw_json_fallback_audit (
                    id UUID PRIMARY KEY,
                    database_name TEXT,
                    collection_name TEXT,
                    silver_table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    original_field_path TEXT,
                    reason TEXT NOT NULL DEFAULT 'rare/dynamic field',
                    needs_custom_transform BOOLEAN NOT NULL DEFAULT false,
                    bi_suitability_impact TEXT NOT NULL DEFAULT 'warning',
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE(silver_table_name, column_name)
                )
                """
            )
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS is_child_table BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS parent_silver_table_name TEXT")
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS child_path TEXT")
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS table_classification TEXT NOT NULL DEFAULT 'analytics_ready'")
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS bi_suitability TEXT NOT NULL DEFAULT 'unknown'")
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS governance_status TEXT NOT NULL DEFAULT 'unknown'")
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS lineage_reference TEXT")
            cursor.execute("ALTER TABLE silver_collection_states ADD COLUMN IF NOT EXISTS transform_strategy TEXT")
            cursor.execute("ALTER TABLE query_safe_views ADD COLUMN IF NOT EXISTS source_column_count BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE query_safe_views ADD COLUMN IF NOT EXISTS blocked_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb")
            cursor.execute("ALTER TABLE query_safe_views ADD COLUMN IF NOT EXISTS hash_version TEXT")
            cursor.execute("ALTER TABLE query_safe_views ADD COLUMN IF NOT EXISTS trino_visible BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS actor TEXT")
            cursor.execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS actor_role TEXT")
            cursor.execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS session_id TEXT")
            cursor.execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS request_id TEXT")
            cursor.execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS client_ip TEXT")
            cursor.execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS query_fingerprint TEXT")
            cursor.execute("ALTER TABLE query_history ADD COLUMN IF NOT EXISTS selected_view_status TEXT")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'info'")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS blocking BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS bi_suitability TEXT NOT NULL DEFAULT 'unknown'")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS governance_status TEXT NOT NULL DEFAULT 'unknown'")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS recommendation_type TEXT")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS table_classification TEXT")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS sql_usability_score DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS bi_readiness_score DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_transformation_quality ADD COLUMN IF NOT EXISTS raw_json_dependency_score DOUBLE PRECISION NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_batch_fingerprints ADD COLUMN IF NOT EXISTS error_type TEXT")
            cursor.execute("ALTER TABLE silver_batch_fingerprints ADD COLUMN IF NOT EXISTS failed_step TEXT")
            cursor.execute("ALTER TABLE silver_batch_fingerprints ADD COLUMN IF NOT EXISTS recommended_fix TEXT")
            cursor.execute("ALTER TABLE silver_batch_fingerprints ADD COLUMN IF NOT EXISTS full_stack_trace TEXT")
            cursor.execute("ALTER TABLE silver_batch_fingerprints ADD COLUMN IF NOT EXISTS possibly_corrupt BOOLEAN NOT NULL DEFAULT false")
            cursor.execute("ALTER TABLE silver_batch_fingerprints ADD COLUMN IF NOT EXISTS retry_count BIGINT NOT NULL DEFAULT 0")
            cursor.execute("ALTER TABLE silver_batch_fingerprints ADD COLUMN IF NOT EXISTS retryable BOOLEAN NOT NULL DEFAULT true")
            cursor.execute("ALTER TABLE bi_dashboards ADD COLUMN IF NOT EXISTS linked_datasets_json JSONB NOT NULL DEFAULT '[]'::jsonb")
            cursor.execute("ALTER TABLE bi_dashboards ADD COLUMN IF NOT EXISTS generated_chart_names_json JSONB NOT NULL DEFAULT '[]'::jsonb")
            cursor.execute("ALTER TABLE bi_dashboards ADD COLUMN IF NOT EXISTS generation_time_ms BIGINT")
            cursor.execute("ALTER TABLE bi_dashboards ADD COLUMN IF NOT EXISTS generated_by TEXT")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_batch_status ON silver_batch_fingerprints(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_batch_table ON silver_batch_fingerprints(silver_table_name, source_database, source_collection)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_batch_failed_type ON silver_batch_fingerprints(error_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_runs_status ON silver_processing_runs(status, progress_updated_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_run_events_run ON silver_run_events(run_id, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_processing_batches_run ON silver_processing_batches(run_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_processing_batches_status ON silver_processing_batches(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_processing_batches_scope ON silver_processing_batches(database_name, collection_name, bronze_table)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_schema_detected_at ON silver_schema_snapshots(detected_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_quality_measured_at ON silver_quality_metrics(measured_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_transformation_quality_status ON silver_transformation_quality(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_transformation_quality_score ON silver_transformation_quality(analytics_readiness_score)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_transformation_quality_validated ON silver_transformation_quality(last_validated_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_collection_child ON silver_collection_states(parent_silver_table_name, child_path)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_collection_classification ON silver_collection_states(table_classification)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_field_profiles_table ON silver_field_profiles(table_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_field_profiles_strategy ON silver_field_profiles(flattening_strategy)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_transform_plans_source ON silver_transform_plans(source_database, source_collection)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_transform_plans_classification ON silver_transform_plans(table_classification)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_pii_audit_scope ON silver_pii_audit(database_name, collection_name, silver_table_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_pii_audit_status ON silver_pii_audit(safe_view_status, severity)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_child_table_audit_parent ON silver_child_table_audit(parent_table)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_raw_json_audit_scope ON silver_raw_json_fallback_audit(database_name, collection_name, silver_table_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_raw_json_audit_transform ON silver_raw_json_fallback_audit(needs_custom_transform)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_safe_views_status ON query_safe_views(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_safe_views_trino_visible ON query_safe_views(trino_visible)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_validation_runs_created_at ON query_validation_runs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_history_started_at ON query_history(started_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_history_status ON query_history(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_history_selected_view ON query_history(selected_view)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_history_actor_started ON query_history(actor, started_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_history_fingerprint ON query_history(query_fingerprint)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_history_is_slow ON query_history(is_slow)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_datasets_status ON bi_datasets(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_datasets_last_refreshed ON bi_datasets(last_refreshed DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_dashboards_status ON bi_dashboards(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_dashboards_last_refresh ON bi_dashboards(last_refresh_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_dashboards_generated_by ON bi_dashboards(generated_by)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_charts_status ON bi_charts(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_charts_dataset ON bi_charts(dataset_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_validation_runs_created_at ON bi_validation_runs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_query_failures_created_at ON bi_query_failures(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bi_dashboard_loads_created_at ON bi_dashboard_loads(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_governance_assets_type_layer ON governance_assets(asset_type, layer)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_governance_assets_domain ON governance_assets(domain)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_governance_pii_asset ON governance_pii_tags(asset_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_governance_lineage_upstream ON governance_lineage_edges(upstream_asset)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_governance_lineage_downstream ON governance_lineage_edges(downstream_asset)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_governance_sync_runs_created_at ON governance_sync_runs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_operations_history_created_at ON operations_history(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_operations_history_layer_type ON operations_history(target_layer, operation_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_events_created_at ON platform_events(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_alerts_status_created ON platform_alerts(status, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_alerts_source ON platform_alerts(source)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_actor ON audit_logs(actor)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_users_role_status ON platform_users(role, status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_teams_status ON platform_teams(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_team_memberships_username ON platform_team_memberships(username)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_api_keys_status ON platform_api_keys(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform_settings_category ON platform_settings(category)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_status ON user_sessions(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_exports_created_at ON backup_exports(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_replay_jobs_status ON replay_jobs(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_replay_jobs_queued_at ON replay_jobs(queued_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_service_restart_history_started_at ON service_restart_history(started_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_global_validation_runs_created_at ON global_validation_runs(created_at DESC)")
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_batch_fingerprint
                ON raw_batch_fingerprints (
                    source_id,
                    database_name,
                    collection_name,
                    cursor_field,
                    min_cursor_value,
                    max_cursor_value,
                    row_count,
                    batch_checksum
                )
                """
            )
    _DASHBOARD_DB_INITIALIZED = True


def fetch_active_sources(source_id: str | None = None) -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            filters = ["s.is_active = true", "s.source_type = 'mongo'"]
            params: list[Any] = []
            if source_id:
                filters.append("s.id = %s")
                params.append(source_id)
            cursor.execute(
                f"""
                SELECT s.*,
                       COALESCE(
                           jsonb_agg(
                               jsonb_build_object(
                                   'collection_name', c.collection_name,
                                   'record_count', c.record_count,
                                   'estimated_size_bytes', c.estimated_size_bytes,
                                   'detected_cursor_field', c.detected_cursor_field,
                                   'detected_cursor_strategy', c.detected_cursor_strategy,
                                   'ingestion_schedule_type', c.ingestion_schedule_type,
                                   'ingestion_schedule_cron', c.ingestion_schedule_cron,
                                   'last_scheduled_run_at', c.last_scheduled_run_at,
                                   'next_scheduled_run_at', c.next_scheduled_run_at,
                                   'schedule_enabled', c.schedule_enabled
                               )
                               ORDER BY c.collection_name
                           ) FILTER (WHERE c.id IS NOT NULL),
                           '[]'::jsonb
                       ) AS active_collections_json
                FROM source_connections s
                LEFT JOIN source_connection_collections c
                    ON c.source_id = s.id
                    AND c.is_active = true
                WHERE {' AND '.join(filters)}
                GROUP BY s.id
                ORDER BY s.source_name
                """,
                params,
            )
            return [as_dict(row) for row in cursor.fetchall()]


def fetch_source(source_id: str) -> dict[str, Any]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM source_connections WHERE id = %s", (source_id,))
            return as_dict(cursor.fetchone())


def _insert_raw_run_event(
    cursor,
    *,
    run_id: str,
    event_type: str,
    message: str | None = None,
    database_name: str | None = None,
    collection_name: str | None = None,
    batch_id: str | None = None,
) -> None:
    cursor.execute(
        """
        INSERT INTO raw_run_events (
            id, run_id, event_type, message, database_name, collection_name, batch_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (new_id(), run_id, event_type, message, database_name, collection_name, batch_id),
    )


def record_raw_run_event(
    run_id: str,
    event_type: str,
    message: str | None = None,
    *,
    database_name: str | None = None,
    collection_name: str | None = None,
    batch_id: str | None = None,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            _insert_raw_run_event(
                cursor,
                run_id=run_id,
                event_type=event_type,
                message=message,
                database_name=database_name,
                collection_name=collection_name,
                batch_id=batch_id,
            )


def record_raw_run_timing(
    *,
    run_id: str,
    level: str,
    database_name: str | None = None,
    collection_name: str | None = None,
    batch_id: str | None = None,
    source_connect_seconds: float = 0,
    query_seconds: float = 0,
    file_write_seconds: float = 0,
    metadata_update_seconds: float = 0,
    total_duration_seconds: float = 0,
    warning_message: str | None = None,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_run_timings (
                    id, run_id, level, database_name, collection_name, batch_id,
                    source_connect_seconds, query_seconds, file_write_seconds,
                    metadata_update_seconds, total_duration_seconds, warning_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    new_id(),
                    run_id,
                    level,
                    database_name,
                    collection_name,
                    batch_id,
                    max(0.0, float(source_connect_seconds or 0)),
                    max(0.0, float(query_seconds or 0)),
                    max(0.0, float(file_write_seconds or 0)),
                    max(0.0, float(metadata_update_seconds or 0)),
                    max(0.0, float(total_duration_seconds or 0)),
                    warning_message,
                ),
            )


def ensure_raw_run(airflow_run_id: str, triggered_by: str = "airflow") -> str:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT id FROM raw_ingestion_runs WHERE airflow_run_id = %s", (airflow_run_id,))
            existing = cursor.fetchone()
            now = datetime.now(timezone.utc)
            if existing:
                run_id = str(existing["id"])
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM raw_run_events WHERE run_id = %s AND event_type = 'queued')",
                    (run_id,),
                )
                if not bool(cursor.fetchone()[0]):
                    _insert_raw_run_event(cursor, run_id=run_id, event_type="queued", message="RAW run queued")
                cursor.execute("DELETE FROM raw_run_database_progress WHERE run_id = %s", (run_id,))
                cursor.execute(
                    """
                    UPDATE raw_ingestion_runs
                    SET status = 'running',
                        queued_at = COALESCE(queued_at, created_at, %s),
                        started_at = COALESCE(started_at, %s),
                        finished_at = NULL,
                        duration_seconds = NULL,
                        total_rows = 0,
                        total_files = 0,
                        total_rows_found = 0,
                        total_rows_written = 0,
                        no_new_data_collections = 0,
                        duplicate_batches_skipped = 0,
                        progress_phase = 'initializing',
                        progress_percent = 1,
                        total_databases = 0,
                        completed_databases = 0,
                        total_collections = 0,
                        completed_collections = 0,
                        total_estimated_records = 0,
                        processed_records = 0,
                        failed_collections = 0,
                        total_batches = 0,
                        completed_batches = 0,
                        failed_batches = 0,
                        skipped_batches = 0,
                        current_batch_number = NULL,
                        current_database = NULL,
                        current_collection = NULL,
                        progress_started_at = COALESCE(progress_started_at, started_at, %s),
                        progress_updated_at = %s,
                        estimated_completion_at = NULL,
                        estimated_remaining_seconds = NULL,
                        progress_message = 'Initializing RAW ingestion',
                        progress_details_json = '{}'::jsonb,
                        error_message = NULL,
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (now, now, now, now, run_id),
                )
                _insert_raw_run_event(cursor, run_id=run_id, event_type="started", message="RAW ingestion started")
                return run_id

            run_id = new_id()
            cursor.execute(
                """
                INSERT INTO raw_ingestion_runs (
                    id, airflow_run_id, status, queued_at, started_at, triggered_by,
                    progress_phase, progress_percent, progress_started_at,
                    progress_updated_at, progress_message
                )
                VALUES (%s, %s, 'running', %s, %s, %s, 'initializing', 1, %s, %s,
                        'Initializing RAW ingestion')
                """,
                (run_id, airflow_run_id, now, now, triggered_by, now, now),
            )
            _insert_raw_run_event(cursor, run_id=run_id, event_type="queued", message="RAW run queued")
            _insert_raw_run_event(cursor, run_id=run_id, event_type="started", message="RAW ingestion started")
            return run_id


def set_raw_run_scope(run_id: str, source_id: str | None = None, collection_name: str | None = None) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE raw_ingestion_runs
                SET target_source_id = COALESCE(%s, target_source_id),
                    target_collection_name = COALESCE(%s, target_collection_name),
                    updated_at = now()
                WHERE id = %s
                """,
                (source_id, collection_name, run_id),
            )


def set_raw_run_retry_lineage(run_id: str, retry_of_run_id: str | None = None, retry_reason: str | None = None) -> None:
    if not retry_of_run_id and not retry_reason:
        return
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE raw_ingestion_runs
                SET retry_of_run_id = COALESCE(%s, retry_of_run_id),
                    retry_reason = COALESCE(%s, retry_reason),
                    progress_details_json = progress_details_json || jsonb_strip_nulls(%s::jsonb),
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    retry_of_run_id,
                    retry_reason,
                    json_param({"retry_of_run_id": retry_of_run_id, "retry_reason": retry_reason}),
                    run_id,
                ),
            )


RAW_TERMINAL_SUCCESS_STATUSES = {"success", "no_new_data", "duplicate_batch_skipped"}


def _int_value(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def raw_progress_percent(
    *,
    status: str | None,
    phase: str | None,
    completed_batches: int = 0,
    total_batches: int = 0,
    completed_collections: int = 0,
    total_collections: int = 0,
    processed_records: int = 0,
    estimated_records: int = 0,
) -> float:
    status_value = str(status or "").lower()
    phase_value = str(phase or "").lower()
    if status_value in RAW_TERMINAL_SUCCESS_STATUSES:
        return 100.0
    if status_value in {"requested", "queued"}:
        return 0.0

    percent = 0.0
    if total_batches > 0:
        percent = (completed_batches / total_batches) * 100
    elif total_collections > 0:
        percent = (completed_collections / total_collections) * 100
    elif estimated_records > 0:
        percent = (processed_records / estimated_records) * 100
    elif phase_value == "initializing" or status_value in {"running", "processing"}:
        percent = 1.0

    percent = max(0.0, min(100.0, percent))
    if status_value in {"running", "processing", "cancelling"} or phase_value in {
        "initializing",
        "reading source",
        "extracting",
        "writing raw file",
        "updating metadata",
    }:
        percent = max(1.0, percent)
    if status_value == "failed":
        return max(1.0, min(99.0, percent))
    if status_value not in RAW_TERMINAL_SUCCESS_STATUSES:
        return min(99.0, percent)
    return percent


def update_raw_run_progress(
    run_id: str,
    *,
    status: str | None = None,
    phase: str | None = None,
    total_databases: int | None = None,
    completed_databases: int | None = None,
    total_collections: int | None = None,
    completed_collections: int | None = None,
    total_estimated_records: int | None = None,
    processed_records: int | None = None,
    failed_collections: int | None = None,
    total_batches: int | None = None,
    completed_batches: int | None = None,
    failed_batches: int | None = None,
    skipped_batches: int | None = None,
    current_batch_number: int | None = None,
    raw_batch_size: int | None = None,
    raw_parallel_collections: int | None = None,
    raw_parallel_batches: int | None = None,
    current_database: str | None = None,
    current_collection: str | None = None,
    progress_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    init_dashboard_db()
    now = datetime.now(timezone.utc)
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM raw_ingestion_runs WHERE id = %s", (run_id,))
            row = as_dict(cursor.fetchone())
            if not row:
                return

            next_status = status or row.get("status") or "running"
            next_phase = phase or row.get("progress_phase") or "running"
            next_total_databases = _int_value(total_databases, _int_value(row.get("total_databases")))
            next_completed_databases = _int_value(completed_databases, _int_value(row.get("completed_databases")))
            next_total_collections = _int_value(total_collections, _int_value(row.get("total_collections")))
            next_completed_collections = _int_value(completed_collections, _int_value(row.get("completed_collections")))
            next_total_estimated_records = _int_value(total_estimated_records, _int_value(row.get("total_estimated_records")))
            next_processed_records = _int_value(processed_records, _int_value(row.get("processed_records")))
            next_failed_collections = _int_value(failed_collections, _int_value(row.get("failed_collections")))
            next_total_batches = _int_value(total_batches, _int_value(row.get("total_batches")))
            next_completed_batches = _int_value(completed_batches, _int_value(row.get("completed_batches")))
            next_failed_batches = _int_value(failed_batches, _int_value(row.get("failed_batches")))
            next_skipped_batches = _int_value(skipped_batches, _int_value(row.get("skipped_batches")))
            next_batch_size = _int_value(raw_batch_size, _int_value(row.get("raw_batch_size"), 1000))
            next_parallel_collections = _int_value(raw_parallel_collections, _int_value(row.get("raw_parallel_collections"), 1))
            next_parallel_batches = _int_value(raw_parallel_batches, _int_value(row.get("raw_parallel_batches"), 1))
            started_at = row.get("progress_started_at") or row.get("started_at") or now
            if not isinstance(started_at, datetime):
                started_at = now
            percent = raw_progress_percent(
                status=next_status,
                phase=next_phase,
                completed_batches=next_completed_batches,
                total_batches=next_total_batches,
                completed_collections=next_completed_collections,
                total_collections=next_total_collections,
                processed_records=next_processed_records,
                estimated_records=next_total_estimated_records,
            )
            remaining_seconds = None
            estimated_completion_at = None
            elapsed_seconds = max(0.0, (now - started_at).total_seconds())
            if percent >= 100:
                remaining_seconds = 0.0
                estimated_completion_at = now
            elif percent > 0 and elapsed_seconds > 0:
                remaining_seconds = max(0.0, elapsed_seconds * ((100.0 - percent) / percent))
                estimated_completion_at = now + timedelta(seconds=remaining_seconds)

            progress_details = row.get("progress_details_json") or {}
            if details:
                progress_details = {**progress_details, **details}

            cursor.execute(
                """
                UPDATE raw_ingestion_runs
                SET status = %s,
                    progress_phase = %s,
                    progress_percent = %s,
                    total_databases = %s,
                    completed_databases = %s,
                    total_collections = %s,
                    completed_collections = %s,
                    total_estimated_records = %s,
                    processed_records = %s,
                    failed_collections = %s,
                    total_batches = %s,
                    completed_batches = %s,
                    failed_batches = %s,
                    skipped_batches = %s,
                    current_batch_number = %s,
                    raw_batch_size = %s,
                    raw_parallel_collections = %s,
                    raw_parallel_batches = %s,
                    current_database = %s,
                    current_collection = %s,
                    progress_started_at = COALESCE(progress_started_at, started_at, %s),
                    progress_updated_at = %s,
                    estimated_completion_at = %s,
                    estimated_remaining_seconds = %s,
                    progress_message = %s,
                    progress_details_json = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    next_status,
                    next_phase,
                    round(percent, 2),
                    next_total_databases,
                    next_completed_databases,
                    next_total_collections,
                    next_completed_collections,
                    next_total_estimated_records,
                    next_processed_records,
                    next_failed_collections,
                    next_total_batches,
                    next_completed_batches,
                    next_failed_batches,
                    next_skipped_batches,
                    current_batch_number if current_batch_number is not None else row.get("current_batch_number"),
                    next_batch_size,
                    next_parallel_collections,
                    next_parallel_batches,
                    current_database if current_database is not None else row.get("current_database"),
                    current_collection if current_collection is not None else row.get("current_collection"),
                    started_at,
                    now,
                    estimated_completion_at,
                    remaining_seconds,
                    progress_message if progress_message is not None else row.get("progress_message"),
                    json_param(progress_details),
                    run_id,
                ),
            )


def upsert_raw_database_progress(
    *,
    run_id: str,
    source_id: str,
    database_name: str,
    status: str,
    total_collections: int,
    completed_collections: int,
    estimated_records: int,
    processed_records: int,
    current_collection: str | None = None,
    error_message: str | None = None,
    started: bool = False,
    finished: bool = False,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_run_database_progress (
                    id, run_id, source_id, database_name, status, total_collections,
                    completed_collections, estimated_records, processed_records,
                    current_collection, started_at, finished_at, error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        CASE WHEN %s THEN now() ELSE NULL END,
                        CASE WHEN %s THEN now() ELSE NULL END,
                        %s)
                ON CONFLICT (run_id, source_id, database_name)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    total_collections = EXCLUDED.total_collections,
                    completed_collections = EXCLUDED.completed_collections,
                    estimated_records = EXCLUDED.estimated_records,
                    processed_records = EXCLUDED.processed_records,
                    current_collection = EXCLUDED.current_collection,
                    started_at = COALESCE(raw_run_database_progress.started_at, EXCLUDED.started_at),
                    finished_at = CASE WHEN %s THEN COALESCE(raw_run_database_progress.finished_at, now()) ELSE NULL END,
                    error_message = EXCLUDED.error_message,
                    updated_at = now()
                """,
                (
                    new_id(),
                    run_id,
                    source_id,
                    database_name,
                    status,
                    total_collections,
                    completed_collections,
                    estimated_records,
                    processed_records,
                    current_collection,
                    started,
                    finished,
                    error_message,
                    finished,
                ),
            )


def ensure_raw_ingestion_batches(
    *,
    run_id: str,
    source_id: str,
    database_name: str,
    collection_name: str,
    total_batches: int,
    estimated_rows: int,
    cursor_strategy: str,
) -> list[dict[str, Any]]:
    init_dashboard_db()
    estimated_per_batch = max(0, int(estimated_rows or 0))
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            for batch_number in range(1, max(1, int(total_batches or 1)) + 1):
                cursor.execute(
                    """
                    INSERT INTO raw_ingestion_batches (
                        batch_id, run_id, source_id, database_name, collection_name,
                        batch_number, total_batches, estimated_rows, status, cursor_strategy
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'queued', %s)
                    ON CONFLICT (run_id, source_id, database_name, collection_name, batch_number)
                    DO UPDATE SET
                        total_batches = GREATEST(raw_ingestion_batches.total_batches, EXCLUDED.total_batches),
                        estimated_rows = GREATEST(raw_ingestion_batches.estimated_rows, EXCLUDED.estimated_rows),
                        cursor_strategy = COALESCE(raw_ingestion_batches.cursor_strategy, EXCLUDED.cursor_strategy),
                        updated_at = now()
                    """,
                    (
                        new_id(),
                        run_id,
                        source_id,
                        database_name,
                        collection_name,
                        batch_number,
                        int(total_batches or 0),
                        estimated_per_batch,
                        cursor_strategy,
                    ),
                )
            cursor.execute(
                """
                SELECT *
                FROM raw_ingestion_batches
                WHERE run_id = %s
                  AND source_id = %s
                  AND database_name = %s
                  AND collection_name = %s
                ORDER BY batch_number
                """,
                (run_id, source_id, database_name, collection_name),
            )
            return [as_dict(row) for row in cursor.fetchall()]


def start_raw_ingestion_batch(batch_id: str) -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                UPDATE raw_ingestion_batches
                SET status = CASE WHEN status IN ('failed', 'retrying') THEN 'retrying' ELSE 'running' END,
                    started_at = COALESCE(started_at, now()),
                    finished_at = NULL,
                    duration_seconds = NULL,
                    processed_rows = 0,
                    error_type = NULL,
                    error_message = NULL,
                    retryable = true,
                    recommended_fix = NULL,
                    retry_count = CASE WHEN status IN ('failed', 'retrying') THEN retry_count + 1 ELSE retry_count END,
                    updated_at = now()
                WHERE batch_id = %s
                  AND status IN ('queued', 'running', 'failed', 'retrying')
                RETURNING *
                """,
                (batch_id,),
            )
            batch = as_dict(cursor.fetchone())
            if batch:
                _insert_raw_run_event(
                    cursor,
                    run_id=str(batch["run_id"]),
                    event_type="batch_started",
                    message=f"Batch {batch['batch_number']} started for {batch['database_name']}.{batch['collection_name']}",
                    database_name=batch.get("database_name"),
                    collection_name=batch.get("collection_name"),
                    batch_id=str(batch.get("batch_id")),
                )
            return batch


def raw_run_progress_details(run_id: str) -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT progress_details_json FROM raw_ingestion_runs WHERE id = %s", (run_id,))
            row = cursor.fetchone()
            if not row:
                return {}
            details = row["progress_details_json"] or {}
            if isinstance(details, str):
                return json.loads(details)
            return dict(details)


def finish_raw_ingestion_batch(
    *,
    batch_id: str,
    status: str,
    actual_rows: int = 0,
    processed_rows: int | None = None,
    cursor_start: str | None = None,
    cursor_end: str | None = None,
    raw_object_key: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    retryable: bool = True,
    recommended_fix: str | None = None,
) -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                UPDATE raw_ingestion_batches
                SET status = %s,
                    actual_rows = %s,
                    processed_rows = %s,
                    cursor_start = COALESCE(%s, cursor_start),
                    cursor_end = COALESCE(%s, cursor_end),
                    raw_object_key = COALESCE(%s, raw_object_key),
                    error_type = %s,
                    error_message = %s,
                    retryable = %s,
                    recommended_fix = %s,
                    finished_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now()))),
                    updated_at = now()
                WHERE batch_id = %s
                RETURNING *
                """,
                (
                    status,
                    actual_rows,
                    actual_rows if processed_rows is None else processed_rows,
                    cursor_start,
                    cursor_end,
                    raw_object_key,
                    error_type,
                    error_message,
                    retryable,
                    recommended_fix,
                    batch_id,
                ),
            )
            batch = as_dict(cursor.fetchone())
            if batch:
                event_type = "batch_completed" if status in {"success", "skipped"} else status
                _insert_raw_run_event(
                    cursor,
                    run_id=str(batch["run_id"]),
                    event_type=event_type,
                    message=f"Batch {batch['batch_number']} {status} for {batch['database_name']}.{batch['collection_name']}",
                    database_name=batch.get("database_name"),
                    collection_name=batch.get("collection_name"),
                    batch_id=str(batch.get("batch_id")),
                )
            return batch


def raw_batch_status_counts(run_id: str) -> dict[str, int]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT count(*) AS total_batches,
                       count(*) FILTER (WHERE status = ANY(%s)) AS completed_batches,
                       count(*) FILTER (WHERE status = 'queued') AS queued_batches,
                       count(*) FILTER (WHERE status IN ('running', 'retrying')) AS running_batches,
                       count(*) FILTER (WHERE status = 'failed') AS failed_batches,
                       count(*) FILTER (WHERE status = 'skipped') AS skipped_batches,
                       count(*) FILTER (WHERE status = 'cancelled') AS cancelled_batches,
                       COALESCE(sum(processed_rows), 0) AS records_processed
                FROM raw_ingestion_batches
                WHERE run_id = %s
                """,
                (["success", "skipped"], run_id),
            )
            row = as_dict(cursor.fetchone())
    return {key: int(row.get(key) or 0) for key in row}


def mark_interrupted_raw_batches(run_id: str | None = None, timeout_minutes: int = 30) -> int:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            params: list[Any] = [max(1, int(timeout_minutes or 30))]
            run_clause = ""
            if run_id:
                run_clause = "AND run_id = %s"
                params.append(run_id)
            cursor.execute(
                f"""
                UPDATE raw_ingestion_batches
                SET status = 'failed',
                    finished_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, updated_at, now()))),
                    error_type = 'Interrupted',
                    error_message = 'Batch was running past the interruption timeout and was marked retryable.',
                    retryable = true,
                    recommended_fix = 'Retry failed RAW batches. Successful batches will be skipped.',
                    updated_at = now()
                WHERE status IN ('running', 'retrying')
                  AND COALESCE(started_at, updated_at, now()) < now() - (%s || ' minutes')::interval
                  {run_clause}
                """,
                params,
            )
            return cursor.rowcount


def finish_raw_run(
    run_id: str,
    status: str,
    total_rows_found: int,
    total_rows_written: int,
    total_files: int,
    no_new_data_collections: int = 0,
    duplicate_batches_skipped: int = 0,
    error_message: str | None = None,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE raw_ingestion_runs
                SET status = %s,
                    finished_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now()))),
                    total_rows = %s,
                    total_files = %s,
                    total_rows_found = %s,
                    total_rows_written = %s,
                    no_new_data_collections = %s,
                    duplicate_batches_skipped = %s,
                    progress_phase = CASE
                        WHEN %s = 'failed' THEN 'failed'
                        WHEN %s = 'cancelled' THEN 'cancelled'
                        ELSE 'completed'
                    END,
                    progress_percent = CASE WHEN %s IN ('failed', 'cancelled') THEN LEAST(99, GREATEST(1, progress_percent)) ELSE 100 END,
                    completed_collections = CASE
                        WHEN %s IN ('failed', 'cancelled') THEN completed_collections
                        ELSE GREATEST(completed_collections, total_collections)
                    END,
                    processed_records = GREATEST(processed_records, %s),
                    failed_collections = GREATEST(failed_collections, (
                        SELECT count(*)
                        FROM raw_collection_run_statuses
                        WHERE run_id = %s AND status = 'failed'
                    )),
                    total_batches = GREATEST(total_batches, (
                        SELECT count(*)
                        FROM raw_ingestion_batches
                        WHERE run_id = %s
                    )),
                    completed_batches = GREATEST(completed_batches, (
                        SELECT count(*)
                        FROM raw_ingestion_batches
                        WHERE run_id = %s AND status = ANY(%s)
                    )),
                    failed_batches = GREATEST(failed_batches, (
                        SELECT count(*)
                        FROM raw_ingestion_batches
                        WHERE run_id = %s AND status = 'failed'
                    )),
                    skipped_batches = GREATEST(skipped_batches, (
                        SELECT count(*)
                        FROM raw_ingestion_batches
                        WHERE run_id = %s AND status = 'skipped'
                    )),
                    current_batch_number = CASE WHEN %s IN ('failed', 'cancelled') THEN current_batch_number ELSE NULL END,
                    current_database = CASE WHEN %s IN ('failed', 'cancelled') THEN current_database ELSE NULL END,
                    current_collection = CASE WHEN %s IN ('failed', 'cancelled') THEN current_collection ELSE NULL END,
                    progress_updated_at = now(),
                    estimated_completion_at = CASE WHEN %s IN ('failed', 'cancelled') THEN estimated_completion_at ELSE now() END,
                    estimated_remaining_seconds = CASE WHEN %s IN ('failed', 'cancelled') THEN estimated_remaining_seconds ELSE 0 END,
                    progress_message = CASE
                        WHEN %s = 'failed' THEN COALESCE(%s, progress_message, 'RAW ingestion failed')
                        WHEN %s = 'cancelled' THEN COALESCE(%s, progress_message, 'RAW ingestion cancelled')
                        ELSE 'RAW ingestion completed'
                    END,
                    error_message = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    status,
                    total_rows_written,
                    total_files,
                    total_rows_found,
                    total_rows_written,
                    no_new_data_collections,
                    duplicate_batches_skipped,
                    status,
                    status,
                    status,
                    status,
                    total_rows_found,
                    run_id,
                    run_id,
                    run_id,
                    ["success", "skipped"],
                    run_id,
                    run_id,
                    status,
                    status,
                    status,
                    status,
                    status,
                    status,
                    error_message,
                    status,
                    error_message,
                    error_message,
                    run_id,
                ),
            )
            event_type = "completed" if status in RAW_TERMINAL_SUCCESS_STATUSES else status
            _insert_raw_run_event(
                cursor,
                run_id=run_id,
                event_type=event_type,
                message=error_message or f"RAW ingestion finished with status {status}",
            )


def upsert_collection_state(
    *,
    source_id: str,
    database_name: str,
    collection_name: str,
    cursor_field: str,
    configured_cursor_field: str | None = None,
    detected_cursor_field: str | None = None,
    ingestion_strategy: str = "incremental_timestamp",
    cursor_warning: str | None = None,
    last_cursor_value: str | None,
    last_snapshot_fingerprint: str | None = None,
    last_row_count: int,
    last_raw_path: str | None,
    latest_source_document_at: datetime | None = None,
    estimated_ingestion_lag_seconds: float | None = None,
    records_since_last_run: int = 0,
    freshness_status: str | None = None,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_collection_states (
                    id, source_id, database_name, collection_name, cursor_field,
                    configured_cursor_field, detected_cursor_field, ingestion_strategy, cursor_warning,
                    last_cursor_value, last_snapshot_fingerprint, last_success_at, last_row_count, last_raw_path,
                    latest_source_document_at, estimated_ingestion_lag_seconds, records_since_last_run,
                    freshness_status, freshness_checked_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (source_id, database_name, collection_name)
                DO UPDATE SET
                    cursor_field = EXCLUDED.cursor_field,
                    configured_cursor_field = EXCLUDED.configured_cursor_field,
                    detected_cursor_field = EXCLUDED.detected_cursor_field,
                    ingestion_strategy = EXCLUDED.ingestion_strategy,
                    cursor_warning = EXCLUDED.cursor_warning,
                    last_cursor_value = COALESCE(EXCLUDED.last_cursor_value, raw_collection_states.last_cursor_value),
                    last_snapshot_fingerprint = COALESCE(EXCLUDED.last_snapshot_fingerprint, raw_collection_states.last_snapshot_fingerprint),
                    last_success_at = now(),
                    last_row_count = EXCLUDED.last_row_count,
                    last_raw_path = COALESCE(EXCLUDED.last_raw_path, raw_collection_states.last_raw_path),
                    latest_source_document_at = COALESCE(EXCLUDED.latest_source_document_at, raw_collection_states.latest_source_document_at),
                    estimated_ingestion_lag_seconds = EXCLUDED.estimated_ingestion_lag_seconds,
                    records_since_last_run = EXCLUDED.records_since_last_run,
                    freshness_status = EXCLUDED.freshness_status,
                    freshness_checked_at = now(),
                    updated_at = now()
                """,
                (
                    new_id(),
                    source_id,
                    database_name,
                    collection_name,
                    cursor_field,
                    configured_cursor_field,
                    detected_cursor_field,
                    ingestion_strategy,
                    cursor_warning,
                    last_cursor_value,
                    last_snapshot_fingerprint,
                    last_row_count,
                    last_raw_path,
                    latest_source_document_at,
                    estimated_ingestion_lag_seconds,
                    records_since_last_run,
                    freshness_status,
                ),
            )


def latest_collection_state(source_id: str, database_name: str, collection_name: str) -> dict[str, Any]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM raw_collection_states
                WHERE source_id = %s AND database_name = %s AND collection_name = %s
                """,
                (source_id, database_name, collection_name),
            )
            return as_dict(cursor.fetchone())


def insert_collection_run_status(
    *,
    run_id: str,
    source_id: str,
    database_name: str,
    collection_name: str,
    status: str,
    rows_found: int,
    rows_written: int,
    previous_cursor_value: str | None,
    new_cursor_value: str | None,
    raw_object_key: str | None = None,
    cursor_strategy: str | None = None,
    error_type: str | None = None,
    message: str | None = None,
    recommended_fix: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_collection_run_statuses (
                    id, run_id, source_id, database_name, collection_name, status,
                    rows_found, rows_written, raw_object_key, previous_cursor_value,
                    new_cursor_value, cursor_strategy, error_type, message,
                    recommended_fix, started_at, finished_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), COALESCE(%s, now()))
                """,
                (
                    new_id(),
                    run_id,
                    source_id,
                    database_name,
                    collection_name,
                    status,
                    rows_found,
                    rows_written,
                    raw_object_key,
                    previous_cursor_value,
                    new_cursor_value,
                    cursor_strategy,
                    error_type,
                    message,
                    recommended_fix,
                    started_at,
                    finished_at,
                ),
            )


def raw_batch_fingerprint_exists(
    *,
    source_id: str,
    database_name: str,
    collection_name: str,
    cursor_field: str,
    min_cursor_value: str,
    max_cursor_value: str,
    row_count: int,
    batch_checksum: str,
) -> dict[str, Any]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM raw_batch_fingerprints
                WHERE source_id = %s
                    AND database_name = %s
                    AND collection_name = %s
                    AND cursor_field = %s
                    AND min_cursor_value = %s
                    AND max_cursor_value = %s
                    AND row_count = %s
                    AND batch_checksum = %s
                LIMIT 1
                """,
                (
                    source_id,
                    database_name,
                    collection_name,
                    cursor_field,
                    min_cursor_value,
                    max_cursor_value,
                    row_count,
                    batch_checksum,
                ),
            )
            return as_dict(cursor.fetchone())


def insert_raw_batch_fingerprint(
    *,
    source_id: str,
    database_name: str,
    collection_name: str,
    cursor_field: str,
    min_cursor_value: str,
    max_cursor_value: str,
    row_count: int,
    batch_checksum: str,
    raw_object_key: str,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_batch_fingerprints (
                    id, source_id, database_name, collection_name, cursor_field,
                    min_cursor_value, max_cursor_value, row_count, batch_checksum, raw_object_key
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    new_id(),
                    source_id,
                    database_name,
                    collection_name,
                    cursor_field,
                    min_cursor_value,
                    max_cursor_value,
                    row_count,
                    batch_checksum,
                    raw_object_key,
                ),
            )


def latest_schema_fields(source_id: str, database_name: str, collection_name: str) -> list[str]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT fields_json
                FROM raw_schema_snapshots
                WHERE source_id = %s AND database_name = %s AND collection_name = %s
                ORDER BY detected_at DESC
                LIMIT 1
                """,
                (source_id, database_name, collection_name),
            )
            row = cursor.fetchone()
            if not row:
                return []
            fields_json = row["fields_json"] or {}
            if isinstance(fields_json, str):
                fields_json = json.loads(fields_json)
            return list(fields_json.get("fields", []))


def insert_schema_snapshot(
    *,
    source_id: str,
    database_name: str,
    collection_name: str,
    schema_hash: str,
    fields_json: dict[str, Any],
    new_fields: list[str],
    removed_fields: list[str],
    change_type: str,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_schema_snapshots (
                    id, source_id, database_name, collection_name, schema_hash,
                    fields_json, new_fields_json, removed_fields_json, change_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    new_id(),
                    source_id,
                    database_name,
                    collection_name,
                    schema_hash,
                    json_param(fields_json),
                    json_param(new_fields),
                    json_param(removed_fields),
                    change_type,
                ),
            )


def insert_raw_file(
    *,
    run_id: str,
    source_id: str,
    database_name: str,
    collection_name: str,
    minio_bucket: str,
    object_key: str,
    row_count: int,
    file_size_bytes: int,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_files (
                    id, run_id, source_id, database_name, collection_name,
                    minio_bucket, object_key, row_count, file_size_bytes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (minio_bucket, object_key)
                DO UPDATE SET
                    row_count = EXCLUDED.row_count,
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    created_at = now()
                """,
                (
                    new_id(),
                    run_id,
                    source_id,
                    database_name,
                    collection_name,
                    minio_bucket,
                    object_key,
                    row_count,
                    file_size_bytes,
                ),
            )
            _insert_raw_run_event(
                cursor,
                run_id=run_id,
                event_type="file_written",
                message=f"RAW file written for {database_name}.{collection_name}: {object_key}",
                database_name=database_name,
                collection_name=collection_name,
            )


def record_health(service_name: str, status: str, message: str | None = None) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO service_health_checks (id, service_name, status, message)
                VALUES (%s, %s, %s, %s)
                """,
                (new_id(), service_name, status, message),
            )


def record_bronze_run_event(
    run_id: str,
    event_type: str,
    message: str | None = None,
    *,
    database_name: str | None = None,
    collection_name: str | None = None,
    raw_file_id: str | None = None,
    raw_object_key: str | None = None,
) -> None:
    init_dashboard_db()

    def operation() -> None:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO bronze_run_events (
                        id, run_id, event_type, message, database_name, collection_name, raw_file_id, raw_object_key
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (new_id(), run_id, event_type, message, database_name, collection_name, raw_file_id, raw_object_key),
                )

    with_bronze_metadata_retry("record_bronze_run_event", operation)


def update_bronze_run_progress(run_id: str, **fields: Any) -> None:
    allowed = {
        "status",
        "current_database_name",
        "current_collection_name",
        "current_raw_file_id",
        "current_raw_object_key",
        "current_phase",
        "total_databases",
        "completed_databases",
        "total_collections",
        "completed_collections",
        "total_raw_files",
        "completed_raw_files",
        "total_estimated_rows",
        "processed_rows",
        "read_raw_seconds",
        "parse_json_seconds",
        "add_audit_seconds",
        "write_delta_seconds",
        "metadata_update_seconds",
        "validation_seconds",
        "total_timing_seconds",
        "error_message",
    }
    assignments = []
    values = []
    for key, value in fields.items():
        if key not in allowed:
            continue
        assignments.append(f"{key} = %s")
        values.append(value)
    if not assignments:
        return

    def operation() -> None:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE bronze_processing_runs
                    SET {", ".join(assignments)},
                        progress_updated_at = now(),
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (*values, run_id),
                )

    with_bronze_metadata_retry("update_bronze_run_progress", operation)


def ensure_bronze_run(
    airflow_run_id: str,
    triggered_by: str = "airflow",
    *,
    scope: str = "all_pending",
    source_id: str | None = None,
    database_name: str | None = None,
    collection_name: str | None = None,
    raw_file_id: str | None = None,
    raw_object_key: str | None = None,
    retry_failed: bool = False,
) -> str:
    init_dashboard_db()
    scope = scope or "all_pending"
    source_id = source_id or None
    database_name = database_name or None
    collection_name = collection_name or None
    raw_file_id = raw_file_id or None
    raw_object_key = raw_object_key or None
    def operation() -> str:
        with dashboard_connection() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("SELECT id FROM bronze_processing_runs WHERE airflow_run_id = %s", (airflow_run_id,))
                existing = cursor.fetchone()
                now = datetime.now(timezone.utc)
                if existing:
                    run_id = str(existing["id"])
                    cursor.execute(
                        """
                        UPDATE bronze_processing_runs
                        SET status = 'running',
                            scope = COALESCE(NULLIF(scope, ''), %s),
                            source_id = COALESCE(source_id, %s),
                            database_name = COALESCE(database_name, %s),
                            collection_name = COALESCE(collection_name, %s),
                            raw_file_id = COALESCE(raw_file_id, %s),
                            raw_object_key = COALESCE(raw_object_key, %s),
                            retry_failed = retry_failed OR %s,
                            started_at = COALESCE(started_at, %s),
                            finished_at = NULL,
                            duration_seconds = NULL,
                            current_phase = 'initializing',
                            completed_databases = 0,
                            completed_collections = 0,
                            completed_raw_files = 0,
                            processed_rows = 0,
                            read_raw_seconds = 0,
                            parse_json_seconds = 0,
                            add_audit_seconds = 0,
                            write_delta_seconds = 0,
                            metadata_update_seconds = 0,
                            validation_seconds = 0,
                            total_timing_seconds = 0,
                            total_files_processed = 0,
                            total_files_skipped = 0,
                            total_rows_written = 0,
                            failed_files = 0,
                            error_message = NULL,
                            progress_updated_at = now(),
                            updated_at = now()
                        WHERE id = %s
                        """,
                        (scope, source_id, database_name, collection_name, raw_file_id, raw_object_key, retry_failed, now, run_id),
                    )
                    return run_id

                run_id = new_id()
                cursor.execute(
                    """
                    INSERT INTO bronze_processing_runs (
                        id, airflow_run_id, status, scope, source_id, database_name, collection_name,
                        raw_file_id, raw_object_key, retry_failed, current_phase, started_at, triggered_by,
                        progress_updated_at
                    )
                    VALUES (%s, %s, 'running', %s, %s, %s, %s, %s, %s, %s, 'initializing', %s, %s, now())
                    """,
                    (
                        run_id,
                        airflow_run_id,
                        scope,
                        source_id,
                        database_name,
                        collection_name,
                        raw_file_id,
                        raw_object_key,
                        retry_failed,
                        now,
                        triggered_by,
                    ),
                )
                return run_id

    return with_bronze_metadata_retry("ensure_bronze_run", operation)


def finish_bronze_run(
    run_id: str,
    status: str,
    total_raw_files_found: int,
    total_files_processed: int,
    total_files_skipped: int,
    total_rows_written: int,
    failed_files: int,
    error_message: str | None = None,
) -> None:
    def operation() -> None:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                current_phase = "completed" if status == "success" else "skipped" if status == "no_new_data" else "failed"
                completed_raw_files = total_files_processed + total_files_skipped + failed_files
                cursor.execute(
                    """
                    UPDATE bronze_processing_runs
                    SET status = %s,
                        current_phase = %s,
                        finished_at = now(),
                        duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now()))),
                        total_raw_files_found = %s,
                        total_raw_files = %s,
                        completed_raw_files = %s,
                        total_files_processed = %s,
                        total_files_skipped = %s,
                        total_rows_written = %s,
                        failed_files = %s,
                        processed_rows = %s,
                        error_message = %s,
                        progress_updated_at = now(),
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        status,
                        current_phase,
                        total_raw_files_found,
                        total_raw_files_found,
                        completed_raw_files,
                        total_files_processed,
                        total_files_skipped,
                        total_rows_written,
                        failed_files,
                        total_rows_written,
                        error_message,
                        run_id,
                    ),
                )

    with_bronze_metadata_retry("finish_bronze_run", operation)


def fetch_raw_files_for_bronze(
    *,
    scope: str = "all_pending",
    source_id: str | None = None,
    database_name: str | None = None,
    collection_name: str | None = None,
    raw_file_id: str | None = None,
    raw_object_key: str | None = None,
    retry_failed: bool = False,
) -> list[dict[str, Any]]:
    init_dashboard_db()
    clauses = []
    params: list[Any] = []
    normalized_scope = (scope or "all_pending").strip().lower()

    if source_id:
        clauses.append("f.source_id::text = %s")
        params.append(source_id)
    if normalized_scope in {"database", "collection", "pending_only"} and database_name:
        clauses.append("f.database_name = %s")
        params.append(database_name)
    if normalized_scope in {"collection", "pending_only"} and collection_name:
        clauses.append("f.collection_name = %s")
        params.append(collection_name)
    if normalized_scope == "raw_file":
        if raw_file_id:
            clauses.append("f.id::text = %s")
            params.append(raw_file_id)
        elif raw_object_key:
            clauses.append("f.object_key = %s")
            params.append(raw_object_key)
    elif raw_object_key:
        clauses.append("f.object_key = %s")
        params.append(raw_object_key)

    if retry_failed:
        clauses.append("bfs.status = 'failed'")
        clauses.append("COALESCE(bfs.retryable, true) = true")
    elif normalized_scope == "pending_only":
        clauses.append("bfs.id IS NULL")
    else:
        clauses.append("(bfs.id IS NULL OR bfs.status <> 'success')")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT
                    f.*,
                    s.source_name,
                    bfs.status AS bronze_status,
                    bfs.rows_written AS bronze_rows_written,
                    bfs.finished_at AS bronze_finished_at
                FROM raw_files f
                LEFT JOIN source_connections s ON s.id = f.source_id
                LEFT JOIN bronze_file_states bfs ON bfs.raw_file_id = f.id OR (bfs.raw_file_id IS NULL AND bfs.raw_object_key = f.object_key)
                {where}
                ORDER BY f.created_at ASC, f.object_key ASC
                """,
                params,
            )
            return [as_dict(row) for row in cursor.fetchall()]


def bronze_file_success_exists(raw_file_id: str, raw_object_key: str) -> bool:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM bronze_file_states
                WHERE status = 'success'
                    AND (raw_file_id = %s OR raw_object_key = %s)
                LIMIT 1
                """,
                (raw_file_id, raw_object_key),
            )
            return cursor.fetchone() is not None


def mark_bronze_file_processing(
    *,
    bronze_run_id: str,
    raw_file_id: str,
    raw_object_key: str,
    source_id: str | None,
    database_name: str,
    collection_name: str,
    raw_row_count: int,
    raw_file_size_bytes: int,
    raw_file_checksum: str | None,
    bronze_table_path: str,
) -> dict[str, str]:
    init_dashboard_db()
    state_id = new_id()
    attempt_id = new_id()
    def operation() -> dict[str, str]:
        with dashboard_connection() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(
                    """
                    INSERT INTO bronze_file_attempts (
                        id, bronze_run_id, raw_file_id, raw_object_key, source_id, database_name, collection_name,
                        raw_row_count, raw_file_size_bytes, raw_file_checksum, status,
                        bronze_table_path, rows_written, started_at, finished_at, duration_seconds,
                        failed_step, error_type, error_message, stack_trace_summary, retryable, recommended_fix
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'processing',
                        %s, 0, now(), NULL, NULL, NULL, NULL, NULL, NULL, false, NULL
                    )
                    """,
                    (
                        attempt_id,
                        bronze_run_id,
                        raw_file_id,
                        raw_object_key,
                        source_id,
                        database_name,
                        collection_name,
                        raw_row_count,
                        raw_file_size_bytes,
                        raw_file_checksum,
                        bronze_table_path,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO bronze_file_states (
                        id, bronze_run_id, raw_file_id, raw_object_key, source_id, database_name, collection_name,
                        raw_row_count, raw_file_size_bytes, raw_file_checksum, status,
                        bronze_table_path, rows_written, started_at, finished_at, duration_seconds,
                        failed_step, error_type, error_message, stack_trace_summary, retryable, recommended_fix
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'processing',
                        %s, 0, now(), NULL, NULL, NULL, NULL, NULL, NULL, false, NULL
                    )
                    ON CONFLICT (raw_object_key)
                    DO UPDATE SET
                        bronze_run_id = EXCLUDED.bronze_run_id,
                        raw_file_id = EXCLUDED.raw_file_id,
                        source_id = EXCLUDED.source_id,
                        database_name = EXCLUDED.database_name,
                        collection_name = EXCLUDED.collection_name,
                        raw_row_count = EXCLUDED.raw_row_count,
                        raw_file_size_bytes = EXCLUDED.raw_file_size_bytes,
                        raw_file_checksum = EXCLUDED.raw_file_checksum,
                        status = 'processing',
                        bronze_table_path = EXCLUDED.bronze_table_path,
                        started_at = now(),
                        finished_at = NULL,
                        duration_seconds = NULL,
                        failed_step = NULL,
                        error_type = NULL,
                        error_message = NULL,
                        stack_trace_summary = NULL,
                        retryable = false,
                        recommended_fix = NULL,
                        updated_at = now()
                    RETURNING id
                    """,
                    (
                        state_id,
                        bronze_run_id,
                        raw_file_id,
                        raw_object_key,
                        source_id,
                        database_name,
                        collection_name,
                        raw_row_count,
                        raw_file_size_bytes,
                        raw_file_checksum,
                        bronze_table_path,
                    ),
                )
                return {"state_id": str(cursor.fetchone()["id"]), "attempt_id": attempt_id}

    return with_bronze_metadata_retry("mark_bronze_file_processing", operation)


def finish_bronze_file_state(
    state_id: str,
    status: str,
    rows_written: int,
    error_message: str | None = None,
    *,
    attempt_id: str | None = None,
    failed_step: str | None = None,
    error_type: str | None = None,
    stack_trace_summary: str | None = None,
    retryable: bool = False,
    recommended_fix: str | None = None,
) -> None:
    def operation() -> None:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE bronze_file_states
                    SET status = %s,
                        rows_written = CASE
                            WHEN %s = 0 AND rows_written > 0 THEN rows_written
                            ELSE %s
                        END,
                        finished_at = now(),
                        duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now()))),
                        failed_step = %s,
                        error_type = %s,
                        error_message = %s,
                        stack_trace_summary = %s,
                        retryable = %s,
                        recommended_fix = %s,
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        status,
                        rows_written,
                        rows_written,
                        failed_step,
                        error_type,
                        error_message,
                        stack_trace_summary,
                        retryable,
                        recommended_fix,
                        state_id,
                    ),
                )
                if attempt_id:
                    cursor.execute(
                        """
                        UPDATE bronze_file_attempts
                        SET status = %s,
                            rows_written = %s,
                            finished_at = now(),
                            duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now()))),
                            failed_step = %s,
                            error_type = %s,
                            error_message = %s,
                            stack_trace_summary = %s,
                            retryable = %s,
                            recommended_fix = %s,
                            updated_at = now()
                        WHERE id = %s
                        """,
                        (
                            status,
                            rows_written,
                            failed_step,
                            error_type,
                            error_message,
                            stack_trace_summary,
                            retryable,
                            recommended_fix,
                            attempt_id,
                        ),
                    )

    with_bronze_metadata_retry("finish_bronze_file_state", operation)


def mark_bronze_files_failed_fallback(
    *,
    bronze_run_id: str,
    raw_files: list[dict[str, Any]],
    bronze_table_path: str,
    failed_step: str,
    error_type: str,
    error_message: str,
    stack_trace_summary: str | None = None,
    retryable: bool = True,
    recommended_fix: str | None = None,
) -> int:
    init_dashboard_db()
    if not raw_files:
        return 0

    def operation() -> int:
        marked = 0
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                for raw_file in raw_files:
                    raw_file_id = str(raw_file["id"])
                    raw_object_key = raw_file["object_key"]
                    source_id = str(raw_file["source_id"]) if raw_file.get("source_id") else None
                    database_name = raw_file["database_name"]
                    collection_name = raw_file["collection_name"]
                    raw_row_count = int(raw_file.get("row_count") or 0)
                    raw_file_size_bytes = int(raw_file.get("file_size_bytes") or 0)
                    raw_file_checksum = raw_file.get("raw_file_checksum")
                    attempt_id = new_id()
                    cursor.execute(
                        """
                        INSERT INTO bronze_file_attempts (
                            id, bronze_run_id, raw_file_id, raw_object_key, source_id, database_name, collection_name,
                            raw_row_count, raw_file_size_bytes, raw_file_checksum, status,
                            bronze_table_path, rows_written, started_at, finished_at, duration_seconds,
                            failed_step, error_type, error_message, stack_trace_summary, retryable, recommended_fix
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'failed',
                            %s, 0, now(), now(), 0,
                            %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            attempt_id,
                            bronze_run_id,
                            raw_file_id,
                            raw_object_key,
                            source_id,
                            database_name,
                            collection_name,
                            raw_row_count,
                            raw_file_size_bytes,
                            raw_file_checksum,
                            bronze_table_path,
                            failed_step,
                            error_type,
                            error_message,
                            stack_trace_summary,
                            retryable,
                            recommended_fix,
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO bronze_file_states (
                            id, bronze_run_id, raw_file_id, raw_object_key, source_id, database_name, collection_name,
                            raw_row_count, raw_file_size_bytes, raw_file_checksum, status,
                            bronze_table_path, rows_written, started_at, finished_at, duration_seconds,
                            failed_step, error_type, error_message, stack_trace_summary, retryable, recommended_fix
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'failed',
                            %s, 0, now(), now(), 0,
                            %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (raw_object_key)
                        DO UPDATE SET
                            bronze_run_id = EXCLUDED.bronze_run_id,
                            raw_file_id = EXCLUDED.raw_file_id,
                            source_id = EXCLUDED.source_id,
                            database_name = EXCLUDED.database_name,
                            collection_name = EXCLUDED.collection_name,
                            raw_row_count = EXCLUDED.raw_row_count,
                            raw_file_size_bytes = EXCLUDED.raw_file_size_bytes,
                            raw_file_checksum = EXCLUDED.raw_file_checksum,
                            status = 'failed',
                            bronze_table_path = EXCLUDED.bronze_table_path,
                            rows_written = CASE
                                WHEN bronze_file_states.status = 'success' THEN bronze_file_states.rows_written
                                ELSE 0
                            END,
                            started_at = COALESCE(bronze_file_states.started_at, now()),
                            finished_at = now(),
                            duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(bronze_file_states.started_at, now()))),
                            failed_step = EXCLUDED.failed_step,
                            error_type = EXCLUDED.error_type,
                            error_message = EXCLUDED.error_message,
                            stack_trace_summary = EXCLUDED.stack_trace_summary,
                            retryable = EXCLUDED.retryable,
                            recommended_fix = EXCLUDED.recommended_fix,
                            updated_at = now()
                        """,
                        (
                            new_id(),
                            bronze_run_id,
                            raw_file_id,
                            raw_object_key,
                            source_id,
                            database_name,
                            collection_name,
                            raw_row_count,
                            raw_file_size_bytes,
                            raw_file_checksum,
                            bronze_table_path,
                            failed_step,
                            error_type,
                            error_message,
                            stack_trace_summary,
                            retryable,
                            recommended_fix,
                        ),
                    )
                    marked += 1
        return marked

    return with_bronze_metadata_retry("mark_bronze_files_failed_fallback", operation)


def latest_bronze_schema_fields(source_id: str | None, database_name: str, collection_name: str) -> list[str]:
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT fields_json
                FROM bronze_schema_snapshots
                WHERE (%s IS NULL OR source_id = %s)
                    AND database_name = %s
                    AND collection_name = %s
                ORDER BY detected_at DESC
                LIMIT 1
                """,
                (source_id, source_id, database_name, collection_name),
            )
            row = cursor.fetchone()
            if not row:
                return []
            fields_json = row["fields_json"] or {}
            if isinstance(fields_json, str):
                fields_json = json.loads(fields_json)
            fields = fields_json.get("fields", [])
            if fields and isinstance(fields[0], dict):
                return [f"{item.get('name')}:{item.get('type')}" for item in fields]
            return list(fields)


def insert_bronze_schema_snapshot(
    *,
    source_id: str | None,
    database_name: str,
    collection_name: str,
    bronze_table_path: str,
    schema_hash: str,
    fields_json: dict[str, Any],
    new_fields: list[str],
    removed_fields: list[str],
    change_type: str,
) -> None:
    def operation() -> None:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO bronze_schema_snapshots (
                        id, source_id, database_name, collection_name, bronze_table_path,
                        schema_hash, fields_json, new_fields_json, removed_fields_json, change_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        new_id(),
                        source_id,
                        database_name,
                        collection_name,
                        bronze_table_path,
                        schema_hash,
                        json_param(fields_json),
                        json_param(new_fields),
                        json_param(removed_fields),
                        change_type,
                    ),
                )

    with_bronze_metadata_retry("insert_bronze_schema_snapshot", operation)


def upsert_bronze_column_mappings(
    *,
    source_id: str | None,
    database_name: str,
    collection_name: str,
    raw_object_key: str,
    mapping_version: str,
    mappings: list[dict[str, Any]],
) -> None:
    init_dashboard_db()

    def operation() -> None:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM bronze_column_mappings
                    WHERE database_name = %s
                        AND collection_name = %s
                        AND raw_object_key = %s
                    """,
                    (database_name, collection_name, raw_object_key),
                )
                if not mappings:
                    return
                cursor.executemany(
                    """
                    INSERT INTO bronze_column_mappings (
                        id, source_id, database_name, collection_name, raw_object_key,
                        raw_field_path, bronze_field_path, reason, mapping_version
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            new_id(),
                            source_id,
                            database_name,
                            collection_name,
                            raw_object_key,
                            item["raw_field_path"],
                            item["bronze_field_path"],
                            item.get("reason") or "renamed",
                            mapping_version,
                        )
                        for item in mappings
                    ],
                )

    with_bronze_metadata_retry("upsert_bronze_column_mappings", operation)


def upsert_bronze_collection_state(
    *,
    source_id: str | None,
    database_name: str,
    collection_name: str,
    bronze_table_path: str,
    trino_table_name: str,
    last_rows_written: int,
    last_schema_hash: str,
    last_processed_raw_object_key: str,
) -> None:
    def operation() -> None:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO bronze_collection_states (
                        id, source_id, database_name, collection_name, bronze_table_path,
                        trino_table_name, last_success_at, last_rows_written, total_rows_written,
                        last_schema_hash, last_processed_raw_object_key
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s)
                    ON CONFLICT (source_id, database_name, collection_name)
                    DO UPDATE SET
                        bronze_table_path = EXCLUDED.bronze_table_path,
                        trino_table_name = EXCLUDED.trino_table_name,
                        last_success_at = now(),
                        last_rows_written = EXCLUDED.last_rows_written,
                        total_rows_written = bronze_collection_states.total_rows_written + EXCLUDED.last_rows_written,
                        last_schema_hash = EXCLUDED.last_schema_hash,
                        last_processed_raw_object_key = EXCLUDED.last_processed_raw_object_key,
                        updated_at = now()
                    """,
                    (
                        new_id(),
                        source_id,
                        database_name,
                        collection_name,
                        bronze_table_path,
                        trino_table_name,
                        last_rows_written,
                        last_rows_written,
                        last_schema_hash,
                        last_processed_raw_object_key,
                    ),
                )

    with_bronze_metadata_retry("upsert_bronze_collection_state", operation)


def normalize_bronze_table_ref(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    text = str(value).strip()
    if not text:
        return None, None
    if "." in text:
        database_name, collection_name = text.split(".", 1)
        return database_name.strip() or None, collection_name.strip() or None
    if "__" in text:
        database_name, collection_name = text.split("__", 1)
        return database_name.strip() or None, collection_name.strip() or None
    return None, text


def silver_scope_filters(
    *,
    scope: str = "all_pending",
    database_name: str | None = None,
    collection_name: str | None = None,
    bronze_table: str | None = None,
    bronze_file: str | None = None,
    retry_failed: bool = False,
    table_alias: str = "bcs",
    batch_alias: str | None = None,
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    normalized_scope = (scope or "all_pending").strip().lower()
    table_database, table_collection = normalize_bronze_table_ref(bronze_table)
    effective_database = database_name or table_database
    effective_collection = collection_name or table_collection

    if normalized_scope in {"database", "collection", "bronze_table", "bronze_file", "table", "file"} and effective_database:
        clauses.append(f"{table_alias}.database_name = %s")
        params.append(effective_database)
    if normalized_scope in {"collection", "bronze_table", "bronze_file", "table", "file"} and effective_collection:
        clauses.append(f"{table_alias}.collection_name = %s")
        params.append(effective_collection)

    if batch_alias and bronze_file:
        clauses.append(f"({batch_alias}.raw_file_id::text = %s OR {batch_alias}.raw_object_key = %s OR {batch_alias}.id::text = %s)")
        params.extend([bronze_file, bronze_file, bronze_file])

    if batch_alias and retry_failed:
        clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM silver_batch_fingerprints sbf
                WHERE sbf.bronze_batch_id = COALESCE(%s.raw_file_id::text, %s.raw_object_key, %s.id::text)
                  AND sbf.status = 'failed'
                  AND COALESCE(sbf.retryable, true) = true
            )
            """
            % (batch_alias, batch_alias, batch_alias)
        )
    return clauses, params


def record_silver_run_event(
    run_id: str,
    event_type: str,
    message: str | None = None,
    *,
    database_name: str | None = None,
    collection_name: str | None = None,
    bronze_table: str | None = None,
    bronze_file: str | None = None,
    batch_id: str | None = None,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_run_events (
                    id, run_id, event_type, message, database_name, collection_name,
                    bronze_table, bronze_file, batch_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (new_id(), run_id, event_type, message, database_name, collection_name, bronze_table, bronze_file, batch_id),
            )


def update_silver_run_progress(run_id: str, **fields: Any) -> None:
    allowed = {
        "status",
        "current_database_name",
        "current_collection_name",
        "current_bronze_table",
        "current_bronze_file",
        "current_phase",
        "total_databases",
        "completed_databases",
        "total_collections",
        "completed_collections",
        "total_bronze_tables",
        "completed_bronze_tables",
        "total_batches",
        "completed_batches",
        "processed_rows",
        "bronze_read_seconds",
        "flattening_seconds",
        "child_table_generation_seconds",
        "delta_write_seconds",
        "metadata_update_seconds",
        "profiling_seconds",
        "total_timing_seconds",
        "silver_batch_size",
        "silver_parallel_collections",
        "silver_parallel_tables",
        "total_bronze_batches_found",
        "total_batches_processed",
        "total_batches_skipped",
        "total_rows_written",
        "failed_batches",
        "processed_tables",
        "failed_tables",
        "error_message",
    }
    assignments = []
    values = []
    for key, value in fields.items():
        if key not in allowed:
            continue
        assignments.append(f"{key} = %s")
        values.append(value)
    if not assignments:
        return
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE silver_processing_runs
                SET {", ".join(assignments)},
                    progress_updated_at = now(),
                    updated_at = now()
                WHERE id = %s
                """,
                (*values, run_id),
            )


def silver_run_cancel_requested(run_id: str) -> bool:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT cancel_requested FROM silver_processing_runs WHERE id = %s", (run_id,))
            row = cursor.fetchone()
            return bool(row and row[0])


def ensure_silver_run(
    airflow_run_id: str,
    triggered_by: str = "airflow",
    *,
    scope: str = "all_pending",
    database_name: str | None = None,
    collection_name: str | None = None,
    bronze_table: str | None = None,
    bronze_file: str | None = None,
    retry_failed: bool = False,
    silver_batch_size: int | None = None,
    silver_parallel_collections: int | None = None,
    silver_parallel_tables: int | None = None,
) -> str:
    init_dashboard_db()
    scope = (scope or "all_pending").strip().lower()
    database_name = database_name or None
    collection_name = collection_name or None
    bronze_table = bronze_table or None
    bronze_file = bronze_file or None
    silver_batch_size = int(silver_batch_size or os.environ.get("SILVER_BATCH_SIZE", "50") or 50)
    silver_parallel_collections = int(silver_parallel_collections or os.environ.get("SILVER_PARALLEL_COLLECTIONS", "1") or 1)
    silver_parallel_tables = int(silver_parallel_tables or os.environ.get("SILVER_PARALLEL_TABLES", "1") or 1)
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT id FROM silver_processing_runs WHERE airflow_run_id = %s", (airflow_run_id,))
            existing = cursor.fetchone()
            now = datetime.now(timezone.utc)
            if existing:
                run_id = str(existing["id"])
                cursor.execute(
                    """
                    UPDATE silver_processing_runs
                    SET status = 'running',
                        scope = COALESCE(NULLIF(scope, ''), %s),
                        database_name = COALESCE(database_name, %s),
                        collection_name = COALESCE(collection_name, %s),
                        bronze_table = COALESCE(bronze_table, %s),
                        bronze_file = COALESCE(bronze_file, %s),
                        retry_failed = retry_failed OR %s,
                        started_at = COALESCE(started_at, %s),
                        finished_at = NULL,
                        duration_seconds = NULL,
                        current_phase = 'initializing',
                        current_database_name = NULL,
                        current_collection_name = NULL,
                        current_bronze_table = NULL,
                        current_bronze_file = NULL,
                        total_databases = 0,
                        completed_databases = 0,
                        total_collections = 0,
                        completed_collections = 0,
                        total_bronze_tables = 0,
                        completed_bronze_tables = 0,
                        total_batches = 0,
                        completed_batches = 0,
                        processed_rows = 0,
                        bronze_read_seconds = 0,
                        flattening_seconds = 0,
                        child_table_generation_seconds = 0,
                        delta_write_seconds = 0,
                        metadata_update_seconds = 0,
                        profiling_seconds = 0,
                        total_timing_seconds = 0,
                        cancel_requested = false,
                        silver_batch_size = %s,
                        silver_parallel_collections = %s,
                        silver_parallel_tables = %s,
                        total_bronze_batches_found = 0,
                        total_batches_processed = 0,
                        total_batches_skipped = 0,
                        total_rows_written = 0,
                        failed_batches = 0,
                        processed_tables = 0,
                        failed_tables = 0,
                        failed_table_names_json = '[]'::jsonb,
                        error_message = NULL,
                        progress_updated_at = now(),
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        scope,
                        database_name,
                        collection_name,
                        bronze_table,
                        bronze_file,
                        retry_failed,
                        now,
                        silver_batch_size,
                        silver_parallel_collections,
                        silver_parallel_tables,
                        run_id,
                    ),
                )
                return run_id

            run_id = new_id()
            cursor.execute(
                """
                INSERT INTO silver_processing_runs (
                    id, airflow_run_id, status, scope, database_name, collection_name,
                    bronze_table, bronze_file, retry_failed, current_phase, started_at,
                    triggered_by, progress_updated_at, silver_batch_size,
                    silver_parallel_collections, silver_parallel_tables
                )
                VALUES (%s, %s, 'running', %s, %s, %s, %s, %s, %s, 'initializing', %s, %s, now(), %s, %s, %s)
                """,
                (
                    run_id,
                    airflow_run_id,
                    scope,
                    database_name,
                    collection_name,
                    bronze_table,
                    bronze_file,
                    retry_failed,
                    now,
                    triggered_by,
                    silver_batch_size,
                    silver_parallel_collections,
                    silver_parallel_tables,
                ),
            )
            return run_id


def finish_silver_run(
    run_id: str,
    status: str,
    total_bronze_batches_found: int,
    total_batches_processed: int,
    total_batches_skipped: int,
    total_rows_written: int,
    failed_batches: int,
    error_message: str | None = None,
    processed_tables: int = 0,
    failed_tables: int = 0,
    failed_table_names: list[str] | None = None,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_processing_runs
                SET status = %s,
                    current_phase = %s,
                    finished_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now()))),
                    total_bronze_batches_found = %s,
                    total_batches_processed = %s,
                    total_batches_skipped = %s,
                    total_rows_written = %s,
                    failed_batches = %s,
                    completed_batches = %s,
                    processed_rows = %s,
                    processed_tables = %s,
                    failed_tables = %s,
                    failed_table_names_json = %s,
                    error_message = %s,
                    progress_updated_at = now(),
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    status,
                    "completed" if status in {"success", "no_new_data", "warning", "unmapped_bronze_tables"} else "cancelled" if status == "cancelled" else "failed",
                    total_bronze_batches_found,
                    total_batches_processed,
                    total_batches_skipped,
                    total_rows_written,
                    failed_batches,
                    total_batches_processed + total_batches_skipped + failed_batches,
                    total_rows_written,
                    processed_tables,
                    failed_tables,
                    json_param(failed_table_names or []),
                    error_message,
                    run_id,
                ),
            )


def fetch_bronze_tables_for_silver(
    *,
    scope: str = "all_pending",
    database_name: str | None = None,
    collection_name: str | None = None,
    bronze_table: str | None = None,
    bronze_file: str | None = None,
    retry_failed: bool = False,
) -> list[dict[str, Any]]:
    init_dashboard_db()
    clauses, params = silver_scope_filters(
        scope=scope,
        database_name=database_name,
        collection_name=collection_name,
        bronze_table=bronze_table,
        bronze_file=bronze_file,
        retry_failed=retry_failed,
        table_alias="bcs",
    )
    if bronze_file:
        clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM bronze_file_states bfs
                WHERE bfs.status = 'success'
                  AND bfs.database_name = bcs.database_name
                  AND bfs.collection_name = bcs.collection_name
                  AND (bfs.raw_file_id::text = %s OR bfs.raw_object_key = %s OR bfs.id::text = %s)
            )
            """
        )
        params.extend([bronze_file, bronze_file, bronze_file])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM bronze_collection_states bcs
                {where}
                ORDER BY database_name ASC, collection_name ASC
                """,
                params,
            )
            return [as_dict(row) for row in cursor.fetchall()]


def fetch_successful_bronze_batches(
    *,
    scope: str = "all_pending",
    database_name: str | None = None,
    collection_name: str | None = None,
    bronze_table: str | None = None,
    bronze_file: str | None = None,
    retry_failed: bool = False,
) -> list[dict[str, Any]]:
    init_dashboard_db()
    clauses = ["bfs.status = 'success'"]
    params: list[Any] = []
    scope_clauses, scope_params = silver_scope_filters(
        scope=scope,
        database_name=database_name,
        collection_name=collection_name,
        bronze_table=bronze_table,
        bronze_file=bronze_file,
        retry_failed=retry_failed,
        table_alias="bfs",
        batch_alias="bfs",
    )
    clauses.extend(scope_clauses)
    params.extend(scope_params)
    where = f"WHERE {' AND '.join(clauses)}"
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM bronze_file_states bfs
                {where}
                ORDER BY finished_at ASC NULLS LAST, raw_object_key ASC
                """,
                params,
            )
            return [as_dict(row) for row in cursor.fetchall()]


def silver_batch_success_exists(silver_table_name: str, bronze_batch_id: str) -> bool:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM silver_batch_fingerprints
                WHERE silver_table_name = %s
                    AND bronze_batch_id = %s
                    AND status = 'success'
                LIMIT 1
                """,
                (silver_table_name, bronze_batch_id),
            )
            return cursor.fetchone() is not None


def silver_successful_batch_ids(silver_table_name: str, bronze_batch_ids: list[str]) -> set[str]:
    ids = [str(value) for value in bronze_batch_ids if value]
    if not ids:
        return set()
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT bronze_batch_id
                FROM silver_batch_fingerprints
                WHERE silver_table_name = %s
                    AND bronze_batch_id = ANY(%s)
                    AND status = 'success'
                """,
                (silver_table_name, ids),
            )
            return {str(row[0]) for row in cursor.fetchall()}


def mark_silver_batch_processing(
    *,
    silver_table_name: str,
    source_database: str,
    source_collection: str,
    bronze_batch_id: str,
    bronze_raw_object_key: str | None,
    input_row_count: int,
    input_checksum: str | None,
) -> str:
    init_dashboard_db()
    state_id = new_id()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO silver_batch_fingerprints (
                    id, silver_table_name, source_database, source_collection,
                    bronze_batch_id, bronze_raw_object_key, input_row_count,
                    input_checksum, status, rows_written, started_at, finished_at, error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'processing', 0, now(), NULL, NULL)
                ON CONFLICT (silver_table_name, bronze_batch_id)
                DO UPDATE SET
                    source_database = EXCLUDED.source_database,
                    source_collection = EXCLUDED.source_collection,
                    bronze_raw_object_key = EXCLUDED.bronze_raw_object_key,
                    input_row_count = EXCLUDED.input_row_count,
                    input_checksum = EXCLUDED.input_checksum,
                    status = 'processing',
                    rows_written = 0,
                    started_at = now(),
                    finished_at = NULL,
                    error_message = NULL,
                    error_type = NULL,
                    failed_step = NULL,
                    recommended_fix = NULL,
                    full_stack_trace = NULL,
                    possibly_corrupt = false,
                    retryable = true,
                    retry_count = CASE
                        WHEN silver_batch_fingerprints.status = 'failed' THEN silver_batch_fingerprints.retry_count + 1
                        ELSE silver_batch_fingerprints.retry_count
                    END,
                    updated_at = now()
                RETURNING id
                """,
                (
                    state_id,
                    silver_table_name,
                    source_database,
                    source_collection,
                    bronze_batch_id,
                    bronze_raw_object_key,
                    input_row_count,
                    input_checksum,
                ),
            )
            return str(cursor.fetchone()["id"])


def finish_silver_batch_state(
    state_id: str,
    status: str,
    rows_written: int,
    error_message: str | None = None,
    *,
    error_type: str | None = None,
    failed_step: str | None = None,
    recommended_fix: str | None = None,
    full_stack_trace: str | None = None,
    possibly_corrupt: bool = False,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_batch_fingerprints
                SET status = %s,
                    rows_written = %s,
                    finished_at = now(),
                    error_message = %s,
                    error_type = %s,
                    failed_step = %s,
                    recommended_fix = %s,
                    full_stack_trace = %s,
                    possibly_corrupt = %s,
                    retryable = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    status,
                    rows_written,
                    error_message,
                    error_type,
                    failed_step,
                    recommended_fix,
                    full_stack_trace,
                    possibly_corrupt,
                    status != "success",
                    state_id,
                ),
            )


def queue_silver_processing_batch(
    *,
    run_id: str,
    database_name: str,
    collection_name: str,
    bronze_table: str | None,
    bronze_file: str | None,
    batch_number: int,
    total_batches: int,
) -> str:
    init_dashboard_db()
    state_id = new_id()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO silver_processing_batches (
                    batch_id, run_id, database_name, collection_name, bronze_table, bronze_file,
                    batch_number, total_batches, status, retryable
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'queued', true)
                ON CONFLICT (run_id, bronze_table, bronze_file, batch_number)
                DO UPDATE SET
                    status = CASE
                        WHEN silver_processing_batches.status IN ('success', 'skipped') THEN silver_processing_batches.status
                        WHEN silver_processing_batches.status IN ('processing', 'retrying') THEN silver_processing_batches.status
                        ELSE 'queued'
                    END,
                    total_batches = EXCLUDED.total_batches,
                    finished_at = CASE
                        WHEN silver_processing_batches.status IN ('success', 'skipped') THEN silver_processing_batches.finished_at
                        ELSE NULL
                    END,
                    duration_seconds = CASE
                        WHEN silver_processing_batches.status IN ('success', 'skipped') THEN silver_processing_batches.duration_seconds
                        ELSE NULL
                    END,
                    retryable = CASE
                        WHEN silver_processing_batches.status IN ('success', 'skipped') THEN false
                        ELSE true
                    END,
                    updated_at = now()
                RETURNING batch_id
                """,
                (
                    state_id,
                    run_id,
                    database_name,
                    collection_name,
                    bronze_table,
                    bronze_file,
                    batch_number,
                    total_batches,
                ),
            )
            return str(cursor.fetchone()["batch_id"])


def start_silver_processing_batch(
    *,
    run_id: str,
    database_name: str,
    collection_name: str,
    bronze_table: str | None,
    bronze_file: str | None,
    batch_number: int,
    total_batches: int,
) -> str:
    init_dashboard_db()
    state_id = new_id()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO silver_processing_batches (
                    batch_id, run_id, database_name, collection_name, bronze_table, bronze_file,
                    batch_number, total_batches, status, started_at, retryable
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'processing', now(), true)
                ON CONFLICT (run_id, bronze_table, bronze_file, batch_number)
                DO UPDATE SET
                    status = CASE
                        WHEN silver_processing_batches.status = 'failed' THEN 'retrying'
                        ELSE 'processing'
                    END,
                    started_at = now(),
                    finished_at = NULL,
                    duration_seconds = NULL,
                    rows_processed = 0,
                    total_batches = EXCLUDED.total_batches,
                    retry_count = CASE
                        WHEN silver_processing_batches.status = 'failed' THEN silver_processing_batches.retry_count + 1
                        ELSE silver_processing_batches.retry_count
                    END,
                    failed_step = NULL,
                    error_type = NULL,
                    error_message = NULL,
                    retryable = true,
                    recommended_fix = NULL,
                    updated_at = now()
                RETURNING batch_id
                """,
                (
                    state_id,
                    run_id,
                    database_name,
                    collection_name,
                    bronze_table,
                    bronze_file,
                    batch_number,
                    total_batches,
                ),
            )
            batch_id = str(cursor.fetchone()["batch_id"])
            cursor.execute(
                """
                INSERT INTO silver_run_events (
                    id, run_id, event_type, message, database_name, collection_name,
                    bronze_table, bronze_file, batch_id
                )
                VALUES (%s, %s, 'started', %s, %s, %s, %s, %s, %s)
                """,
                (
                    new_id(),
                    run_id,
                    f"Silver batch {batch_number}/{total_batches} started for {database_name}.{collection_name}",
                    database_name,
                    collection_name,
                    bronze_table,
                    bronze_file,
                    batch_id,
                ),
            )
            return batch_id


def finish_silver_processing_batch(
    batch_id: str,
    status: str,
    rows_processed: int = 0,
    *,
    failed_step: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    retryable: bool = True,
    recommended_fix: str | None = None,
) -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                UPDATE silver_processing_batches
                SET status = %s,
                    rows_processed = %s,
                    finished_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now()))),
                    failed_step = %s,
                    error_type = %s,
                    error_message = %s,
                    retryable = %s,
                    recommended_fix = %s,
                    updated_at = now()
                WHERE batch_id = %s
                RETURNING *
                """,
                (
                    status,
                    rows_processed,
                    failed_step,
                    error_type,
                    error_message,
                    retryable,
                    recommended_fix,
                    batch_id,
                ),
            )
            batch = as_dict(cursor.fetchone())
            if batch:
                event_type = "completed" if status in {"success", "skipped"} else status
                cursor.execute(
                    """
                    INSERT INTO silver_run_events (
                        id, run_id, event_type, message, database_name, collection_name,
                        bronze_table, bronze_file, batch_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        new_id(),
                        batch["run_id"],
                        event_type,
                        f"Silver batch {batch['batch_number']}/{batch['total_batches']} {status} for {batch['database_name']}.{batch['collection_name']}",
                        batch.get("database_name"),
                        batch.get("collection_name"),
                        batch.get("bronze_table"),
                        batch.get("bronze_file"),
                        batch_id,
                    ),
                )


def silver_processing_batch_counts(run_id: str) -> dict[str, int]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT count(*) AS total_batches,
                       count(*) FILTER (WHERE status IN ('success', 'skipped')) AS completed_batches,
                       count(*) FILTER (WHERE status = 'queued') AS queued_batches,
                       count(*) FILTER (WHERE status IN ('processing', 'retrying')) AS running_batches,
                       count(*) FILTER (WHERE status = 'failed') AS failed_batches,
                       count(*) FILTER (WHERE status = 'skipped') AS skipped_batches,
                       count(*) FILTER (WHERE status = 'cancelled') AS cancelled_batches,
                       COALESCE(sum(rows_processed), 0) AS rows_processed
                FROM silver_processing_batches
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = as_dict(cursor.fetchone())
    return {key: int(row.get(key) or 0) for key in row}


def mark_interrupted_silver_batches(run_id: str | None = None, timeout_minutes: int = 30) -> int:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            params: list[Any] = [max(1, int(timeout_minutes or 30))]
            run_clause = ""
            if run_id:
                run_clause = "AND run_id = %s"
                params.append(run_id)
            cursor.execute(
                f"""
                UPDATE silver_processing_batches
                SET status = 'failed',
                    finished_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - COALESCE(started_at, updated_at, now()))),
                    failed_step = 'interrupted',
                    error_type = 'Interrupted',
                    error_message = 'Silver batch was running past the interruption timeout and was marked retryable.',
                    retryable = true,
                    recommended_fix = 'Retry failed Silver. Successful Silver batches and fingerprints will be skipped.',
                    updated_at = now()
                WHERE status IN ('processing', 'retrying')
                  AND COALESCE(started_at, updated_at, now()) < now() - (%s || ' minutes')::interval
                  {run_clause}
                """,
                params,
            )
            interrupted = cursor.rowcount
            fingerprint_clause = ""
            fingerprint_params: list[Any] = []
            if run_id:
                fingerprint_clause = """
                  AND EXISTS (
                      SELECT 1
                      FROM silver_processing_batches spb
                      WHERE spb.run_id = %s
                        AND spb.status = 'failed'
                        AND spb.error_type = 'Interrupted'
                        AND spb.database_name = silver_batch_fingerprints.source_database
                        AND spb.collection_name = silver_batch_fingerprints.source_collection
                        AND (
                            spb.bronze_file = silver_batch_fingerprints.bronze_raw_object_key
                            OR spb.bronze_file = silver_batch_fingerprints.bronze_batch_id
                            OR spb.bronze_file IS NULL
                        )
                  )
                """
                fingerprint_params.append(run_id)
            cursor.execute(
                f"""
                UPDATE silver_batch_fingerprints
                SET status = 'failed',
                    finished_at = now(),
                    error_type = 'Interrupted',
                    failed_step = 'interrupted',
                    error_message = 'Silver fingerprint was processing during an interrupted run and was marked retryable.',
                    retryable = true,
                    recommended_fix = 'Retry failed Silver. Successful fingerprints will be skipped.',
                    updated_at = now()
                WHERE status = 'processing'
                  {fingerprint_clause}
                """,
                fingerprint_params,
            )
            return interrupted


def latest_silver_schema_fields(silver_table_name: str) -> list[str]:
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
                (silver_table_name,),
            )
            row = cursor.fetchone()
            if not row:
                return []
            fields_json = row["fields_json"] or {}
            if isinstance(fields_json, str):
                fields_json = json.loads(fields_json)
            fields = fields_json.get("fields", [])
            if fields and isinstance(fields[0], dict):
                return [f"{item.get('name')}:{item.get('type')}" for item in fields]
            return list(fields)


def silver_collection_row_count(silver_table_name: str) -> int | None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT row_count
                FROM silver_collection_states
                WHERE silver_table_name = %s
                """,
                (silver_table_name,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return int(row[0] or 0)


def insert_silver_schema_snapshot(
    *,
    silver_table_name: str,
    schema_hash: str,
    fields_json: dict[str, Any],
    new_fields: list[str],
    removed_fields: list[str],
    change_type: str,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_schema_snapshots (
                    id, silver_table_name, schema_hash, fields_json,
                    new_fields_json, removed_fields_json, change_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    new_id(),
                    silver_table_name,
                    schema_hash,
                    json_param(fields_json),
                    json_param(new_fields),
                    json_param(removed_fields),
                    change_type,
                ),
            )


def upsert_silver_collection_state(
    *,
    silver_table_name: str,
    source_database: str | None,
    source_collection: str | None,
    silver_table_path: str,
    trino_table_name: str,
    primary_key_column: str,
    row_count: int,
    last_rows_written: int,
    last_schema_hash: str,
    flattened_field_count: int,
    partition_info: str,
    last_processed_bronze_batch_id: str | None,
    is_child_table: bool = False,
    parent_silver_table_name: str | None = None,
    child_path: str | None = None,
    table_classification: str = "analytics_ready",
    bi_suitability: str = "unknown",
    governance_status: str = "unknown",
    lineage_reference: str | None = None,
    transform_strategy: str | None = None,
) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_collection_states (
                    id, silver_table_name, source_database, source_collection,
                    silver_table_path, trino_table_name, primary_key_column,
                    row_count, last_rows_written, total_rows_written, last_success_at,
                    last_schema_hash, flattened_field_count, partition_info,
                    last_processed_bronze_batch_id, is_child_table,
                    parent_silver_table_name, child_path, table_classification,
                    bi_suitability, governance_status, lineage_reference,
                    transform_strategy
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (silver_table_name)
                DO UPDATE SET
                    source_database = EXCLUDED.source_database,
                    source_collection = EXCLUDED.source_collection,
                    silver_table_path = EXCLUDED.silver_table_path,
                    trino_table_name = EXCLUDED.trino_table_name,
                    primary_key_column = EXCLUDED.primary_key_column,
                    row_count = EXCLUDED.row_count,
                    last_rows_written = EXCLUDED.last_rows_written,
                    total_rows_written = silver_collection_states.total_rows_written + EXCLUDED.last_rows_written,
                    last_success_at = now(),
                    last_schema_hash = EXCLUDED.last_schema_hash,
                    flattened_field_count = EXCLUDED.flattened_field_count,
                    partition_info = EXCLUDED.partition_info,
                    last_processed_bronze_batch_id = COALESCE(EXCLUDED.last_processed_bronze_batch_id, silver_collection_states.last_processed_bronze_batch_id),
                    is_child_table = EXCLUDED.is_child_table,
                    parent_silver_table_name = EXCLUDED.parent_silver_table_name,
                    child_path = EXCLUDED.child_path,
                    table_classification = EXCLUDED.table_classification,
                    bi_suitability = EXCLUDED.bi_suitability,
                    governance_status = EXCLUDED.governance_status,
                    lineage_reference = EXCLUDED.lineage_reference,
                    transform_strategy = EXCLUDED.transform_strategy,
                    updated_at = now()
                """,
                (
                    new_id(),
                    silver_table_name,
                    source_database,
                    source_collection,
                    silver_table_path,
                    trino_table_name,
                    primary_key_column,
                    row_count,
                    last_rows_written,
                    last_rows_written,
                    last_schema_hash,
                    flattened_field_count,
                    partition_info,
                    last_processed_bronze_batch_id,
                    is_child_table,
                    parent_silver_table_name,
                    child_path,
                    table_classification,
                    bi_suitability,
                    governance_status,
                    lineage_reference,
                    transform_strategy,
                ),
            )


def upsert_silver_transform_plan(plan: dict[str, Any]) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_transform_plans (
                    id, source_bronze_table, source_database, source_collection,
                    generated_silver_tables_json, extracted_columns_json,
                    generated_child_tables_json, raw_json_fallback_fields_json,
                    ignored_fields_json, reasons_json, pii_fields_json,
                    recommendations_json, complexity_score,
                    estimated_analytics_readiness, table_classification,
                    status, plan_json, last_planned_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    COALESCE(%s, now()), now()
                )
                ON CONFLICT (source_database, source_collection)
                DO UPDATE SET
                    source_bronze_table = EXCLUDED.source_bronze_table,
                    generated_silver_tables_json = EXCLUDED.generated_silver_tables_json,
                    extracted_columns_json = EXCLUDED.extracted_columns_json,
                    generated_child_tables_json = EXCLUDED.generated_child_tables_json,
                    raw_json_fallback_fields_json = EXCLUDED.raw_json_fallback_fields_json,
                    ignored_fields_json = EXCLUDED.ignored_fields_json,
                    reasons_json = EXCLUDED.reasons_json,
                    pii_fields_json = EXCLUDED.pii_fields_json,
                    recommendations_json = EXCLUDED.recommendations_json,
                    complexity_score = EXCLUDED.complexity_score,
                    estimated_analytics_readiness = EXCLUDED.estimated_analytics_readiness,
                    table_classification = EXCLUDED.table_classification,
                    status = EXCLUDED.status,
                    plan_json = EXCLUDED.plan_json,
                    last_planned_at = EXCLUDED.last_planned_at,
                    updated_at = now()
                """,
                (
                    new_id(),
                    plan["source_bronze_table"],
                    plan["source_database"],
                    plan["source_collection"],
                    json_param(plan.get("generated_silver_tables") or []),
                    json_param(plan.get("extracted_columns") or []),
                    json_param(plan.get("generated_child_tables") or []),
                    json_param(plan.get("raw_json_fallback_fields") or []),
                    json_param(plan.get("ignored_fields") or []),
                    json_param(plan.get("reasons") or []),
                    json_param(plan.get("pii_fields") or []),
                    json_param(plan.get("recommendations") or []),
                    float(plan.get("complexity_score") or 0),
                    float(plan.get("estimated_analytics_readiness") or 0),
                    plan.get("table_classification") or "analytics_ready",
                    plan.get("status") or "planned",
                    json_param(plan.get("plan") or plan),
                    plan.get("last_planned_at"),
                ),
            )


def replace_silver_field_profiles(table_name: str, profiles: list[dict[str, Any]]) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM silver_field_profiles WHERE table_name = %s", (table_name,))
            for profile in profiles:
                cursor.execute(
                    """
                    INSERT INTO silver_field_profiles (
                        id, table_name, source_database, source_collection, field_path,
                        detected_type, occurrence_count, occurrence_percent,
                        extracted_as_column, extracted_table, raw_json_fallback,
                        flattening_strategy, pii_detected, details_json,
                        last_profiled_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), now())
                    ON CONFLICT (table_name, field_path)
                    DO UPDATE SET
                        source_database = EXCLUDED.source_database,
                        source_collection = EXCLUDED.source_collection,
                        detected_type = EXCLUDED.detected_type,
                        occurrence_count = EXCLUDED.occurrence_count,
                        occurrence_percent = EXCLUDED.occurrence_percent,
                        extracted_as_column = EXCLUDED.extracted_as_column,
                        extracted_table = EXCLUDED.extracted_table,
                        raw_json_fallback = EXCLUDED.raw_json_fallback,
                        flattening_strategy = EXCLUDED.flattening_strategy,
                        pii_detected = EXCLUDED.pii_detected,
                        details_json = EXCLUDED.details_json,
                        last_profiled_at = EXCLUDED.last_profiled_at,
                        updated_at = now()
                    """,
                    (
                        new_id(),
                        table_name,
                        profile.get("source_database"),
                        profile.get("source_collection"),
                        profile["field_path"],
                        profile.get("detected_type") or "unknown",
                        int(profile.get("occurrence_count") or 0),
                        float(profile.get("occurrence_percent") or 0),
                        bool(profile.get("extracted_as_column")),
                        profile.get("extracted_table"),
                        bool(profile.get("raw_json_fallback")),
                        profile.get("flattening_strategy") or "unknown",
                        bool(profile.get("pii_detected")),
                        json_param(profile.get("details") or {}),
                        profile.get("last_profiled_at"),
                    ),
                )


def replace_silver_quality_metrics(silver_table_name: str, metrics: list[dict[str, Any]]) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM silver_quality_metrics WHERE silver_table_name = %s", (silver_table_name,))
            for metric in metrics:
                cursor.execute(
                    """
                    INSERT INTO silver_quality_metrics (
                        id, silver_table_name, metric_name, metric_value, severity, details_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        new_id(),
                        silver_table_name,
                        metric["metric_name"],
                        float(metric.get("metric_value") or 0),
                        metric.get("severity") or "info",
                        json_param(metric.get("details") or {}),
                    ),
                )
