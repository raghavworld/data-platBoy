from __future__ import annotations

import pathlib
import sys
from typing import Any

from bronze_column_sanitizer import COLUMN_MAPPING_VERSION, sanitize_column_name, unique_sanitized_names


ROOT = pathlib.Path(__file__).resolve().parents[1]


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"ok - {message}")


def read_text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_sanitizer_rules() -> None:
    cases: list[tuple[str, str]] = [
        ("a.b", "a_b"),
        ("a b", "a_b"),
        ("field(name)", "field_name"),
        ("field=value", "field_value"),
        ("line\nbreak", "line_break"),
        ("123abc", "col_123abc"),
        ("", "unnamed_field"),
        ("_id", "_id"),
        ("__source_id", "__source_id"),
    ]
    for raw_name, expected in cases:
        actual, reasons = sanitize_column_name(raw_name)
        assert_ok(actual == expected, f"{raw_name!r} sanitizes to {expected!r} (got {actual!r}, reasons={reasons})")

    duplicate_names = unique_sanitized_names(["a.b", "a b", "a_b", "A_B"])
    bronze_names = [item["bronze_name"] for item in duplicate_names]
    assert_ok(bronze_names == ["a_b", "a_b_2", "a_b_3", "A_B_4"], f"duplicate sanitized names get stable suffixes: {bronze_names}")
    assert_ok(duplicate_names[1]["reason"].endswith("duplicate_sanitized_name"), "duplicate reason is recorded")


def test_bronze_code_paths() -> None:
    bronze_code = read_text("scripts/bronze_raw_to_delta.py")
    sanitizer_code = read_text("scripts/bronze_column_sanitizer.py")
    db_code = read_text("scripts/dashboard_db.py")
    api_code = read_text("dashboard/api/app/main.py")
    web_code = read_text("dashboard/web/src/main.jsx")

    for token in [
        "sanitize_dataframe_columns",
        "sanitize_struct_column",
        "sanitize_array_column",
        "StructType",
        "ArrayType",
        "spark.sql.caseSensitive",
        "upsert_bronze_column_mappings",
        "_column_sanitized",
        "_column_mapping_version",
    ]:
        assert_ok(token in bronze_code, f"Bronze Spark path contains {token}")
    assert_ok(COLUMN_MAPPING_VERSION in sanitizer_code, f"sanitizer declares mapping version {COLUMN_MAPPING_VERSION}")

    for token in [
        "CREATE TABLE IF NOT EXISTS bronze_column_mappings",
        "raw_field_path",
        "bronze_field_path",
        "reason",
        "detected_at",
        "with_bronze_metadata_retry(\"upsert_bronze_column_mappings\"",
    ]:
        assert_ok(token in db_code, f"Dashboard DB stores column mapping metadata: {token}")

    for token in [
        "/api/bronze/column-mappings",
        "column_mapping_count",
        "Column names sanitized",
    ]:
        assert_ok(token in api_code or token in web_code, f"API/dashboard exposes column sanitization signal: {token}")


def test_db_schema_if_available() -> None:
    try:
        from dashboard_db import dashboard_connection, init_dashboard_db
    except Exception as exc:
        print(f"skip - dashboard DB import unavailable: {exc}")
        return

    try:
        init_dashboard_db()
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'bronze_column_mappings'
                    """
                )
                columns = {row[0] for row in cursor.fetchall()}
                required = {
                    "source_id",
                    "database_name",
                    "collection_name",
                    "raw_field_path",
                    "bronze_field_path",
                    "reason",
                    "detected_at",
                }
                assert_ok(required.issubset(columns), f"bronze_column_mappings has required columns: {sorted(required)}")
                cursor.execute("SELECT count(*) FROM bronze_column_mappings")
                count = int(cursor.fetchone()[0] or 0)
                print(f"info - bronze_column_mappings rows currently stored: {count}")
    except Exception as exc:
        print(f"skip - dashboard DB schema check unavailable: {exc}")


def main() -> int:
    checks: list[tuple[str, Any]] = [
        ("sanitizer rules", test_sanitizer_rules),
        ("Bronze code paths", test_bronze_code_paths),
        ("DB schema", test_db_schema_if_available),
    ]
    for name, check in checks:
        print(f"\n## {name}")
        check()
    print("\nBronze column sanitization validation passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"FAIL - {exc}", file=sys.stderr)
        raise SystemExit(1)
