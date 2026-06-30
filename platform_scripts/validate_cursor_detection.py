#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone

from bson import ObjectId

from ingest_mongo_to_raw import (
    INCREMENTAL_OBJECTID,
    INCREMENTAL_TIMESTAMP,
    SNAPSHOT_FINGERPRINT,
    batch_fingerprint,
    build_incremental_query,
    detect_cursor_plan_from_documents,
)


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def case_updated_at() -> dict[str, str]:
    now = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    docs = [{"_id": ObjectId(), "updatedAt": now}]
    plan = detect_cursor_plan_from_documents(docs, "AUTO")
    assert_ok(plan.ingestion_strategy == INCREMENTAL_TIMESTAMP, "updatedAt should use timestamp incremental strategy")
    assert_ok(plan.detected_cursor_field == "updatedAt", "updatedAt should be detected")
    fingerprint = batch_fingerprint(docs, plan)
    assert_ok(fingerprint["max_cursor_value"] == now.isoformat(), "updatedAt fingerprint should keep max cursor")
    return {"case": "updatedAt", "strategy": plan.ingestion_strategy, "detected": plan.detected_cursor_field or ""}


def case_created_at_only() -> dict[str, str]:
    now = datetime(2026, 5, 11, 12, 5, tzinfo=timezone.utc)
    docs = [{"_id": ObjectId(), "createdAt": now}]
    plan = detect_cursor_plan_from_documents(docs, "updatedAt")
    assert_ok(plan.ingestion_strategy == INCREMENTAL_TIMESTAMP, "createdAt should use timestamp incremental strategy")
    assert_ok(plan.detected_cursor_field == "createdAt", "createdAt should be detected when updatedAt is missing")
    assert_ok(plan.cursor_warning and "updatedAt" in plan.cursor_warning, "missing configured cursor should produce warning")
    return {"case": "createdAt only", "strategy": plan.ingestion_strategy, "detected": plan.detected_cursor_field or ""}


def case_object_id_only() -> dict[str, str]:
    first = ObjectId()
    docs = [{"_id": first, "name": "objectid only"}, {"_id": ObjectId(), "name": "next"}]
    plan = detect_cursor_plan_from_documents(docs, "AUTO")
    assert_ok(plan.ingestion_strategy == INCREMENTAL_OBJECTID, "ObjectId-only collection should use ObjectId strategy")
    query = build_incremental_query(str(first), plan)
    assert_ok("_id" in query and "$gt" in query["_id"], "ObjectId strategy should build _id incremental query")
    return {"case": "_id ObjectId only", "strategy": plan.ingestion_strategy, "detected": plan.detected_cursor_field or ""}


def case_no_cursor_fields() -> dict[str, str]:
    docs = [{"_id": "A1", "name": "snapshot"}, {"_id": "A2", "name": "snapshot"}]
    plan = detect_cursor_plan_from_documents(docs, "AUTO")
    assert_ok(plan.ingestion_strategy == SNAPSHOT_FINGERPRINT, "string-id collection without cursor should use snapshot fingerprint")
    fingerprint = batch_fingerprint(docs, plan)
    assert_ok(fingerprint["batch_checksum"], "snapshot fingerprint should produce checksum")
    return {"case": "no cursor fields", "strategy": plan.ingestion_strategy, "detected": plan.detected_cursor_field or ""}


def case_mixed_documents() -> dict[str, str]:
    now = datetime(2026, 5, 11, 12, 10, tzinfo=timezone.utc)
    docs = [
        {"_id": ObjectId(), "updatedAt": now, "name": "has cursor"},
        {"_id": ObjectId(), "name": "missing cursor"},
    ]
    plan = detect_cursor_plan_from_documents(docs, "updatedAt")
    assert_ok(plan.ingestion_strategy == INCREMENTAL_TIMESTAMP, "mixed documents should still use detected timestamp cursor")
    assert_ok(plan.cursor_warning and "missing" in plan.cursor_warning, "mixed documents should produce cursor warning")
    fingerprint = batch_fingerprint(docs, plan)
    assert_ok(fingerprint["row_count"] == 2, "mixed document fingerprint should include every document")
    return {"case": "mixed documents", "strategy": plan.ingestion_strategy, "detected": plan.detected_cursor_field or ""}


def main() -> int:
    checks = [
        case_updated_at(),
        case_created_at_only(),
        case_object_id_only(),
        case_no_cursor_fields(),
        case_mixed_documents(),
    ]
    print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
