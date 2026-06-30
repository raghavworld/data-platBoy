from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def report(ok: bool, message: str, errors: list[str], details: dict | None = None) -> None:
    print(json.dumps({"status": "ok" if ok else "failed", "check": message, "details": details or {}}, default=str))
    if not ok:
        errors.append(message)


def static_checks(errors: list[str]) -> None:
    query_layer = (ROOT / "scripts" / "query_layer.py").read_text()
    main_api = (ROOT / "dashboard" / "api" / "app" / "main.py").read_text()
    main_js = (ROOT / "dashboard" / "web" / "src" / "main.jsx").read_text()
    report(all(token in query_layer for token in ["safe_view_groups", "missing_safe_views", "failed_queries", "pii_protection_status", "select_only_policy"]), "Query metadata helpers expose grouping, missing views, failed queries, and SELECT-only policy", errors)
    report(all(token in main_api for token in ["/api/query/safe-view-groups", "/api/query/missing-safe-views", "/api/query/failed"]), "Query API exposes UX metadata endpoints", errors)
    report(all(token in main_js for token in ["Safe Views by Database / Collection", "Silver to Safe View Mapping", "Missing / Failed Safe Views", "SELECT-only", "Failed Queries"]), "Query UI shows grouped mappings, missing views, policy, and failed queries", errors)


def live_checks(errors: list[str]) -> None:
    from query_layer import failed_queries, missing_safe_views, safe_view_groups, safe_views_detail

    groups = safe_view_groups()
    views = safe_views_detail()
    missing = missing_safe_views()
    failed = failed_queries(10)
    report(isinstance(groups.get("groups"), list), "Safe views are grouped by database and collection", errors, {"groups": len(groups.get("groups", []))})
    report(all("source_table" in view and "view_name" in view for view in views), "Safe views expose Silver table to safe view mapping", errors, {"views": len(views)})
    report(isinstance(missing, list), "Missing safe views are queryable", errors, {"missing": len(missing)})
    report(isinstance(failed, list), "Failed query history is queryable", errors, {"failed": len(failed)})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    errors: list[str] = []
    static_checks(errors)
    if not args.static_only:
        live_checks(errors)
    if errors:
        raise SystemExit(f"Query UX metadata validation failed: {', '.join(errors)}")
    print("Query UX metadata validation passed")


if __name__ == "__main__":
    main()
