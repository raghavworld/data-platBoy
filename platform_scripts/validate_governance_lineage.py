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
    governance = (ROOT / "scripts" / "governance_metadata.py").read_text()
    main_js = (ROOT / "dashboard" / "web" / "src" / "main.jsx").read_text()
    report(all(token in governance for token in ['asset_type="dataset"', "superset_dataset", "generated_child_table", "safe_view", "powers_dashboard"]), "Governance lineage models Silver, child tables, safe views, datasets, and dashboards", errors)
    report(all(token in governance for token in ["cataloged_datasets", "unsafe_assets", "missing_metadata", "child_tables"]), "Governance overview exposes datasets, child tables, unsafe assets, and missing metadata", errors)
    report(all(token in main_js for token in ["Superset Datasets", "Safe/unsafe", "Sync status", "Child Tables", "Dataset"]), "Governance UI exposes lineage and filters", errors)


def live_checks(errors: list[str]) -> None:
    from governance_metadata import governance_catalog, governance_lineage, governance_overview

    overview = governance_overview()
    catalog = governance_catalog(limit=1000)
    lineage = governance_lineage()
    asset_types = {asset.get("asset_type") for asset in catalog}
    report("dataset" in asset_types or overview.get("cataloged_datasets", 0) == 0, "Governance catalog supports Superset dataset assets", errors, {"asset_types": sorted(asset_types)})
    report(isinstance(lineage.get("edges"), list) and isinstance(lineage.get("timelines"), list), "Governance lineage returns edges and dynamic timelines", errors, {"edges": len(lineage.get("edges", [])), "timelines": len(lineage.get("timelines", []))})
    report("unsafe_assets" in overview and "missing_metadata" in overview, "Governance overview exposes unsafe and missing metadata counts", errors, overview)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    errors: list[str] = []
    static_checks(errors)
    if not args.static_only:
        live_checks(errors)
    if errors:
        raise SystemExit(f"Governance lineage validation failed: {', '.join(errors)}")
    print("Governance lineage validation passed")


if __name__ == "__main__":
    main()
