from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def report(ok: bool, message: str, errors: list[str], details: dict | None = None) -> None:
    print(json.dumps({"status": "ok" if ok else "failed", "check": message, "details": details or {}}, default=str))
    if not ok:
        errors.append(message)


def ensure_bi_layer_body(source: str) -> str:
    match = re.search(r"def ensure_bi_layer\([\s\S]*?\n\ndef dashboard_generation_disabled", source)
    return match.group(0) if match else ""


def static_checks(errors: list[str]) -> None:
    bi_layer = (ROOT / "scripts" / "bi_layer.py").read_text()
    main_api = (ROOT / "dashboard" / "api" / "app" / "main.py").read_text()
    main_js = (ROOT / "dashboard" / "web" / "src" / "main.jsx").read_text()
    body = ensure_bi_layer_body(bi_layer)
    report("DASHBOARD_GENERATION_DISABLED_MESSAGE" in bi_layer and "dashboard_generation_disabled" in bi_layer, "BI layer explicitly disables automatic dashboard generation", errors)
    report("ensure_superset_dashboards(" not in body and "dashboard_specs_for_datasets(" not in body, "Dataset sync path does not create dashboards or chart specs", errors)
    report(all(token in bi_layer for token in ["dataset_to_safe_view_mapping", "dataset_owner", "source_system", "failed_dataset_syncs"]), "BI datasets expose owner/source/mapping and failed sync counts", errors)
    report(all(token in main_js for token in ["Dashboard recommendations will be generated later from selected datasets.", "Delete Marked Generated", "Failed Dataset Syncs"]) and "new_dashboards_only" not in main_js and "regenerate_dashboards" not in main_js, "BI UI is dataset-focused and has no random dashboard generator buttons", errors)
    report("delete_generated_dashboards" in main_api and "generate_new_superset_dashboards" in main_api, "Old generated dashboard cleanup remains available while generation endpoints are disabled", errors)


def live_checks(errors: list[str]) -> None:
    from bi_layer import bi_overview, fetch_bi_datasets

    overview = bi_overview()
    datasets = fetch_bi_datasets()
    report(overview.get("dashboard_generation_status") == "disabled", "BI overview reports dashboard generation disabled", errors, overview)
    report(all("dataset_to_safe_view_mapping" in dataset for dataset in datasets), "BI dataset rows expose dataset to safe view mapping", errors, {"datasets": len(datasets)})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    errors: list[str] = []
    static_checks(errors)
    if not args.static_only:
        live_checks(errors)
    if errors:
        raise SystemExit(f"BI dataset sync validation failed: {', '.join(errors)}")
    print("BI dataset sync validation passed")


if __name__ == "__main__":
    main()
