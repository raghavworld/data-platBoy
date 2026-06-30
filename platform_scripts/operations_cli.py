from __future__ import annotations

import argparse
import json
import os
from typing import Any

import requests

from common import setup_logging


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 300), **kwargs)
    response.raise_for_status()
    return response.json()


def emit(payload: Any) -> None:
    print(json.dumps(payload, indent=2, default=str, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=[
            "health",
            "validate",
            "restart-all",
            "clear-all-logs",
            "replay-failed",
            "rebuild-all",
            "full-platform-wipe",
            "full-wipe-keep-sources",
            "backup",
            "restore",
            "alerts",
            "audit-logs",
            "logs",
        ],
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--backup-type", default="metadata_snapshot")
    parser.add_argument("--file-name")
    args = parser.parse_args()
    logger = setup_logging("operations_cli")

    if args.action == "health":
        payload = request_json("GET", "/api/operations/health", timeout=180)
    elif args.action == "validate":
        payload = request_json("POST", "/api/operations/global/validate", timeout=300)
    elif args.action == "restart-all":
        payload = request_json("POST", "/api/operations/global/restart-all", json={"dry_run": args.dry_run}, timeout=300)
    elif args.action == "clear-all-logs":
        payload = request_json(
            "POST",
            "/api/operations/global/clear-all-logs",
            json={"confirmation": "CLEAR ALL LOGS"},
            timeout=180,
        )
    elif args.action == "replay-failed":
        payload = request_json("POST", "/api/operations/global/replay-downstream", json={"dry_run": args.dry_run}, timeout=300)
    elif args.action == "rebuild-all":
        payload = request_json(
            "POST",
            "/api/operations/global/rebuild",
            json={"scope": "full", "confirmation": "FULL PLATFORM REBUILD", "dry_run": args.dry_run},
            timeout=600,
        )
    elif args.action in {"full-platform-wipe", "full-wipe-keep-sources"}:
        payload = request_json(
            "POST",
            "/api/platform/full-wipe-keep-sources",
            json={
                "confirmation": "FULL WIPE KEEP SOURCES",
                "triggered_by": "operations_cli",
            },
            timeout=1200,
        )
    elif args.action == "backup":
        payload = request_json(
            "POST",
            "/api/platform/backup/export",
            json={"backup_type": args.backup_type},
            timeout=300,
        )
    elif args.action == "restore":
        payload = request_json(
            "POST",
            "/api/platform/backup/restore",
            json={"file_name": args.file_name, "dry_run": True},
            timeout=180,
        )
    elif args.action == "alerts":
        payload = request_json("GET", "/api/platform/alerts", timeout=120)
    elif args.action == "audit-logs":
        payload = request_json("GET", "/api/platform/audit-logs", timeout=120)
    else:
        payload = request_json("GET", "/api/operations/logs", timeout=120)

    logger.info("%s completed with status=%s", args.action, payload.get("status") if isinstance(payload, dict) else "ok")
    emit(payload)


if __name__ == "__main__":
    main()
