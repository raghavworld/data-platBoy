from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from bi_layer import DASHBOARD_LOAD_THRESHOLD_MS, validate_bi_dashboards
from common import PROJECT_ROOT, setup_logging


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def running_inside_container() -> bool:
    return Path("/.dockerenv").exists()


def run_make_validation(logger) -> None:
    logger.info("Validating Makefile target: make validate-bi-dashboards")
    result = subprocess.run(
        ["make", "validate-bi-dashboards"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=int(os.environ.get("VALIDATE_BI_DASHBOARDS_MAKE_TIMEOUT_SECONDS", "900")),
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
    assert_ok(result.returncode == 0, "make validate-bi-dashboards did not pass")
    logger.info("make validate-bi-dashboards passed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-make-check",
        action="store_true",
        help="Skip the host-side make validate-bi-dashboards recursion check. Used by the Makefile target itself.",
    )
    args = parser.parse_args()
    logger = setup_logging("validate_bi_dashboards")
    logger.info("Validating Superset BI dashboards")

    if not args.skip_make_check and not running_inside_container():
        run_make_validation(logger)
        return

    validation = validate_bi_dashboards()
    assert_ok(validation.get("status") == "ok", f"Dashboard validation failed: {validation}")
    slow = [
        check
        for check in validation.get("checks", [])
        if check.get("details", {}).get("load_duration_ms", 0) > DASHBOARD_LOAD_THRESHOLD_MS
    ]
    assert_ok(not slow, f"Dashboard load times exceeded {DASHBOARD_LOAD_THRESHOLD_MS} ms: {slow}")
    logger.info("Superset BI dashboard validation passed")


if __name__ == "__main__":
    main()
