from __future__ import annotations

from common import setup_logging
from governance_metadata import validate_main_pipeline_removed_state


def main() -> None:
    logger = setup_logging("validate_main_pipeline_removed")
    logger.info("Validating permanent removal of Airflow main_pipeline DAG")
    result = validate_main_pipeline_removed_state()
    if result["status"] != "ok":
        raise RuntimeError(result)
    logger.info("main_pipeline removal validation passed")


if __name__ == "__main__":
    main()
