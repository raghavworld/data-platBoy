from __future__ import annotations

import os

from common import ensure_bucket, setup_logging


def main() -> None:
    logger = setup_logging("create_minio_buckets")
    for bucket_name in (
        os.environ["MINIO_BUCKET_RAW"],
        os.environ["MINIO_BUCKET_DELTA"],
        os.environ["MINIO_BUCKET_AUDIT"],
    ):
        ensure_bucket(bucket_name)
        logger.info("Bucket ready: %s", bucket_name)


if __name__ == "__main__":
    main()
