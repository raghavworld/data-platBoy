# Migration Notes

## Copied from old `local-data-platform`

The new project copied the runtime build contexts and configs needed by Docker: dashboard API/web sources, Docker build folders, Airflow DAGs, platform runtime scripts, Postgres init script, Trino configuration, Superset configuration, and OpenMetadata ingestion examples.

## Intentionally excluded

The old monolithic Compose structure was not reused. Demo/debug Mongo Compose services were not copied into the new Compose files. No Kubernetes files were added.

## Mongo demo exclusion

The new Docker platform does not define `mongo-users`, `mongo-orders`, `mongo-products`, `mongo-payments`, or `mongo-express`. MongoDB is treated as an external source. A local native MongoDB may be used later through `EXTERNAL_MONGO_SOURCE_URI`.

## Data separation

The new project uses bind-mounted data only under `local-data-platform-modular/data/`. It does not reuse or delete `local-data-platform/data/`.

## Possible risks

CORE removes hard startup waits on MinIO, Trino, and Spark so the core services can start independently. DAGs or dashboard actions that actively use optional layers will still require those layers to be started first.

Hive Metastore is configured with the `minio` endpoint for later storage access. If a specific Hive operation touches S3A before STORAGE is running, start `./scripts/up-storage.sh` first.

## Next validation steps

Run CORE first and inspect health with `./scripts/ps.sh`. Then add STORAGE, QUERY, COMPUTE, BI, and GOVERNANCE one layer at a time. Do not start Mongo containers because Mongo is external to this platform.
