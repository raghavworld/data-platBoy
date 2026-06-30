# Architecture

The modular platform is organized into six Docker Compose layers that share one external Docker network: `onov8-data-platform-net`.

## Layers

`CORE` contains the dashboard API and web app, dashboard Postgres, Airflow Postgres, Airflow init/webserver/scheduler, and Hive Metastore. It is the default local control plane.

`STORAGE` contains MinIO and bucket initialization. It is startable independently and can be added after CORE.

`QUERY` contains Trino. It expects MinIO and Hive Metastore to be running, but it starts only when explicitly requested.

`COMPUTE` contains Spark. It expects MinIO and Hive Metastore to be running, but it starts only when explicitly requested.

`BI` contains Superset and Superset initialization. It expects Trino and the CORE metadata database to be available.

`GOVERNANCE` contains OpenMetadata, MySQL, and Elasticsearch. It is optional and starts only when requested.

## Service communication

Services communicate by Docker DNS over the shared network. The important service hostnames are preserved: `dashboard-api`, `dashboard-web`, `dashboard-postgres`, `airflow-postgres`, `airflow-webserver`, `airflow-scheduler`, `hive-metastore`, `minio`, `trino`, `spark`, `superset`, and `openmetadata`.

## Startup order

Recommended local startup order is CORE, STORAGE, QUERY, COMPUTE, BI, then GOVERNANCE. CORE can start without the heavier optional layers. QUERY and COMPUTE require STORAGE plus Hive Metastore. BI requires Trino. GOVERNANCE is isolated from the default path.

## Optional heavy services

Trino, Spark, Superset, OpenMetadata, MySQL for OpenMetadata, and Elasticsearch are treated as optional/heavy services. They can be stopped with `./scripts/down-heavy.sh` without stopping dashboard, Airflow, Hive Metastore, or MinIO.

## Cloud-ready direction

The local layer split maps naturally to cloud or multi-server deployments. Object storage can become S3 or Azure Blob. Postgres can become a managed database. Query, compute, BI, and governance can move onto separate hosts. Kubernetes can be considered later, but it is intentionally not part of the current implementation.
