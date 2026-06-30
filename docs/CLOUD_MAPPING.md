# Cloud Mapping

## CORE server

CORE can run on a small VM or app server. The dashboard, Airflow, and Hive Metastore can stay together initially, with Postgres moved to a managed database when reliability requirements increase.

## Storage server or S3

The STORAGE layer uses MinIO locally. In cloud environments, this can map to S3, Azure Blob Storage, GCS, or a dedicated MinIO server. The main migration work is endpoint, credential, bucket, and IAM configuration.

## Query server

The QUERY layer can run Trino on a separate server. It can also be replaced by a managed query service if it supports the required table formats and access-control model.

## Compute server

The COMPUTE layer can move to a larger Spark server, a standalone Spark cluster, EMR, Dataproc, Synapse, Databricks, or another managed compute service.

## BI server

The BI layer can run Superset on its own host or be replaced by a managed BI service. Superset metadata can move from local Postgres to managed Postgres.

## Governance server

The GOVERNANCE layer can run separately because OpenMetadata, its metadata database, and search backend are isolated from the default CORE path.

## External Mongo sources

MongoDB sources remain external in every deployment model. Local native MongoDB, MongoDB Atlas, self-hosted Mongo, or client-owned Mongo clusters should be registered as sources rather than deployed inside this platform.

## Managed Postgres option

`dashboard-postgres` and `airflow-postgres` can be externalized to managed Postgres. Connection strings and credentials should move into environment variables or a secrets manager.

## Managed object storage option

MinIO can be replaced by managed object storage. The platform should keep bucket names, data layout, and table paths stable while changing only storage credentials and endpoints.

## Future Kubernetes option, not now

Kubernetes could later improve scheduling, resilience, secrets management, and service scaling. It is intentionally not introduced yet because Docker Compose is the right size for local-first modular validation.
