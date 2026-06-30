# Service Matrix

| Service | Layer | Required by default | Heavy or light | Internal port | Host port | Depends on | Can be externalized later | Notes |
|---|---|---:|---|---:|---:|---|---|---|
| dashboard-api | CORE | Yes | Light | 8001 | `${DASHBOARD_API_PORT}` | dashboard-postgres | Yes | Keeps optional URLs for Airflow, MinIO, Superset, and OpenMetadata. |
| dashboard-web | CORE | Yes | Light | 5173 | `${DASHBOARD_WEB_PORT}` | dashboard-api | Yes | Vite dev server preserved. |
| dashboard-postgres | CORE | Yes | Light | 5432 | `${DASHBOARD_POSTGRES_HOST_PORT}` | none | Managed Postgres | Bind mount: `data/core/dashboard-postgres`. |
| airflow-postgres | CORE | Yes | Light | 5432 | `${POSTGRES_HOST_PORT}` | none | Managed Postgres | Also initializes Superset and Hive Metastore databases. |
| airflow-init | CORE | Yes | Light | n/a | n/a | airflow-postgres | No | One-shot Airflow DB/user setup. |
| airflow-webserver | CORE | Yes | Light | 8080 | `${AIRFLOW_PORT}` | airflow-init | Yes | No hard dependency on Spark or Trino at startup. |
| airflow-scheduler | CORE | Yes | Light | n/a | n/a | airflow-init | Yes | DAGs can use optional services when they are running. |
| hive-metastore | CORE | Yes | Light | 9083 | `${HIVE_METASTORE_PORT}` | airflow-postgres | Yes | Configured for `minio:9000`, but CORE does not force MinIO startup. |
| minio | STORAGE | No | Light | 9000, 9001 | `${MINIO_API_PORT}`, `${MINIO_CONSOLE_PORT}` | none | S3 or Azure Blob | Bind mount: `data/storage/minio`. |
| minio-init | STORAGE | No | Light | n/a | n/a | minio | No | Creates buckets. |
| trino | QUERY | No | Heavy | 8080 | `${TRINO_PORT}` | minio, hive-metastore | Managed query engine | Started only by query/full scripts. |
| spark | COMPUTE | No | Heavy | 7077, 8080 | `${SPARK_MASTER_PORT}`, `${SPARK_UI_PORT}` | minio, hive-metastore | Managed Spark or Databricks | Bind mounts under `data/compute`. |
| superset-init | BI | No | Heavy | n/a | n/a | airflow-postgres | No | One-shot Superset setup. |
| superset | BI | No | Heavy | 8088 | `${SUPERSET_PORT}` | superset-init, trino | Managed BI | Script checks Trino before startup. |
| metadata-db | GOVERNANCE | No | Heavy | 3306 | not exposed | none | Managed MySQL/Postgres if supported | Bind mount: `data/governance/metadata-db`. |
| metadata-search | GOVERNANCE | No | Heavy | 9200 | not exposed | none | Managed Elasticsearch/OpenSearch | Bind mount: `data/governance/metadata-search`. |
| openmetadata | GOVERNANCE | No | Heavy | 8585 | `${OPENMETADATA_PORT}` | metadata-db, metadata-search | Managed metadata service | Optional governance layer. |
