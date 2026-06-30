# OpenMetadata Profile

OpenMetadata is the local catalog and advanced metadata exploration tool for Phase 6. It is intentionally kept behind the optional `metadata` Compose profile because the full stack is heavy for small Docker Desktop allocations.

```bash
docker compose --profile metadata up -d metadata-db metadata-search openmetadata
```

Notes:

- Expect noticeably higher memory usage when the profile is enabled.
- If Docker Desktop is still limited to 8 GB, raise it to 10-12 GB before enabling metadata services.
- After startup, OpenMetadata should be available at `http://localhost:8585`.

## ONOV8 Data Console Fallback

The ONOV8 Data Console does not replace OpenMetadata. It keeps a fallback governance catalog in dashboard PostgreSQL so daily operational monitoring still works when OpenMetadata is stopped.

Fallback tables:

- `governance_assets`
- `governance_lineage_edges`
- `governance_pii_tags`
- `governance_sync_runs`
- `governance_ownership`

Run the fallback sync:

```bash
make governance-sync
make validate-governance
```

## Optional Ingestion Configs

Best-effort ingestion profiles are provided for environments where the OpenMetadata ingestion package is installed:

- `openmetadata/trino-ingestion.yaml`
- `openmetadata/superset-ingestion.yaml`

The minimum local governance path does not require these configs. The dashboard sync catalogs Silver tables, safe Trino views, Superset dashboards, PII tags, ownership, and lineage in PostgreSQL, then links to OpenMetadata URLs when OpenMetadata is reachable.

## PII and Ownership Defaults

PII classification:

- `PII.Sensitive`: raw email and phone fields
- `PII.Hash`: hash columns such as `email_hash`
- `PII.Safe`: analyst-safe views with no raw PII
- `Business.Critical`: governed analytics assets
- `DataQuality.Validated`: safe views that pass validation

Ownership:

- `data-engineering-team`: Raw, Bronze, Silver
- `analytics-team`: safe views and Superset dashboards
- `platform-team`: Trino and OpenMetadata services
