#!/bin/sh
set -eu

for db in "$AIRFLOW_DB_NAME" "$SUPERSET_DB_NAME" "$METASTORE_DB_NAME"; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
    SELECT 'CREATE DATABASE "$db"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
done
