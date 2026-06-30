#!/usr/bin/env bash
set -euo pipefail

SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"
SPARK_WORKER_WEBUI_PORT="${SPARK_WORKER_WEBUI_PORT:-8081}"
SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-1g}"
SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"

/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host spark \
  --port "${SPARK_MASTER_PORT}" \
  --webui-port "${SPARK_MASTER_WEBUI_PORT}" &

sleep 5

exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores "${SPARK_WORKER_CORES}" \
  --memory "${SPARK_WORKER_MEMORY}" \
  --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
  "spark://spark:${SPARK_MASTER_PORT}"
