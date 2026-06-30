from __future__ import annotations

import json
import os
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    array_join,
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    expr,
    lit,
    lower,
    posexplode_outer,
    regexp_replace,
    sha2,
    size,
    sum as spark_sum,
    struct,
    to_date,
    to_json,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import ArrayType, BinaryType, DataType, MapType, NullType, StringType, StructType
from common import hash_strings, s3_client, safe_identifier, setup_logging, sql_string_literal, trino_connection
from dashboard_db import (
    ensure_silver_run,
    fetch_bronze_tables_for_silver,
    fetch_successful_bronze_batches,
    finish_silver_batch_state,
    finish_silver_processing_batch,
    finish_silver_run,
    insert_silver_schema_snapshot,
    latest_silver_schema_fields,
    mark_silver_batch_processing,
    queue_silver_processing_batch,
    record_silver_run_event,
    replace_silver_field_profiles,
    replace_silver_quality_metrics,
    silver_run_cancel_requested,
    start_silver_processing_batch,
    silver_collection_row_count,
    silver_successful_batch_ids,
    update_silver_run_progress,
    upsert_silver_collection_state,
    upsert_silver_transform_plan,
)


@dataclass(frozen=True)
class SilverTarget:
    table_name: str
    source_database: str
    source_collection: str
    primary_key: str
    transform_strategy: str = "specific"
    path_table_name: str | None = None

    @property
    def path(self) -> str:
        table_path_name = self.path_table_name or self.table_name
        return f"s3a://{os.environ['MINIO_BUCKET_DELTA']}/silver/{self.source_database}/{table_path_name}"


@dataclass(frozen=True)
class BronzeTable:
    database_name: str
    collection_name: str
    bronze_table_path: str
    discovery_source: str

    @property
    def table_ref(self) -> str:
        return f"{self.database_name}__{self.collection_name}"


@dataclass(frozen=True)
class DeltaWriteMetrics:
    changed_rows: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    source_rows: int = 0
    metrics_available: bool = False


DEMO_SPECIFIC_TARGETS = [
    SilverTarget("users_clean", "users_service", "users", "user_id"),
    SilverTarget("orders_clean", "orders_service", "orders", "order_id"),
    SilverTarget("order_items_clean", "orders_service", "orders", "order_item_key"),
    SilverTarget("products_clean", "products_service", "products", "product_id"),
    SilverTarget("payments_clean", "payments_service", "payments", "payment_id"),
]

AUDIT_COLUMNS = {
    "_bronze_ingested_at",
    "_raw_bucket",
    "_raw_object_key",
    "_source_id",
    "_source_name",
    "_source_db",
    "_source_collection",
    "_raw_run_id",
    "_raw_file_id",
    "_schema_version",
    "_record_hash",
    "_column_sanitized",
    "_column_mapping_version",
    "bronze_ingestion_date",
}

_REGISTERED_SILVER_TABLES: set[str] | None = None
_SCHEMA_SIGNATURE_CACHE: dict[str, set[str]] = {}
_TRANSFORM_METADATA_SIGNATURE_CACHE: dict[str, str] = {}


def build_spark_session() -> SparkSession:
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    return (
        SparkSession.builder.appName("silver_bronze_to_delta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SILVER_SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", os.environ.get("SILVER_SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.driver.maxResultSize", os.environ.get("SILVER_SPARK_DRIVER_MAX_RESULT_SIZE", "512m"))
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )


def elapsed_since(started_at: float) -> float:
    return max(0.0, time.perf_counter() - started_at)


def add_timing(timings: dict[str, float], key: str, started_at: float) -> None:
    timings[key] = timings.get(key, 0.0) + elapsed_since(started_at)


def safe_unpersist(frame: DataFrame | None) -> None:
    if frame is None:
        return
    try:
        frame.unpersist()
    except Exception:
        pass


def silver_batch_size() -> int:
    return max(1, int(os.environ.get("SILVER_BATCH_SIZE", "50") or 50))


def silver_parallel_collections() -> int:
    return max(1, int(os.environ.get("SILVER_PARALLEL_COLLECTIONS", "1") or 1))


def silver_parallel_tables() -> int:
    return max(1, int(os.environ.get("SILVER_PARALLEL_TABLES", "1") or 1))


def chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + size] for index in range(0, len(items), max(1, size))]


def field_lookup(schema: StructType, name: str):
    lowered = name.lower()
    for field in schema.fields:
        if field.name.lower() == lowered:
            return field
    return None


def maybe_col(frame: DataFrame, name: str) -> Column | None:
    field = field_lookup(frame.schema, name)
    return col(f"`{field.name}`") if field else None


def optional_col(frame: DataFrame, name: str, data_type: str = "string") -> Column:
    value = maybe_col(frame, name)
    return value.cast(data_type) if value is not None else lit(None).cast(data_type)


def nested_col(frame: DataFrame, path: str, data_type: str = "string") -> Column:
    parts = path.split(".")
    root = field_lookup(frame.schema, parts[0])
    if root is None:
        return lit(None).cast(data_type)

    value: Column = col(f"`{root.name}`")
    current_type: DataType = root.dataType
    for part in parts[1:]:
        if not isinstance(current_type, StructType):
            return lit(None).cast(data_type)
        field = field_lookup(current_type, part)
        if field is None:
            return lit(None).cast(data_type)
        value = value.getField(field.name)
        current_type = field.dataType
    return value.cast(data_type)


def optional_json(frame: DataFrame, name: str) -> Column:
    field = field_lookup(frame.schema, name)
    if field is None:
        return lit(None).cast("string")
    return to_json(col(f"`{field.name}`"))


def optional_array_csv(frame: DataFrame, path: str) -> Column:
    parts = path.split(".")
    root = field_lookup(frame.schema, parts[0])
    if root is None:
        return lit(None).cast("string")
    value: Column = col(f"`{root.name}`")
    current_type: DataType = root.dataType
    for part in parts[1:]:
        if not isinstance(current_type, StructType):
            return lit(None).cast("string")
        field = field_lookup(current_type, part)
        if field is None:
            return lit(None).cast("string")
        value = value.getField(field.name)
        current_type = field.dataType
    if isinstance(current_type, ArrayType):
        return array_join(value.cast("array<string>"), ",")
    return value.cast("string")


def clean_text(value: Column) -> Column:
    return trim(value.cast("string"))


def normalized_enum(value: Column) -> Column:
    return lower(regexp_replace(trim(value.cast("string")), r"[\s-]+", "_"))


def normalized_upper(value: Column) -> Column:
    return upper(trim(value.cast("string")))


def timestamp_col(frame: DataFrame, name: str) -> Column:
    return to_timestamp(optional_col(frame, name, "string"))


def stable_hash(*values: Column) -> Column:
    return sha2(concat_ws("||", *[coalesce(value.cast("string"), lit("")) for value in values]), 256)


def technical_audit(frame: DataFrame) -> list[Column]:
    return [
        optional_col(frame, "_source_id").alias("source_id"),
        optional_col(frame, "_source_name").alias("source_name"),
        optional_col(frame, "_source_db").alias("source_database"),
        optional_col(frame, "_source_collection").alias("source_collection"),
        optional_col(frame, "_raw_run_id").alias("raw_run_id"),
        optional_col(frame, "_raw_file_id").alias("raw_file_id"),
        optional_col(frame, "_raw_object_key").alias("raw_object_key"),
        optional_col(frame, "_record_hash").alias("bronze_record_hash"),
        to_timestamp(optional_col(frame, "_bronze_ingested_at")).alias("bronze_ingested_at"),
    ]


def with_silver_audit(frame: DataFrame, hash_columns: list[Column]) -> DataFrame:
    return (
        frame.withColumn("silver_record_hash", stable_hash(*hash_columns))
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("silver_ingestion_date", to_date(col("silver_processed_at")))
    )


def transform_users(frame: DataFrame) -> DataFrame:
    country_code = normalized_upper(optional_col(frame, "country"))
    selected = frame.select(
        clean_text(optional_col(frame, "_id")).alias("user_id"),
        clean_text(optional_col(frame, "name")).alias("full_name"),
        lower(clean_text(optional_col(frame, "email"))).alias("email_address"),
        clean_text(optional_col(frame, "phone")).alias("phone_number"),
        country_code.alias("country_code"),
        when(country_code == "AE", "United Arab Emirates")
        .when(country_code == "US", "United States")
        .when(country_code == "SA", "Saudi Arabia")
        .otherwise(lit(None))
        .alias("country_name"),
        clean_text(nested_col(frame, "preferences.theme")).alias("preference_theme"),
        nested_col(frame, "preferences.notifications.email", "boolean").alias("prefers_email_notifications"),
        nested_col(frame, "preferences.notifications.sms", "boolean").alias("prefers_sms_notifications"),
        nested_col(frame, "preferences.newsletter", "boolean").alias("newsletter_opt_in"),
        optional_array_csv(frame, "preferences.segments").alias("segments"),
        optional_array_csv(frame, "preferences.preferredCategories").alias("preferred_categories"),
        optional_json(frame, "preferences").alias("preferences_raw_json"),
        timestamp_col(frame, "createdAt").alias("created_at"),
        timestamp_col(frame, "updatedAt").alias("updated_at"),
        *technical_audit(frame),
    )
    return with_silver_audit(selected, [col("user_id"), col("updated_at"), col("bronze_record_hash")])


def transform_orders(frame: DataFrame) -> DataFrame:
    currency_code = normalized_upper(optional_col(frame, "currency"))
    status = normalized_enum(optional_col(frame, "status"))
    item_count = size(maybe_col(frame, "items")) if maybe_col(frame, "items") is not None else lit(0)
    total_quantity = expr("aggregate(items, 0, (acc, x) -> acc + coalesce(cast(x.qty as int), 0))") if maybe_col(frame, "items") is not None else lit(0)
    selected = frame.select(
        clean_text(optional_col(frame, "_id")).alias("order_id"),
        clean_text(optional_col(frame, "customerId")).alias("customer_id"),
        lower(clean_text(optional_col(frame, "customerEmail"))).alias("customer_email"),
        clean_text(optional_col(frame, "customerPhone")).alias("customer_phone"),
        status.alias("order_status"),
        optional_col(frame, "totalAmount", "decimal(18,2)").alias("total_amount"),
        currency_code.alias("currency_code"),
        item_count.cast("int").alias("item_count"),
        total_quantity.cast("int").alias("total_quantity"),
        clean_text(nested_col(frame, "extraFields.channel")).alias("sales_channel"),
        nested_col(frame, "extraFields.giftWrap", "boolean").alias("gift_wrap"),
        clean_text(nested_col(frame, "extraFields.checkoutExperiment")).alias("checkout_experiment"),
        clean_text(nested_col(frame, "extraFields.priority")).alias("priority"),
        clean_text(nested_col(frame, "extraFields.validation")).alias("validation_tag"),
        clean_text(optional_col(frame, "promoCode")).alias("promo_code"),
        optional_json(frame, "extraFields").alias("extra_fields_raw_json"),
        timestamp_col(frame, "createdAt").alias("created_at"),
        timestamp_col(frame, "updatedAt").alias("updated_at"),
        *technical_audit(frame),
    )
    return with_silver_audit(selected, [col("order_id"), col("updated_at"), col("bronze_record_hash")])


def transform_order_items(frame: DataFrame) -> DataFrame:
    items = maybe_col(frame, "items")
    if items is None:
        return frame.sparkSession.createDataFrame([], schema="order_item_key string")
    exploded = frame.select("*", posexplode_outer(items).alias("item_index", "item"))
    selected = exploded.select(
        stable_hash(
            clean_text(optional_col(exploded, "_id")),
            col("item_index").cast("string"),
            col("item").getField("sku").cast("string"),
        ).alias("order_item_key"),
        clean_text(optional_col(exploded, "_id")).alias("order_id"),
        clean_text(optional_col(exploded, "customerId")).alias("customer_id"),
        col("item_index").cast("int").alias("item_index"),
        clean_text(col("item").getField("sku")).alias("sku"),
        col("item").getField("qty").cast("int").alias("quantity"),
        normalized_enum(optional_col(exploded, "status")).alias("order_status"),
        normalized_upper(optional_col(exploded, "currency")).alias("currency_code"),
        optional_col(exploded, "totalAmount", "decimal(18,2)").alias("order_total_amount"),
        timestamp_col(exploded, "createdAt").alias("order_created_at"),
        timestamp_col(exploded, "updatedAt").alias("order_updated_at"),
        *technical_audit(exploded),
    ).where(col("sku").isNotNull())
    return with_silver_audit(selected, [col("order_item_key"), col("quantity"), col("bronze_record_hash")])


def transform_products(frame: DataFrame) -> DataFrame:
    selected = frame.select(
        clean_text(optional_col(frame, "_id")).alias("product_id"),
        normalized_upper(optional_col(frame, "sku")).alias("sku"),
        clean_text(optional_col(frame, "name")).alias("product_name"),
        normalized_enum(optional_col(frame, "category")).alias("category"),
        optional_col(frame, "price", "decimal(18,2)").alias("price_amount"),
        clean_text(nested_col(frame, "attributes.color")).alias("attribute_color"),
        clean_text(nested_col(frame, "attributes.material")).alias("attribute_material"),
        nested_col(frame, "attributes.dimensions.w", "double").alias("attribute_width"),
        nested_col(frame, "attributes.dimensions.h", "double").alias("attribute_height"),
        nested_col(frame, "attributes.lumens", "int").alias("attribute_lumens"),
        nested_col(frame, "attributes.capacityMl", "int").alias("attribute_capacity_ml"),
        nested_col(frame, "attributes.smart", "boolean").alias("is_smart"),
        nested_col(frame, "attributes.insulated", "boolean").alias("is_insulated"),
        optional_array_csv(frame, "attributes.compatibility").alias("compatibility"),
        optional_array_csv(frame, "attributes.tags").alias("tags"),
        clean_text(nested_col(frame, "supplierMetadata.supplierId")).alias("supplier_id"),
        clean_text(nested_col(frame, "supplierMetadata.contractTier")).alias("supplier_contract_tier"),
        optional_json(frame, "attributes").alias("attributes_raw_json"),
        optional_json(frame, "supplierMetadata").alias("supplier_metadata_raw_json"),
        timestamp_col(frame, "createdAt").alias("created_at"),
        timestamp_col(frame, "updatedAt").alias("updated_at"),
        *technical_audit(frame),
    )
    return with_silver_audit(selected, [col("product_id"), col("updated_at"), col("bronze_record_hash")])


def transform_payments(frame: DataFrame) -> DataFrame:
    selected = frame.select(
        clean_text(optional_col(frame, "_id")).alias("payment_id"),
        clean_text(optional_col(frame, "orderId")).alias("order_id"),
        normalized_enum(optional_col(frame, "method")).alias("payment_method"),
        normalized_enum(optional_col(frame, "status")).alias("payment_status"),
        optional_col(frame, "amount", "decimal(18,2)").alias("amount"),
        clean_text(optional_col(frame, "providerRef")).alias("provider_ref"),
        clean_text(nested_col(frame, "gatewayResponse.avs")).alias("gateway_avs"),
        nested_col(frame, "gatewayResponse.riskScore", "int").alias("gateway_risk_score"),
        normalized_enum(nested_col(frame, "gatewayResponse.processor")).alias("gateway_processor"),
        normalized_upper(nested_col(frame, "gatewayResponse.payerCountry")).alias("payer_country_code"),
        normalized_enum(nested_col(frame, "gatewayResponse.fraudCheck")).alias("fraud_check"),
        normalized_enum(nested_col(frame, "gatewayResponse.deviceAccount")).alias("device_account_status"),
        normalized_enum(nested_col(frame, "gatewayResponse.network")).alias("payment_network"),
        optional_json(frame, "gatewayResponse").alias("gateway_response_raw_json"),
        timestamp_col(frame, "createdAt").alias("created_at"),
        timestamp_col(frame, "updatedAt").alias("updated_at"),
        *technical_audit(frame),
    )
    return with_silver_audit(selected, [col("payment_id"), col("updated_at"), col("bronze_record_hash")])


TRANSFORMS = {
    "users_clean": transform_users,
    "orders_clean": transform_orders,
    "order_items_clean": transform_order_items,
    "products_clean": transform_products,
    "payments_clean": transform_payments,
}


def generic_mapping_enabled() -> bool:
    return os.environ.get("SILVER_ENABLE_GENERIC_MAPPING", "true").strip().lower() not in {"0", "false", "no", "off"}


def demo_specific_mapping_enabled() -> bool:
    return os.environ.get("SILVER_ENABLE_DEMO_SPECIFIC_MAPPING", "false").strip().lower() in {"1", "true", "yes", "on"}


def generic_flatten_max_depth() -> int:
    return max(int(os.environ.get("SILVER_FLATTEN_MAX_DEPTH", os.environ.get("SILVER_GENERIC_FLATTEN_MAX_DEPTH", "4"))), 1)


def generic_flatten_max_columns() -> int:
    return max(int(os.environ.get("SILVER_MAX_COLUMNS_PER_TABLE", os.environ.get("SILVER_GENERIC_FLATTEN_MAX_COLUMNS", "250"))), 20)


def silver_child_table_threshold() -> int:
    return max(int(os.environ.get("SILVER_CHILD_TABLE_THRESHOLD", "5")), 1)


def silver_max_child_tables() -> int:
    return max(int(os.environ.get("SILVER_MAX_CHILD_TABLES", "30")), 0)


def silver_max_child_tables_per_run() -> int:
    return max(int(os.environ.get("SILVER_MAX_CHILD_TABLES_PER_RUN", "40")), 0)


def silver_profile_max_fields() -> int:
    return max(int(os.environ.get("SILVER_PROFILE_MAX_FIELDS", "120")), 10)


def smart_silver_enabled() -> bool:
    return os.environ.get("SILVER_SMART_FLATTENING_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}


def generic_silver_table_name(database_name: str, collection_name: str) -> str:
    return safe_identifier(f"{database_name}__{collection_name}_clean", max_length=120)


def generic_silver_path_table_name(collection_name: str) -> str:
    return safe_identifier(f"{collection_name}_clean", max_length=120)


def generic_target(database_name: str, collection_name: str) -> SilverTarget:
    return SilverTarget(
        table_name=generic_silver_table_name(database_name, collection_name),
        source_database=database_name,
        source_collection=collection_name,
        primary_key="silver_row_id",
        transform_strategy="generic_flatten",
        path_table_name=generic_silver_path_table_name(collection_name),
    )


def targets_for_bronze_table(table: BronzeTable) -> list[SilverTarget]:
    specific = [
        target
        for target in (DEMO_SPECIFIC_TARGETS if demo_specific_mapping_enabled() else [])
        if target.source_database == table.database_name and target.source_collection == table.collection_name
    ]
    if specific:
        return specific
    if generic_mapping_enabled():
        return [generic_target(table.database_name, table.collection_name)]
    return []


def selected_target_names() -> set[str]:
    raw = os.environ.get("SILVER_TARGET_TABLES", "").strip()
    names = {item.strip() for item in raw.split(",") if item.strip()} if raw else set()
    table_name = silver_scope_table_name()
    if table_name:
        names.add(table_name)
    return names


def silver_scope() -> str:
    return (os.environ.get("SILVER_SCOPE") or "all_pending").strip().lower() or "all_pending"


def silver_scope_database() -> str | None:
    return os.environ.get("SILVER_DATABASE_NAME") or None


def silver_scope_collection() -> str | None:
    return os.environ.get("SILVER_COLLECTION_NAME") or None


def silver_scope_table_name() -> str | None:
    return os.environ.get("SILVER_TABLE_NAME") or None


def silver_scope_bronze_table() -> str | None:
    return os.environ.get("SILVER_BRONZE_TABLE") or None


def silver_scope_bronze_file() -> str | None:
    return os.environ.get("SILVER_BRONZE_FILE") or None


def silver_retry_failed() -> bool:
    return str(os.environ.get("SILVER_RETRY_FAILED") or "").strip().lower() in {"1", "true", "yes", "on"}


def quote_identifier(name: str) -> str:
    return f"`{str(name).replace('`', '``')}`"


def unique_alias(name: str, seen: dict[str, int]) -> str:
    base = safe_identifier(name, max_length=110)
    if base not in seen:
        seen[base] = 1
        return base
    seen[base] += 1
    return f"{base}_{seen[base]}"


def generic_column_name(parts: list[str], suffix: str | None = None) -> str:
    raw = "_".join(part for part in parts if part)
    if suffix:
        raw = f"{raw}_{suffix}"
    return safe_identifier(raw, max_length=110)


def field_path_text(path_parts: list[str]) -> str:
    return ".".join(path_parts)


def field_path_slug(path_parts: list[str]) -> str:
    return safe_identifier("_".join(path_parts), max_length=80)


def is_pii_path(path: str) -> bool:
    lowered = path.lower().replace(".", "_")
    return any(
        marker in lowered
        for marker in (
            "email",
            "phone",
            "mobile",
            "emirates_id",
            "emiratesid",
            "passport",
            "fullname",
            "full_name",
            "password",
            "token",
            "secret",
            "credential",
        )
    )


def is_dynamic_config_collection(name: str) -> bool:
    lowered = name.lower()
    return any(marker in lowered for marker in ("userpreference", "preference", "setting", "settings", "filter", "config"))


def classify_silver_table(target: SilverTarget, schema: StructType) -> str:
    collection = target.source_collection.lower()
    table = target.table_name.lower()
    paths = [item["path"] for item in source_field_descriptors(schema)]
    if is_dynamic_config_collection(collection) or is_dynamic_config_collection(table):
        return "dynamic_config_like"
    if any(is_pii_path(path) for path in paths):
        return "pii_sensitive"
    if any(marker in collection or marker in table for marker in ("log", "event", "audit")):
        return "log_like"
    if any(marker in collection or marker in table for marker in ("order", "payment", "transaction", "invoice")):
        return "transactional"
    if any(marker in collection or marker in table for marker in ("master", "role", "permission", "tier", "lookup", "default")):
        return "reference_data"
    return "analytics_ready"


def bi_suitability_for_classification(classification: str) -> str:
    if classification in {"analytics_ready", "transactional", "reference_data"}:
        return "bi_ready"
    if classification in {"dynamic_config_like", "operational_reference", "log_like", "low_analytics_value"}:
        return "limited"
    if classification == "requires_custom_transform":
        return "needs_custom_transform"
    return "governed"


def source_field_descriptors(schema: StructType, prefix: list[str] | None = None, depth: int = 0) -> list[dict[str, Any]]:
    prefix = prefix or []
    descriptors: list[dict[str, Any]] = []
    if depth > generic_flatten_max_depth():
        return descriptors
    for field in schema.fields:
        if not prefix and field.name in AUDIT_COLUMNS:
            continue
        path_parts = [*prefix, field.name]
        path = field_path_text(path_parts)
        descriptors.append({"path": path, "parts": path_parts, "data_type": field.dataType, "type": field.dataType.simpleString(), "depth": depth + 1})
        if isinstance(field.dataType, StructType):
            descriptors.extend(source_field_descriptors(field.dataType, path_parts, depth + 1))
    return descriptors


def dataframe_path_col(path_parts: list[str]) -> Column:
    value: Column = col(quote_identifier(path_parts[0]))
    for part in path_parts[1:]:
        value = value.getField(part)
    return value


def generic_row_id_column(frame: DataFrame, target: SilverTarget) -> Column:
    row_id_source = maybe_col(frame, "_record_hash")
    if row_id_source is not None:
        return row_id_source.cast("string")
    row_id_source = maybe_col(frame, "_id")
    if row_id_source is not None:
        return row_id_source.cast("string")
    business_columns = [col(quote_identifier(field.name)).alias(field.name) for field in frame.schema.fields if field.name not in AUDIT_COLUMNS]
    return stable_hash(lit(target.source_database), lit(target.source_collection), to_json(struct(*business_columns)) if business_columns else lit("{}"))


def normalized_generic_value(value: Column, data_type: DataType, path_parts: list[str] | None = None) -> Column:
    path_parts = path_parts or []
    path_name = "_".join(path_parts).lower()
    if isinstance(data_type, NullType):
        return lit(None).cast("string")
    if isinstance(data_type, BinaryType):
        return value.cast("string")
    if path_name.endswith(("_at", "_datetime", "_timestamp")) or path_name in {"createdat", "updatedat", "created_at", "updated_at"}:
        return to_timestamp(value.cast("string"))
    if path_name.endswith("_date") or path_name.endswith("date"):
        return to_date(value.cast("string"))
    if any(marker in path_name for marker in ("amount", "price", "total", "balance", "cost", "fee")):
        return value.cast("decimal(18,2)")
    if path_name.startswith(("is_", "has_", "can_", "should_")) or path_name.endswith(("_flag", "_enabled", "_active")):
        return value.cast("boolean")
    if any(marker in path_name for marker in ("country_code", "currency_code", "currency")):
        return upper(trim(value.cast("string")))
    if path_name.endswith(("status", "type", "category", "mode")):
        return normalized_enum(value)
    if isinstance(data_type, StringType):
        return trim(value.cast("string"))
    return value


def generic_flatten_columns(
    value: Column,
    data_type: DataType,
    path_parts: list[str],
    seen: dict[str, int],
    child_paths: set[str] | None = None,
) -> list[Column]:
    child_paths = child_paths or set()
    columns: list[Column] = []
    if len(seen) >= generic_flatten_max_columns():
        return columns
    if isinstance(data_type, StructType):
        if len(path_parts) >= generic_flatten_max_depth() or len(seen) >= generic_flatten_max_columns():
            raw_alias = unique_alias(generic_column_name(path_parts, "raw_json"), seen)
            columns.append(to_json(value).alias(raw_alias))
            return columns
        for field in data_type.fields:
            if len(seen) >= generic_flatten_max_columns():
                break
            columns.extend(generic_flatten_columns(value.getField(field.name), field.dataType, [*path_parts, field.name], seen, child_paths))
        return columns
    if isinstance(data_type, ArrayType):
        path = field_path_text(path_parts)
        if path in child_paths:
            alias = unique_alias(generic_column_name(path_parts, "count"), seen)
            columns.append(size(value).cast("int").alias(alias))
            return columns
        raw_alias = unique_alias(generic_column_name(path_parts, "raw_json"), seen)
        columns.append(to_json(value).alias(raw_alias))
        return columns
    if isinstance(data_type, MapType):
        raw_alias = unique_alias(generic_column_name(path_parts, "raw_json"), seen)
        columns.append(to_json(value).alias(raw_alias))
        return columns

    alias = unique_alias(generic_column_name(path_parts), seen)
    columns.append(normalized_generic_value(value, data_type, path_parts).alias(alias))
    return columns


def child_table_name(target: SilverTarget, path_parts: list[str]) -> str:
    base = target.table_name[:-6] if target.table_name.endswith("_clean") else target.table_name
    return safe_identifier(f"{base}_{field_path_slug(path_parts)}_clean", max_length=120)


def child_path_table_name(target: SilverTarget, path_parts: list[str]) -> str:
    return safe_identifier(f"{target.source_collection}_{field_path_slug(path_parts)}_clean", max_length=120)


def build_silver_transform_plan(frame: DataFrame, target: SilverTarget) -> dict[str, Any]:
    descriptors = source_field_descriptors(frame.schema)
    child_tables: list[dict[str, Any]] = []
    extracted_columns: list[dict[str, Any]] = []
    raw_json_fields: list[dict[str, Any]] = []
    ignored_fields: list[dict[str, Any]] = []
    reasons: list[str] = []
    pii_fields: list[str] = []
    max_children = silver_max_child_tables()

    for descriptor in descriptors:
        path = descriptor["path"]
        data_type = descriptor["data_type"]
        path_parts = descriptor["parts"]
        if is_pii_path(path):
            pii_fields.append(path)
        if isinstance(data_type, ArrayType):
            if len(child_tables) < max_children and descriptor["depth"] <= generic_flatten_max_depth():
                table_name = child_table_name(target, path_parts)
                child_tables.append(
                    {
                        "source_path": path,
                        "path_parts": path_parts,
                        "table_name": table_name,
                        "path_table_name": child_path_table_name(target, path_parts),
                        "element_type": data_type.elementType.simpleString(),
                        "reason": "array normalized into relational child Silver table",
                    }
                )
            else:
                raw_json_fields.append({"field_path": path, "reason": "array exceeded child table limits"})
            continue
        if isinstance(data_type, MapType):
            raw_json_fields.append({"field_path": path, "reason": "map/dynamic object preserved as raw_json fallback"})
            continue
        if isinstance(data_type, StructType):
            if descriptor["depth"] >= generic_flatten_max_depth():
                raw_json_fields.append({"field_path": path, "reason": "struct reached configured flatten depth"})
            continue
        extracted_columns.append({"field_path": path, "detected_type": descriptor["type"], "reason": "stable scalar extracted as Silver column"})

    if len(child_tables) >= max_children and max_children:
        reasons.append(f"Child table generation capped at {max_children} table(s).")
    if len(extracted_columns) >= generic_flatten_max_columns():
        reasons.append(f"Column extraction capped at {generic_flatten_max_columns()} column(s).")
    if raw_json_fields:
        reasons.append("Complex or dynamic fields are retained as documented raw_json fallbacks.")
    if child_tables:
        reasons.append("Repeated arrays are normalized into child Silver tables with parent lineage columns.")

    classification = classify_silver_table(target, frame.schema)
    complexity_score = min(100.0, (len(raw_json_fields) * 8.0) + (len(child_tables) * 3.0) + max(0, len(extracted_columns) - 80) * 0.5)
    estimated_readiness = max(0.0, min(100.0, 100.0 - (len(raw_json_fields) * 4.0) - max(0, len(extracted_columns) - 200) * 0.2))
    if classification in {"dynamic_config_like", "log_like"}:
        estimated_readiness = max(estimated_readiness, 70.0)

    generated_tables = [target.table_name, *[item["table_name"] for item in child_tables]]
    recommendations: list[str] = []
    if child_tables:
        recommendations.append("Review generated child Silver tables for BI joins.")
    if raw_json_fields:
        recommendations.append("Consider custom transforms for high-value raw_json fallback fields.")
    if classification in {"dynamic_config_like", "log_like"}:
        recommendations.append("Treat this table as operational/config-like unless business reporting requires a custom model.")

    return {
        "source_bronze_table": f"{target.source_database}__{target.source_collection}",
        "source_database": target.source_database,
        "source_collection": target.source_collection,
        "generated_silver_tables": generated_tables,
        "extracted_columns": extracted_columns[: generic_flatten_max_columns()],
        "generated_child_tables": child_tables,
        "raw_json_fallback_fields": raw_json_fields,
        "ignored_fields": ignored_fields,
        "reasons": reasons,
        "pii_fields": sorted(set(pii_fields)),
        "recommendations": recommendations,
        "complexity_score": round(complexity_score, 1),
        "estimated_analytics_readiness": round(estimated_readiness, 1),
        "table_classification": classification,
        "bi_suitability": bi_suitability_for_classification(classification),
        "status": "planned",
        "plan": {
            "max_depth": generic_flatten_max_depth(),
            "max_columns": generic_flatten_max_columns(),
            "child_table_threshold": silver_child_table_threshold(),
            "max_child_tables": max_children,
        },
    }


def build_field_profiles(frame: DataFrame, target: SilverTarget, plan: dict[str, Any]) -> list[dict[str, Any]]:
    descriptors = source_field_descriptors(frame.schema)[: silver_profile_max_fields()]
    extracted = {item["field_path"] for item in plan.get("extracted_columns", [])}
    child_by_path = {item["source_path"]: item["table_name"] for item in plan.get("generated_child_tables", [])}
    raw_json = {item["field_path"] for item in plan.get("raw_json_fallback_fields", [])}
    total_rows = frame.count()
    occurrence_counts: dict[str, int] = {}
    if descriptors:
        aggregate_columns: list[Column] = []
        descriptor_by_alias: dict[str, dict[str, Any]] = {}
        for index, descriptor in enumerate(descriptors):
            alias = f"field_{index}"
            descriptor_by_alias[alias] = descriptor
            try:
                aggregate_columns.append(
                    spark_sum(when(dataframe_path_col(descriptor["parts"]).isNotNull(), lit(1)).otherwise(lit(0))).cast("long").alias(alias)
                )
            except Exception:
                occurrence_counts[descriptor["path"]] = 0
        if aggregate_columns:
            try:
                counts_row = frame.agg(*aggregate_columns).collect()[0].asDict()
                for alias, descriptor in descriptor_by_alias.items():
                    occurrence_counts[descriptor["path"]] = int(counts_row.get(alias) or 0)
            except Exception:
                for descriptor in descriptors:
                    occurrence_counts.setdefault(descriptor["path"], 0)
    profiles: list[dict[str, Any]] = []
    for descriptor in descriptors:
        path = descriptor["path"]
        occurrence_count = occurrence_counts.get(path, 0)
        occurrence_percent = round((occurrence_count / total_rows) * 100, 2) if total_rows else 0.0
        if path in child_by_path:
            strategy = "child_table"
        elif path in raw_json:
            strategy = "raw_json_fallback"
        elif path in extracted:
            strategy = "column"
        elif isinstance(descriptor["data_type"], StructType):
            strategy = "container"
        else:
            strategy = "ignored"
        profiles.append(
            {
                "source_database": target.source_database,
                "source_collection": target.source_collection,
                "field_path": path,
                "detected_type": descriptor["type"],
                "occurrence_count": occurrence_count,
                "occurrence_percent": occurrence_percent,
                "extracted_as_column": path in extracted,
                "extracted_table": child_by_path.get(path),
                "raw_json_fallback": path in raw_json,
                "flattening_strategy": strategy,
                "pii_detected": is_pii_path(path),
                "details": {"depth": descriptor["depth"]},
            }
        )
    return profiles


def transform_metadata_signature(plan: dict[str, Any]) -> str:
    signature_payload = {
        "generated_silver_tables": plan.get("generated_silver_tables") or [],
        "extracted_columns": plan.get("extracted_columns") or [],
        "generated_child_tables": plan.get("generated_child_tables") or [],
        "raw_json_fallback_fields": plan.get("raw_json_fallback_fields") or [],
        "ignored_fields": plan.get("ignored_fields") or [],
        "table_classification": plan.get("table_classification"),
        "bi_suitability": plan.get("bi_suitability"),
        "plan": plan.get("plan") or {},
    }
    return hash_strings([json.dumps(signature_payload, sort_keys=True, default=str)])


def refresh_transform_metadata_if_needed(frame: DataFrame, target: SilverTarget, plan: dict[str, Any]) -> None:
    signature = transform_metadata_signature(plan)
    if _TRANSFORM_METADATA_SIGNATURE_CACHE.get(target.table_name) == signature:
        return
    upsert_silver_transform_plan(plan)
    replace_silver_field_profiles(target.table_name, build_field_profiles(frame, target, plan))
    _TRANSFORM_METADATA_SIGNATURE_CACHE[target.table_name] = signature


def transform_generic(frame: DataFrame, target: SilverTarget, plan: dict[str, Any] | None = None) -> DataFrame:
    seen: dict[str, int] = {"silver_row_id": 1}
    row_id_source = generic_row_id_column(frame, target)
    child_paths = {item["source_path"] for item in (plan or {}).get("generated_child_tables", [])}

    selected_columns: list[Column] = [row_id_source.cast("string").alias("silver_row_id")]
    for field in frame.schema.fields:
        if field.name in AUDIT_COLUMNS:
            continue
        selected_columns.extend(generic_flatten_columns(col(quote_identifier(field.name)), field.dataType, [field.name], seen, child_paths))

    selected = frame.select(
        *selected_columns,
        *technical_audit(frame),
    )
    return with_silver_audit(selected, [col("silver_row_id"), col("bronze_record_hash")])


def child_value_columns(child_value: Column, data_type: DataType) -> list[Column]:
    seen: dict[str, int] = {"child_record_id": 1}
    if isinstance(data_type, StructType):
        columns: list[Column] = []
        for field in data_type.fields:
            if len(seen) >= generic_flatten_max_columns():
                break
            columns.extend(generic_flatten_columns(child_value.getField(field.name), field.dataType, [field.name], seen, set()))
        if not columns:
            columns.append(to_json(child_value).alias("child_value_raw_json"))
        return columns
    if isinstance(data_type, (ArrayType, MapType)):
        return [to_json(child_value).alias("child_value_raw_json")]
    return [normalized_generic_value(child_value, data_type, ["child_value"]).alias("child_value")]


def build_child_frame(frame: DataFrame, target: SilverTarget, child: dict[str, Any]) -> DataFrame:
    path_parts = child["path_parts"]
    array_field = dataframe_path_col(path_parts)
    exploded = frame.select("*", posexplode_outer(array_field).alias("child_index", "child_value")).where(col("child_value").isNotNull())
    source_db = coalesce(optional_col(exploded, "_source_db"), lit(target.source_database))
    source_collection = coalesce(optional_col(exploded, "_source_collection"), lit(target.source_collection))
    parent_record_id = generic_row_id_column(exploded, target)
    parent_record_hash = coalesce(optional_col(exploded, "_record_hash"), parent_record_id)
    child_path = child["source_path"]
    child_payload = to_json(col("child_value")) if isinstance(child["data_type"].elementType, StructType) else col("child_value").cast("string")
    child_record_id = stable_hash(parent_record_id, lit(child_path), col("child_index").cast("string"), child_payload)
    lineage_reference = stable_hash(parent_record_id, lit(child_path), col("child_index").cast("string"))
    selected = exploded.select(
        child_record_id.alias("child_record_id"),
        parent_record_id.alias("parent_record_id"),
        parent_record_hash.alias("parent_record_hash"),
        source_db.alias("source_db"),
        source_collection.alias("source_collection"),
        lit(target.table_name).alias("source_table"),
        lit(child_path).alias("child_path"),
        col("child_index").cast("int").alias("child_index"),
        current_timestamp().alias("silver_ingested_at"),
        lineage_reference.alias("lineage_reference"),
        *child_value_columns(col("child_value"), child["data_type"].elementType),
    )
    return with_silver_audit(selected, [col("child_record_id"), col("parent_record_hash"), col("lineage_reference")])


def process_smart_child_tables(
    spark: SparkSession,
    frame: DataFrame,
    target: SilverTarget,
    plan: dict[str, Any],
    last_processed_bronze_batch_id: str | None,
    logger,
    child_table_budget: dict[str, int] | None = None,
) -> dict[str, int]:
    totals = {"rows": 0, "processed_tables": 0}
    for child in plan.get("generated_child_tables", []):
        if child_table_budget is not None and child_table_budget.get("remaining", 0) <= 0:
            logger.info("Smart Silver child table generation budget exhausted; remaining child plans for %s are recommendations only", target.table_name)
            break
        child = dict(child)
        descriptor = next(
            (item for item in source_field_descriptors(frame.schema) if item["path"] == child["source_path"]),
            None,
        )
        if not descriptor or not isinstance(descriptor["data_type"], ArrayType):
            continue
        child["data_type"] = descriptor["data_type"]
        child_target = SilverTarget(
            table_name=child["table_name"],
            source_database=target.source_database,
            source_collection=target.source_collection,
            primary_key="child_record_id",
            transform_strategy="smart_child_table",
            path_table_name=child["path_table_name"],
        )
        child_frame = build_child_frame(frame, target, child)
        write_metrics = merge_delta(spark, child_frame, child_target.path, child_target.primary_key)
        rows_written = write_metrics.changed_rows
        if rows_written == 0 and not delta_exists(spark, child_target.path):
            logger.info("Skipped empty Smart Silver child table %s from %s", child_target.table_name, child["source_path"])
            continue
        if delta_exists(spark, child_target.path):
            register_delta_table(child_target.table_name, child_target.path, logger)
        schema_hash, flattened_count = detect_schema(child_frame, child_target.table_name)
        row_count = estimated_table_row_count(spark, child_target, write_metrics)
        replace_silver_quality_metrics(child_target.table_name, quality_metrics(child_frame, child_target, total_rows=row_count))
        upsert_silver_collection_state(
            silver_table_name=child_target.table_name,
            source_database=child_target.source_database,
            source_collection=child_target.source_collection,
            silver_table_path=child_target.path,
            trino_table_name=child_target.table_name,
            primary_key_column=child_target.primary_key,
            row_count=row_count,
            last_rows_written=rows_written,
            last_schema_hash=schema_hash,
            flattened_field_count=flattened_count,
            partition_info="silver_ingestion_date",
            last_processed_bronze_batch_id=last_processed_bronze_batch_id,
            is_child_table=True,
            parent_silver_table_name=target.table_name,
            child_path=child["source_path"],
            table_classification="operational_reference",
            bi_suitability="bi_ready",
            governance_status="lineage_tracked",
            lineage_reference=f"silver.{target.table_name}->silver.{child_target.table_name}",
            transform_strategy="smart_child_table",
        )
        totals["rows"] += rows_written
        totals["processed_tables"] += 1
        if child_table_budget is not None:
            child_table_budget["remaining"] = max(child_table_budget.get("remaining", 0) - 1, 0)
        logger.info("Generated Smart Silver child table %s from %s rows=%s", child_target.table_name, child["source_path"], rows_written)
    return totals


def delta_exists(spark: SparkSession, path: str) -> bool:
    del spark
    return delta_log_exists(path)


def s3_path_parts(path: str) -> tuple[str, str] | None:
    if not path.startswith(("s3a://", "s3://")):
        return None
    _, remainder = path.split("://", 1)
    bucket_name, _, prefix = remainder.partition("/")
    return bucket_name, prefix.strip("/")


def s3_prefix_exists(bucket_name: str, prefix: str) -> bool:
    try:
        response = s3_client().list_objects_v2(Bucket=bucket_name, Prefix=prefix.rstrip("/") + "/", MaxKeys=1)
        return bool(response.get("KeyCount") or response.get("Contents"))
    except Exception:
        return False


def delta_log_exists(path: str) -> bool:
    parts = s3_path_parts(path)
    if not parts:
        return False
    bucket_name, prefix = parts
    if not prefix:
        return False
    return s3_prefix_exists(bucket_name, f"{prefix}/_delta_log")


def path_has_objects(path: str) -> bool:
    parts = s3_path_parts(path)
    if not parts:
        return False
    bucket_name, prefix = parts
    return s3_prefix_exists(bucket_name, prefix)


def bronze_path(database_name: str, collection_name: str) -> str:
    return f"s3a://{os.environ['MINIO_BUCKET_DELTA']}/bronze/{database_name}/{collection_name}"


def parse_bronze_table_ref(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    text = str(value).strip()
    if "." in text:
        database_name, collection_name = text.split(".", 1)
        return database_name or None, collection_name or None
    if "__" in text:
        database_name, collection_name = text.split("__", 1)
        return database_name or None, collection_name or None
    return None, text or None


def table_in_scope(table: BronzeTable, *, scope: str, database_name: str | None, collection_name: str | None, bronze_table: str | None) -> bool:
    table_database, table_collection = parse_bronze_table_ref(bronze_table)
    effective_database = database_name or table_database
    effective_collection = collection_name or table_collection
    normalized = (scope or "all_pending").lower()
    if normalized in {"database", "collection", "bronze_table", "bronze_file", "table", "file"} and effective_database:
        if table.database_name != effective_database:
            return False
    if normalized in {"collection", "bronze_table", "bronze_file", "table", "file"} and effective_collection:
        if table.collection_name != effective_collection:
            return False
    return True


def discover_bronze_delta_log_tables(
    logger,
    *,
    scope: str = "all_pending",
    database_name: str | None = None,
    collection_name: str | None = None,
    bronze_table: str | None = None,
) -> list[BronzeTable]:
    bucket_name = os.environ["MINIO_BUCKET_DELTA"]
    discovered: dict[tuple[str, str], BronzeTable] = {}
    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix="bronze/"):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if "/_delta_log/" not in key:
                continue
            parts = key.split("/")
            if len(parts) < 4 or parts[0] != "bronze" or parts[3] != "_delta_log":
                continue
            discovered_database = parts[1]
            discovered_collection = parts[2]
            if not discovered_database or not discovered_collection:
                continue
            candidate = BronzeTable(
                database_name=discovered_database,
                collection_name=discovered_collection,
                bronze_table_path=bronze_path(discovered_database, discovered_collection),
                discovery_source="delta_log",
            )
            if table_in_scope(candidate, scope=scope, database_name=database_name, collection_name=collection_name, bronze_table=bronze_table):
                discovered[(discovered_database, discovered_collection)] = candidate
    logger.info("Discovered %s physical Bronze Delta table(s) from MinIO", len(discovered))
    return sorted(discovered.values(), key=lambda table: (table.database_name, table.collection_name))


def discover_bronze_tables(
    logger,
    *,
    scope: str = "all_pending",
    database_name: str | None = None,
    collection_name: str | None = None,
    bronze_table: str | None = None,
    bronze_file: str | None = None,
    retry_failed: bool = False,
) -> list[BronzeTable]:
    merged: dict[tuple[str, str], BronzeTable] = {}
    for row in fetch_bronze_tables_for_silver(
        scope=scope,
        database_name=database_name,
        collection_name=collection_name,
        bronze_table=bronze_table,
        bronze_file=bronze_file,
        retry_failed=retry_failed,
    ):
        database_name = row["database_name"]
        collection_name = row["collection_name"]
        merged[(database_name, collection_name)] = BronzeTable(
            database_name=database_name,
            collection_name=collection_name,
            bronze_table_path=row.get("bronze_table_path") or bronze_path(database_name, collection_name),
            discovery_source="metadata",
        )

    if not bronze_file and not retry_failed:
        physical_tables = discover_bronze_delta_log_tables(
            logger,
            scope=scope,
            database_name=database_name,
            collection_name=collection_name,
            bronze_table=bronze_table,
        )
    else:
        physical_tables = []
    for table in physical_tables:
        key = (table.database_name, table.collection_name)
        if key in merged:
            existing = merged[key]
            merged[key] = BronzeTable(
                database_name=existing.database_name,
                collection_name=existing.collection_name,
                bronze_table_path=existing.bronze_table_path or table.bronze_table_path,
                discovery_source="metadata+delta_log",
            )
        else:
            merged[key] = table

    tables = sorted(merged.values(), key=lambda item: (item.database_name, item.collection_name))
    logger.info("Silver detected %s Bronze table(s)", len(tables))
    return tables


def schema_items(data_type: Any, prefix: str = "") -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for field in getattr(data_type, "fields", []):
        name = f"{prefix}.{field.name}" if prefix else field.name
        items.append({"name": name, "type": field.dataType.simpleString(), "nullable": str(field.nullable).lower()})
        items.extend(schema_items(field.dataType, name))
    return items


def detect_schema(frame: DataFrame, table_name: str) -> tuple[str, int]:
    fields = schema_items(frame.schema)
    field_signatures = sorted(f"{item['name']}:{item['type']}" for item in fields)
    schema_hash = hash_strings(field_signatures) if field_signatures else "empty"
    if table_name not in _SCHEMA_SIGNATURE_CACHE:
        _SCHEMA_SIGNATURE_CACHE[table_name] = set(latest_silver_schema_fields(table_name))
    previous_set = _SCHEMA_SIGNATURE_CACHE[table_name]
    current_set = set(field_signatures)
    if not previous_set:
        change_type = "initial"
    elif previous_set == current_set:
        change_type = "unchanged"
    else:
        change_type = "changed"
    if change_type != "unchanged":
        insert_silver_schema_snapshot(
            silver_table_name=table_name,
            schema_hash=schema_hash,
            fields_json={"fields": fields},
            new_fields=sorted(current_set - previous_set),
            removed_fields=sorted(previous_set - current_set),
            change_type=change_type,
        )
        _SCHEMA_SIGNATURE_CACHE[table_name] = current_set
    flattened_count = len([field for field in fields if "." not in field["name"] and field["name"] not in AUDIT_COLUMNS])
    return schema_hash, flattened_count


def latest_delta_write_metrics(spark: SparkSession, path: str) -> DeltaWriteMetrics:
    try:
        row = DeltaTable.forPath(spark, path).history(1).select("operationMetrics").collect()[0]
        raw_metrics = row["operationMetrics"] or {}
    except Exception:
        raw_metrics = {}

    def metric(name: str) -> int:
        try:
            return int(raw_metrics.get(name) or 0)
        except (TypeError, ValueError):
            return 0

    inserted = metric("numTargetRowsInserted") or metric("numOutputRows")
    updated = metric("numTargetRowsUpdated")
    source_rows = metric("numSourceRows") or inserted + updated
    return DeltaWriteMetrics(
        changed_rows=inserted + updated,
        inserted_rows=inserted,
        updated_rows=updated,
        source_rows=source_rows,
        metrics_available=bool(raw_metrics),
    )


def delta_write_metrics_usable(write_metrics: DeltaWriteMetrics) -> bool:
    return write_metrics.metrics_available and (
        write_metrics.source_rows > 0
        or write_metrics.inserted_rows > 0
        or write_metrics.updated_rows > 0
    )


def frame_has_primary_key_rows(frame: DataFrame, primary_key: str) -> bool:
    return frame.select(primary_key).where(col(primary_key).isNotNull()).limit(1).count() > 0


def merge_delta(spark: SparkSession, frame: DataFrame, path: str, primary_key: str) -> DeltaWriteMetrics:
    frame = frame.dropDuplicates([primary_key]).where(col(primary_key).isNotNull())
    path_exists = delta_exists(spark, path)
    if path_exists:
        (
            DeltaTable.forPath(spark, path)
            .alias("target")
            .merge(frame.alias("source"), f"target.{primary_key} = source.{primary_key}")
            .whenMatchedUpdateAll(
                condition=(
                    "target.silver_record_hash IS NULL "
                    "OR source.silver_record_hash IS NULL "
                    "OR target.silver_record_hash <> source.silver_record_hash"
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        if not frame_has_primary_key_rows(frame, primary_key):
            return DeltaWriteMetrics()
        (
            frame.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("silver_ingestion_date")
            .save(path)
        )
    return latest_delta_write_metrics(spark, path)


def register_delta_table(table_name: str, path: str, logger) -> None:
    global _REGISTERED_SILVER_TABLES
    if _REGISTERED_SILVER_TABLES is not None and table_name in _REGISTERED_SILVER_TABLES:
        return
    candidates = [path]
    s3_path = path.replace("s3a://", "s3://", 1)
    if s3_path != path:
        candidates.append(s3_path)
    for table_path in candidates:
        connection = None
        cursor = None
        try:
            connection = trino_connection(schema="silver")
            cursor = connection.cursor()
            cursor.execute("CREATE SCHEMA IF NOT EXISTS delta.silver")
            if _REGISTERED_SILVER_TABLES is None:
                cursor.execute("SHOW TABLES FROM delta.silver")
                _REGISTERED_SILVER_TABLES = {row[0] for row in cursor.fetchall()}
            if table_name in _REGISTERED_SILVER_TABLES:
                return
            cursor.execute(
                f"""
                CALL delta.system.register_table(
                    schema_name => 'silver',
                    table_name => {sql_string_literal(table_name)},
                    table_location => {sql_string_literal(table_path)}
                )
                """
            )
            _REGISTERED_SILVER_TABLES.add(table_name)
            logger.info("Registered Delta table delta.silver.%s at %s", table_name, table_path)
            return
        except Exception as exc:
            logger.warning("Could not register delta.silver.%s at %s: %s", table_name, table_path, exc)
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()
    logger.warning("Silver Delta write completed, but Trino registration was unavailable for %s", table_name)


def table_row_count(spark: SparkSession, path: str) -> int:
    if not delta_exists(spark, path):
        return 0
    return spark.read.format("delta").load(path).count()


def estimated_table_row_count(spark: SparkSession, target: SilverTarget, write_metrics: DeltaWriteMetrics) -> int:
    if not delta_exists(spark, target.path):
        return 0
    previous_count = silver_collection_row_count(target.table_name)
    if not delta_write_metrics_usable(write_metrics):
        if previous_count is not None:
            return int(previous_count)
        if write_metrics.changed_rows:
            return int(write_metrics.changed_rows)
        return table_row_count(spark, target.path)
    if previous_count is None:
        if write_metrics.inserted_rows:
            return write_metrics.inserted_rows
        return table_row_count(spark, target.path)
    return max(0, int(previous_count) + int(write_metrics.inserted_rows))


def quality_metrics(frame: DataFrame, target: SilverTarget, *, total_rows: int | None = None) -> list[dict[str, Any]]:
    validation = frame.agg(
        spark_sum(lit(1)).cast("long").alias("batch_rows"),
        spark_sum(when(col(target.primary_key).isNull(), lit(1)).otherwise(lit(0))).cast("long").alias("null_keys"),
    ).collect()[0].asDict()
    batch_rows = int(validation.get("batch_rows") or 0)
    null_key_count = int(validation.get("null_keys") or 0)
    distinct_keys = frame.select(target.primary_key).where(col(target.primary_key).isNotNull()).distinct().count()
    duplicate_count = max(batch_rows - null_key_count - distinct_keys, 0)
    reported_total_rows = int(total_rows if total_rows is not None else batch_rows)
    metrics = [
        {"metric_name": "row_count", "metric_value": reported_total_rows, "severity": "info", "details": {"primary_key": target.primary_key, "scope": "metadata_estimate"}},
        {"metric_name": "batch_rows_validated", "metric_value": batch_rows, "severity": "info", "details": {"scope": "latest_silver_batch"}},
        {"metric_name": "duplicate_primary_keys", "metric_value": duplicate_count, "severity": "error" if duplicate_count else "ok", "details": {"scope": "latest_silver_batch"}},
        {"metric_name": "null_primary_keys", "metric_value": null_key_count, "severity": "error" if null_key_count else "ok", "details": {"scope": "latest_silver_batch"}},
    ]
    if target.table_name == "orders_clean" and "order_status" in frame.columns:
        invalid = frame.where(~col("order_status").isin("completed", "processing", "pending", "cancelled", "incremental_bronze_test", "incremental_test")).count()
        metrics.append({"metric_name": "invalid_statuses", "metric_value": invalid, "severity": "warn" if invalid else "ok", "details": {"column": "order_status"}})
    if target.table_name == "payments_clean" and "payment_status" in frame.columns:
        invalid = frame.where(~col("payment_status").isin("captured", "authorized", "pending", "failed", "refunded")).count()
        metrics.append({"metric_name": "invalid_statuses", "metric_value": invalid, "severity": "warn" if invalid else "ok", "details": {"column": "payment_status"}})
    if target.table_name == "products_clean" and "price_amount" in frame.columns:
        invalid = frame.where(col("price_amount").isNull() | (col("price_amount") < 0)).count()
        metrics.append({"metric_name": "invalid_prices", "metric_value": invalid, "severity": "warn" if invalid else "ok", "details": {"column": "price_amount"}})
    return metrics


def batch_id(batch: dict[str, Any]) -> str:
    return str(batch.get("raw_file_id") or batch.get("bronze_batch_id") or batch.get("raw_object_key") or batch.get("bronze_raw_object_key"))


def merged_candidate_batches(target: SilverTarget, batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [
        batch for batch in batches
        if batch["database_name"] == target.source_database and batch["collection_name"] == target.source_collection
    ]
    if candidates:
        return candidates
    return [
        {
            "database_name": target.source_database,
            "collection_name": target.source_collection,
            "raw_file_id": None,
            "raw_object_key": f"table_snapshot/{target.source_database}/{target.source_collection}",
            "rows_written": 0,
            "raw_row_count": 0,
            "raw_file_checksum": None,
        }
    ]


def target_has_unprocessed_batches(target: SilverTarget, candidate_batches: list[dict[str, Any]]) -> bool:
    candidate_ids = [batch_id(batch) for batch in candidate_batches]
    successful_ids = silver_successful_batch_ids(target.table_name, candidate_ids)
    return any(candidate_id not in successful_ids for candidate_id in candidate_ids)


def exception_text(exc: Exception) -> str:
    return f"{exc}\n{traceback.format_exc()}"


def is_spark_jvm_crash(exc: Exception) -> bool:
    text = exception_text(exc).lower()
    class_name = exc.__class__.__name__.lower()
    return any(
        marker in text or marker in class_name
        for marker in [
            "connectionrefusederror",
            "connection refused",
            "py4jnetworkerror",
            "py4jerror",
            "py4j does not exist in the jvm",
            "java gateway process exited",
            "gatewayserver",
            "answer from java side is empty",
        ]
    )


def classify_silver_failure(exc: Exception, failed_step: str, target: SilverTarget) -> dict[str, Any]:
    full_trace = exception_text(exc)
    message = str(exc).splitlines()[0] if str(exc).strip() else exc.__class__.__name__
    error_type = exc.__class__.__name__
    recommended_fix = "Inspect the Silver failure details, fix the transform or data issue, then retry this table."
    if is_spark_jvm_crash(exc):
        error_type = "SparkJVMCrashed"
        message = "Spark JVM stopped responding while processing this Silver table."
        recommended_fix = "Restart Spark/Airflow worker or reduce partition/schema complexity, then retry this table."
    elif "is not a delta table" in full_trace.lower() or "delta" in full_trace.lower() and "corrupt" in full_trace.lower():
        error_type = "DeltaTablePossiblyCorrupt"
        recommended_fix = "Flush this Silver table and rebuild it from Bronze."
    elif "cannot resolve" in full_trace.lower() or "analysisexception" in full_trace.lower():
        error_type = "SparkAnalysisError"
        recommended_fix = "Review the Bronze schema and Silver mapping for this table, then retry after correcting incompatible fields."

    possibly_corrupt = path_has_objects(target.path) and (
        not delta_log_exists(target.path)
        or failed_step in {"check_silver_delta", "merge_delta", "detect_schema", "count_silver_rows", "quality_metrics"}
    )
    return {
        "error_type": error_type,
        "message": message[:700],
        "failed_step": failed_step,
        "recommended_fix": recommended_fix,
        "full_stack_trace": full_trace[:24000],
        "possibly_corrupt": possibly_corrupt,
        "spark_crashed": is_spark_jvm_crash(exc),
    }


def backfill_smart_metadata_on_noop(
    spark: SparkSession,
    target: SilverTarget,
    bronze_path: str,
    logger,
    child_table_budget: dict[str, int] | None = None,
) -> dict[str, int]:
    if not smart_silver_enabled() or target.transform_strategy != "generic_flatten":
        return {"rows": 0, "processed_tables": 0}
    try:
        bronze = spark.read.format("delta").load(bronze_path)
        plan = build_silver_transform_plan(bronze, target)
        refresh_transform_metadata_if_needed(bronze, target, plan)
        child_totals = process_smart_child_tables(
            spark,
            bronze,
            target,
            plan,
            "smart_metadata_backfill",
            logger,
            child_table_budget,
        )
        logger.info("Backfilled Smart Silver transform metadata for %s without rewriting Silver data", target.table_name)
        return child_totals
    except Exception as exc:
        logger.warning("Unable to backfill Smart Silver metadata for %s during no-op run: %s", target.table_name, exc)
        return {"rows": 0, "processed_tables": 0}


def mark_silver_phase(
    run_id: str,
    phase: str,
    target: SilverTarget | None = None,
    *,
    bronze_table: str | None = None,
    bronze_file: str | None = None,
    event_type: str | None = None,
    message: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {"current_phase": phase}
    if target:
        payload.update(
            {
                "current_database_name": target.source_database,
                "current_collection_name": target.source_collection,
                "current_bronze_table": bronze_table or f"{target.source_database}__{target.source_collection}",
                "current_bronze_file": bronze_file,
            }
        )
    if extra:
        payload.update(extra)
    update_silver_run_progress(run_id, **payload)
    if event_type:
        record_silver_run_event(
            run_id,
            event_type,
            message,
            database_name=target.source_database if target else None,
            collection_name=target.source_collection if target else None,
            bronze_table=bronze_table,
            bronze_file=bronze_file,
        )


def filter_bronze_frame(bronze: DataFrame, selected_batches: list[dict[str, Any]]) -> DataFrame:
    batch_ids = [str(batch.get("raw_file_id") or "") for batch in selected_batches if batch.get("raw_file_id")]
    raw_object_keys = [str(batch.get("raw_object_key") or "") for batch in selected_batches if batch.get("raw_object_key")]
    filters: list[Column] = []
    raw_file_id_column = maybe_col(bronze, "_raw_file_id")
    raw_object_key_column = maybe_col(bronze, "_raw_object_key")
    if raw_file_id_column is not None and batch_ids:
        filters.append(raw_file_id_column.cast("string").isin(batch_ids))
    if raw_object_key_column is not None and raw_object_keys:
        filters.append(raw_object_key_column.isin(raw_object_keys))
    if not filters:
        return bronze
    filter_expr = filters[0]
    for item in filters[1:]:
        filter_expr = filter_expr | item
    return bronze.where(filter_expr)


def batch_file_label(batch_group: list[dict[str, Any]]) -> str:
    if len(batch_group) == 1:
        return str(batch_group[0].get("raw_object_key") or batch_id(batch_group[0]))
    return f"{len(batch_group)} bronze files"


def process_target(
    spark: SparkSession,
    target: SilverTarget,
    bronze_path: str,
    batches: list[dict[str, Any]],
    logger,
    *,
    run_id: str,
    run_total_batches: int,
    completed_batches: int,
    processed_rows: int,
    bronze_frame: DataFrame | None = None,
    child_table_budget: dict[str, int] | None = None,
) -> dict[str, Any]:
    failed_step = "detect_batches"
    candidate_batches = merged_candidate_batches(target, batches)
    total = len(candidate_batches)
    batch_groups = chunked(candidate_batches, silver_batch_size())
    result: dict[str, Any] = {
        "found": total,
        "processed": 0,
        "skipped": 0,
        "rows": 0,
        "failed": 0,
        "processed_tables": 0,
        "failed_tables": 0,
        "completed_batches": completed_batches,
        "processed_rows": processed_rows,
        "timings": {
            "bronze_read_seconds": 0.0,
            "flattening_seconds": 0.0,
            "child_table_generation_seconds": 0.0,
            "delta_write_seconds": 0.0,
            "metadata_update_seconds": 0.0,
            "profiling_seconds": 0.0,
            "total_timing_seconds": 0.0,
        },
    }

    if not batch_groups:
        return result

    successful_batch_ids = silver_successful_batch_ids(target.table_name, [batch_id(batch) for batch in candidate_batches])
    local_bronze = bronze_frame

    for batch_number, batch_group in enumerate(batch_groups, start=1):
        if silver_run_cancel_requested(run_id):
            mark_silver_phase(
                run_id,
                "cancelled",
                target,
                bronze_table=f"{target.source_database}__{target.source_collection}",
                bronze_file=batch_file_label(batch_group),
                event_type="cancelled",
                message="Silver cancellation requested; remaining batches skipped.",
            )
            result["cancelled"] = 1
            break

        bronze_file_label = batch_file_label(batch_group)
        processing_batch_id = start_silver_processing_batch(
            run_id=run_id,
            database_name=target.source_database,
            collection_name=target.source_collection,
            bronze_table=f"{target.source_database}__{target.source_collection}",
            bronze_file=bronze_file_label,
            batch_number=batch_number,
            total_batches=max(1, len(batch_groups)),
        )
        unprocessed = [batch for batch in batch_group if batch_id(batch) not in successful_batch_ids]
        already_done = len(batch_group) - len(unprocessed)
        result["skipped"] += already_done
        if not unprocessed:
            finish_silver_processing_batch(processing_batch_id, "skipped", 0, retryable=False)
            result["completed_batches"] += 1
            update_silver_run_progress(
                run_id,
                current_phase="skipped",
                current_database_name=target.source_database,
                current_collection_name=target.source_collection,
                current_bronze_table=f"{target.source_database}__{target.source_collection}",
                current_bronze_file=bronze_file_label,
                total_batches=run_total_batches,
                completed_batches=result["completed_batches"],
                total_batches_skipped=result["skipped"],
                processed_rows=result["processed_rows"],
            )
            logger.info("Skipped already-successful Silver batch %s/%s for %s", batch_number, len(batch_groups), target.table_name)
            continue

        states: list[tuple[str, dict[str, Any]]] = []
        for batch in unprocessed:
            current_batch_id = batch_id(batch)
            states.append(
                (
                    mark_silver_batch_processing(
                        silver_table_name=target.table_name,
                        source_database=target.source_database,
                        source_collection=target.source_collection,
                        bronze_batch_id=current_batch_id,
                        bronze_raw_object_key=batch.get("raw_object_key"),
                        input_row_count=int(batch.get("rows_written") or batch.get("raw_row_count") or 0),
                        input_checksum=batch.get("raw_file_checksum"),
                    ),
                    batch,
                )
            )

        batch_started_at = time.perf_counter()
        batch_timings = {
            "bronze_read_seconds": 0.0,
            "flattening_seconds": 0.0,
            "child_table_generation_seconds": 0.0,
            "delta_write_seconds": 0.0,
            "metadata_update_seconds": 0.0,
            "profiling_seconds": 0.0,
            "total_timing_seconds": 0.0,
        }
        rows_written = 0
        filtered: DataFrame | None = None
        silver: DataFrame | None = None
        try:
            failed_step = "load_bronze_delta"
            mark_silver_phase(
                run_id,
                "reading bronze",
                target,
                bronze_table=f"{target.source_database}__{target.source_collection}",
                bronze_file=bronze_file_label,
                event_type=None,
                message=f"Silver batch {batch_number}/{len(batch_groups)} reading Bronze",
            )
            timing_started = time.perf_counter()
            if local_bronze is None:
                local_bronze = spark.read.format("delta").load(bronze_path)
            filtered = filter_bronze_frame(local_bronze, unprocessed).persist()
            add_timing(batch_timings, "bronze_read_seconds", timing_started)

            failed_step = "flattening"
            if target.transform_strategy == "generic_flatten":
                failed_step = "plan_smart_transform"
                mark_silver_phase(run_id, "profiling fields", target, bronze_table=f"{target.source_database}__{target.source_collection}", bronze_file=bronze_file_label, event_type=None, message=f"Profiling fields for {target.table_name}")
                timing_started = time.perf_counter()
                plan = build_silver_transform_plan(filtered, target) if smart_silver_enabled() else {
                    "generated_child_tables": [],
                    "table_classification": "analytics_ready",
                    "bi_suitability": "unknown",
                    "source_database": target.source_database,
                    "source_collection": target.source_collection,
                    "source_bronze_table": f"{target.source_database}__{target.source_collection}",
                }
                if smart_silver_enabled():
                    refresh_transform_metadata_if_needed(filtered, target, plan)
                add_timing(batch_timings, "profiling_seconds", timing_started)

                failed_step = "flattening"
                mark_silver_phase(run_id, "flattening nested arrays", target, bronze_table=f"{target.source_database}__{target.source_collection}", bronze_file=bronze_file_label, event_type=None, message=f"Flattening {target.table_name}")
                timing_started = time.perf_counter()
                mark_silver_phase(run_id, "sanitizing columns", target, bronze_table=f"{target.source_database}__{target.source_collection}", bronze_file=bronze_file_label, event_type=None, message=f"Sanitizing Silver columns for {target.table_name}")
                silver = transform_generic(filtered, target, plan).persist()
                add_timing(batch_timings, "flattening_seconds", timing_started)
            else:
                plan = {
                    "generated_child_tables": [],
                    "table_classification": "analytics_ready",
                    "bi_suitability": "bi_ready",
                }
                mark_silver_phase(run_id, "sanitizing columns", target, bronze_table=f"{target.source_database}__{target.source_collection}", bronze_file=bronze_file_label, event_type=None, message=f"Applying Silver transform for {target.table_name}")
                timing_started = time.perf_counter()
                silver = TRANSFORMS[target.table_name](filtered).persist()
                add_timing(batch_timings, "flattening_seconds", timing_started)

            failed_step = "merge_delta"
            mark_silver_phase(run_id, "writing delta", target, bronze_table=f"{target.source_database}__{target.source_collection}", bronze_file=bronze_file_label, event_type=None, message=f"Writing Delta for {target.table_name}")
            timing_started = time.perf_counter()
            write_metrics = merge_delta(spark, silver, target.path, target.primary_key)
            rows_written = write_metrics.changed_rows
            if delta_exists(spark, target.path):
                register_delta_table(target.table_name, target.path, logger)
            add_timing(batch_timings, "delta_write_seconds", timing_started)

            keep_filtered_for_children = target.transform_strategy == "generic_flatten" and smart_silver_enabled()
            if not keep_filtered_for_children:
                safe_unpersist(filtered)
                filtered = None

            failed_step = "metadata_update"
            mark_silver_phase(run_id, "metadata update", target, bronze_table=f"{target.source_database}__{target.source_collection}", bronze_file=bronze_file_label, event_type=None, message=f"Updating Silver metadata for {target.table_name}")
            timing_started = time.perf_counter()
            schema_hash, flattened_count = detect_schema(silver, target.table_name)
            row_count = estimated_table_row_count(spark, target, write_metrics)
            replace_silver_quality_metrics(target.table_name, quality_metrics(silver, target, total_rows=row_count))
            upsert_silver_collection_state(
                silver_table_name=target.table_name,
                source_database=target.source_database,
                source_collection=target.source_collection,
                silver_table_path=target.path,
                trino_table_name=target.table_name,
                primary_key_column=target.primary_key,
                row_count=row_count,
                last_rows_written=rows_written,
                last_schema_hash=schema_hash,
                flattened_field_count=flattened_count,
                partition_info="silver_ingestion_date",
                last_processed_bronze_batch_id=batch_id(unprocessed[-1]),
                is_child_table=False,
                parent_silver_table_name=None,
                child_path=None,
                table_classification=plan.get("table_classification") or "analytics_ready",
                bi_suitability=plan.get("bi_suitability") or bi_suitability_for_classification(plan.get("table_classification") or "analytics_ready"),
                governance_status="profiled" if smart_silver_enabled() and target.transform_strategy == "generic_flatten" else "tracked",
                lineage_reference=f"bronze.{target.source_database}__{target.source_collection}->silver.{target.table_name}",
                transform_strategy="smart_flatten_v2" if smart_silver_enabled() and target.transform_strategy == "generic_flatten" else target.transform_strategy,
            )
            if batch_number == len(batch_groups):
                record_silver_run_event(
                    run_id,
                    "generating_safe_views",
                    f"{target.table_name} is registered for downstream safe-view generation.",
                    database_name=target.source_database,
                    collection_name=target.source_collection,
                    bronze_table=f"{target.source_database}__{target.source_collection}",
                    bronze_file=bronze_file_label,
                    batch_id=processing_batch_id,
                )
            add_timing(batch_timings, "metadata_update_seconds", timing_started)

            child_totals = {"rows": 0, "processed_tables": 0}
            safe_unpersist(silver)
            silver = None
            if target.transform_strategy == "generic_flatten" and smart_silver_enabled():
                failed_step = "generating_child_tables"
                mark_silver_phase(run_id, "generating child tables", target, bronze_table=f"{target.source_database}__{target.source_collection}", bronze_file=bronze_file_label, event_type=None, message=f"Generating child tables for {target.table_name}")
                timing_started = time.perf_counter()
                child_totals = process_smart_child_tables(
                    spark,
                    filtered,
                    target,
                    plan,
                    batch_id(unprocessed[-1]),
                    logger,
                    child_table_budget,
                )
                rows_written += child_totals["rows"]
                add_timing(batch_timings, "child_table_generation_seconds", timing_started)

            for state_id, _ in states:
                finish_silver_batch_state(state_id, "success", rows_written)
            successful_batch_ids.update(batch_id(batch) for batch in unprocessed)
            safe_unpersist(silver)
            safe_unpersist(filtered)
            batch_timings["total_timing_seconds"] = elapsed_since(batch_started_at)
            for key, value in batch_timings.items():
                result["timings"][key] += value
            finish_silver_processing_batch(processing_batch_id, "success", rows_written, retryable=False)
            result["processed"] += len(unprocessed)
            result["rows"] += rows_written
            result["processed_rows"] += rows_written
            result["completed_batches"] += 1
            result["processed_tables"] += (1 if len(unprocessed) else 0) + child_totals["processed_tables"]
            update_silver_run_progress(
                run_id,
                current_phase="completed batch",
                total_batches=run_total_batches,
                completed_batches=result["completed_batches"],
                total_batches_processed=result["processed"],
                total_batches_skipped=result["skipped"],
                total_rows_written=result["processed_rows"],
                processed_rows=result["processed_rows"],
                bronze_read_seconds=result["timings"]["bronze_read_seconds"],
                flattening_seconds=result["timings"]["flattening_seconds"],
                child_table_generation_seconds=result["timings"]["child_table_generation_seconds"],
                delta_write_seconds=result["timings"]["delta_write_seconds"],
                metadata_update_seconds=result["timings"]["metadata_update_seconds"],
                profiling_seconds=result["timings"]["profiling_seconds"],
                total_timing_seconds=result["timings"]["total_timing_seconds"],
            )
            logger.info("Processed Silver batch %s/%s for %s rows=%s", batch_number, len(batch_groups), target.table_name, rows_written)
        except Exception as exc:
            safe_unpersist(silver)
            safe_unpersist(filtered)
            failure = classify_silver_failure(exc, failed_step, target)
            for state_id, _ in states:
                finish_silver_batch_state(
                    state_id,
                    "failed",
                    0,
                    failure["message"],
                    error_type=failure["error_type"],
                    failed_step=failure["failed_step"],
                    recommended_fix=failure["recommended_fix"],
                    full_stack_trace=failure["full_stack_trace"],
                    possibly_corrupt=bool(failure["possibly_corrupt"]),
                )
            finish_silver_processing_batch(
                processing_batch_id,
                "failed",
                0,
                failed_step=failure["failed_step"],
                error_type=failure["error_type"],
                error_message=failure["message"],
                retryable=True,
                recommended_fix=failure["recommended_fix"],
            )
            record_silver_run_event(
                run_id,
                "failed",
                failure["message"],
                database_name=target.source_database,
                collection_name=target.source_collection,
                bronze_table=f"{target.source_database}__{target.source_collection}",
                bronze_file=bronze_file_label,
                batch_id=processing_batch_id,
            )
            result["failed"] += len(unprocessed)
            result["failed_tables"] = 1
            result["completed_batches"] += 1
            if failure["spark_crashed"]:
                result["spark_crashed"] = 1
            update_silver_run_progress(
                run_id,
                current_phase="failed",
                failed_batches=result["failed"],
                failed_tables=result["failed_tables"],
                completed_batches=result["completed_batches"],
                total_batches=run_total_batches,
                error_message=failure["message"],
            )
            logger.error("Failed Silver target %s step=%s type=%s message=%s", target.table_name, failure["failed_step"], failure["error_type"], failure["message"])
            break

    return result


def main() -> None:
    logger = setup_logging("silver_bronze_to_delta")
    airflow_run_id = os.environ.get("AIRFLOW_SILVER_RUN_ID") or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or f"local__{uuid.uuid4().hex}"
    triggered_by = os.environ.get("ONOV8_TRIGGERED_BY", "airflow")
    scope = silver_scope()
    database_name = silver_scope_database()
    collection_name = silver_scope_collection()
    table_name_ref = silver_scope_table_name()
    if table_name_ref and scope == "all_pending":
        scope = "table"
    bronze_table_ref = silver_scope_bronze_table()
    bronze_file_ref = silver_scope_bronze_file()
    retry_failed = silver_retry_failed()
    run_id = ensure_silver_run(
        airflow_run_id,
        triggered_by=triggered_by,
        scope=scope,
        database_name=database_name,
        collection_name=collection_name,
        bronze_table=bronze_table_ref,
        bronze_file=bronze_file_ref,
        retry_failed=retry_failed,
        silver_batch_size=silver_batch_size(),
        silver_parallel_collections=silver_parallel_collections(),
        silver_parallel_tables=silver_parallel_tables(),
    )
    totals = {
        "found": 0,
        "processed": 0,
        "skipped": 0,
        "rows": 0,
        "failed": 0,
        "unmapped": 0,
        "processed_tables": 0,
        "failed_tables": 0,
        "completed_batches": 0,
        "bronze_read_seconds": 0.0,
        "flattening_seconds": 0.0,
        "child_table_generation_seconds": 0.0,
        "delta_write_seconds": 0.0,
        "metadata_update_seconds": 0.0,
        "profiling_seconds": 0.0,
        "total_timing_seconds": 0.0,
    }
    failed_table_names: set[str] = set()
    spark: SparkSession | None = None

    try:
        table_message = f" table={table_name_ref}" if table_name_ref else ""
        mark_silver_phase(run_id, "initializing", event_type="started", message=f"Silver run started scope={scope}{table_message}")
        spark = build_spark_session()
        bronze_tables = discover_bronze_tables(
            logger,
            scope=scope,
            database_name=database_name,
            collection_name=collection_name,
            bronze_table=bronze_table_ref,
            bronze_file=bronze_file_ref,
            retry_failed=retry_failed,
        )
        batches = fetch_successful_bronze_batches(
            scope=scope,
            database_name=database_name,
            collection_name=collection_name,
            bronze_table=bronze_table_ref,
            bronze_file=bronze_file_ref,
            retry_failed=retry_failed,
        )
        only_targets = selected_target_names()
        planned_targets: list[tuple[BronzeTable, SilverTarget, list[dict[str, Any]]]] = []
        for bronze_table_item in bronze_tables:
            targets = targets_for_bronze_table(bronze_table_item)
            if only_targets:
                targets = [target for target in targets if target.table_name in only_targets]
                if not targets:
                    continue
            if not targets:
                totals["unmapped"] += 1
                logger.warning(
                    "Bronze table %s.%s is unmapped and generic mapping is disabled",
                    bronze_table_item.database_name,
                    bronze_table_item.collection_name,
                )
                continue
            for target in targets:
                candidate_batches = merged_candidate_batches(target, batches)
                planned_targets.append((bronze_table_item, target, candidate_batches))
                totals["found"] += len(candidate_batches)

        run_total_batches = sum(len(chunked(candidate_batches, silver_batch_size())) for _, _, candidate_batches in planned_targets)
        total_collections = len({(item.database_name, item.collection_name) for item, _, _ in planned_targets})
        total_databases = len({item.database_name for item, _, _ in planned_targets})
        for bronze_table_item, target, candidate_batches in planned_targets:
            batch_groups = chunked(candidate_batches, silver_batch_size())
            for batch_number, batch_group in enumerate(batch_groups, start=1):
                queue_silver_processing_batch(
                    run_id=run_id,
                    database_name=target.source_database,
                    collection_name=target.source_collection,
                    bronze_table=bronze_table_item.table_ref,
                    bronze_file=batch_file_label(batch_group),
                    batch_number=batch_number,
                    total_batches=max(1, len(batch_groups)),
                )
        if run_total_batches:
            record_silver_run_event(run_id, "queued", f"Queued {run_total_batches} Silver batch(es) for processing.")
        update_silver_run_progress(
            run_id,
            status="running",
            current_phase="queued" if not planned_targets else "initializing",
            total_databases=total_databases,
            total_collections=total_collections,
            total_bronze_tables=len({item.table_ref for item, _, _ in planned_targets}),
            total_batches=run_total_batches,
            total_bronze_batches_found=totals["found"],
            silver_batch_size=silver_batch_size(),
        )
        if not planned_targets:
            record_silver_run_event(run_id, "completed", "No Bronze-backed Silver tables matched the requested scope.")

        child_table_budget = {"remaining": silver_max_child_tables_per_run()} if smart_silver_enabled() else None
        completed_table_refs: set[str] = set()
        completed_collection_refs: set[tuple[str, str]] = set()
        completed_database_refs: set[str] = set()
        planned_by_bronze: dict[str, list[tuple[BronzeTable, SilverTarget, list[dict[str, Any]]]]] = {}
        for item in planned_targets:
            planned_by_bronze.setdefault(item[0].table_ref, []).append(item)

        stop_requested = False
        for table_targets in planned_by_bronze.values():
            bronze_table_item = table_targets[0][0]
            has_pending_work = any(
                target_has_unprocessed_batches(target, candidate_batches)
                for _, target, candidate_batches in table_targets
            )
            shared_bronze = spark.read.format("delta").load(bronze_table_item.bronze_table_path).persist() if has_pending_work else None
            try:
                for bronze_table_item, target, _candidate_batches in table_targets:
                    result = process_target(
                        spark,
                        target,
                        bronze_table_item.bronze_table_path,
                        batches,
                        logger,
                        run_id=run_id,
                        run_total_batches=run_total_batches,
                        completed_batches=int(totals["completed_batches"]),
                        processed_rows=int(totals["rows"]),
                        bronze_frame=shared_bronze,
                        child_table_budget=child_table_budget,
                    )
                    for key, value in result.items():
                        if key == "timings":
                            for timing_key, timing_value in value.items():
                                if timing_key in totals:
                                    totals[timing_key] += timing_value
                            continue
                        if key in {"completed_batches", "processed_rows"}:
                            continue
                        if key == "found":
                            continue
                        if key in totals:
                            totals[key] += value
                    totals["completed_batches"] = result.get("completed_batches", totals["completed_batches"])
                    if result.get("failed_tables"):
                        failed_table_names.add(target.table_name)
                    else:
                        completed_table_refs.add(bronze_table_item.table_ref)
                        completed_collection_refs.add((bronze_table_item.database_name, bronze_table_item.collection_name))
                        completed_database_refs.add(bronze_table_item.database_name)
                    update_silver_run_progress(
                        run_id,
                        completed_databases=len(completed_database_refs),
                        completed_collections=len(completed_collection_refs),
                        completed_bronze_tables=len(completed_table_refs),
                        completed_batches=int(totals["completed_batches"]),
                        total_batches_processed=int(totals["processed"]),
                        total_batches_skipped=int(totals["skipped"]),
                        total_rows_written=int(totals["rows"]),
                        processed_rows=int(totals["rows"]),
                        failed_batches=int(totals["failed"]),
                        processed_tables=int(totals["processed_tables"]),
                        failed_tables=int(totals["failed_tables"]),
                        bronze_read_seconds=totals["bronze_read_seconds"],
                        flattening_seconds=totals["flattening_seconds"],
                        child_table_generation_seconds=totals["child_table_generation_seconds"],
                        delta_write_seconds=totals["delta_write_seconds"],
                        metadata_update_seconds=totals["metadata_update_seconds"],
                        profiling_seconds=totals["profiling_seconds"],
                        total_timing_seconds=totals["total_timing_seconds"],
                    )
                    if result.get("cancelled"):
                        stop_requested = True
                        break
                    if result.get("spark_crashed"):
                        logger.warning("Spark JVM crash detected while processing %s; restarting Spark session before continuing", target.table_name)
                        try:
                            spark.stop()
                        except Exception:
                            pass
                        spark = build_spark_session()
                        break
            finally:
                safe_unpersist(shared_bronze)
            if stop_requested:
                break

        if silver_run_cancel_requested(run_id):
            status = "cancelled"
            error_message = "Silver run cancellation requested"
        elif totals["failed"] > 0:
            status = "failed"
            error_message = f"{totals['failed_tables']} Silver table(s) failed: {', '.join(sorted(failed_table_names))}"
        elif totals["processed"] > 0:
            status = "success"
            error_message = None
        elif totals["unmapped"] > 0:
            status = "warning"
            error_message = f"{totals['unmapped']} Bronze table(s) have no active Silver mapping"
        else:
            status = "no_new_data"
            error_message = None

        finish_silver_run(
            run_id,
            status,
            totals["found"],
            totals["processed"],
            totals["skipped"],
            totals["rows"],
            totals["failed"],
            error_message,
            processed_tables=totals["processed_tables"],
            failed_tables=totals["failed_tables"],
            failed_table_names=sorted(failed_table_names),
        )
        record_silver_run_event(
            run_id,
            "completed" if status in {"success", "no_new_data", "warning"} else status,
            f"Silver run {status}: processed={int(totals['processed'])} skipped={int(totals['skipped'])} failed={int(totals['failed'])}",
        )
        summary = {
            "status": status,
            "bronze_tables_detected": len(bronze_tables),
            "total_bronze_batches_found": totals["found"],
            "batches_processed": totals["processed"],
            "batches_skipped": totals["skipped"],
            "rows_written": totals["rows"],
            "failures": totals["failed"],
            "processed_tables": totals["processed_tables"],
            "failed_tables": totals["failed_tables"],
            "failed_table_names": sorted(failed_table_names),
            "unmapped_bronze_tables": totals["unmapped"],
        }
        logger.info("Silver processing summary: %s", json.dumps(summary, sort_keys=True))
        if status == "failed":
            raise RuntimeError(error_message)
    except Exception as exc:
        logger.error("Silver processing run failed: %s\n%s", exc, traceback.format_exc())
        record_silver_run_event(run_id, "failed", str(exc))
        finish_silver_run(
            run_id,
            "failed",
            totals["found"],
            totals["processed"],
            totals["skipped"],
            totals["rows"],
            max(totals["failed"], 1),
            str(exc),
            processed_tables=totals["processed_tables"],
            failed_tables=max(totals["failed_tables"], 1),
            failed_table_names=sorted(failed_table_names),
        )
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
