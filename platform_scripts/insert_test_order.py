from __future__ import annotations

from datetime import timedelta

from common import (
    bronze_table_name,
    mongo_client,
    mongo_sources,
    run_subprocess,
    setup_logging,
    spark_submit_command,
    sql_string_literal,
    trino_connection,
    utc_now,
)
from ingest_mongo_to_raw import main as ingest_main


def insert_test_order(logger, order_id: str) -> None:
    now = utc_now()
    sources = {source.collection_name: source for source in mongo_sources()}

    with mongo_client(sources["orders"].service_name) as client:
        collection = client[sources["orders"].database_name]["orders"]
        collection.replace_one(
            {"_id": order_id},
            {
                "_id": order_id,
                "customerId": "USR-1001",
                "status": "completed",
                "totalAmount": 515.80,
                "currency": "AED",
                "items": [{"sku": "SKU-CHAIR-001", "qty": 1}, {"sku": "SKU-MUG-003", "qty": 1}],
                "customerEmail": "amina.rahman@example.com",
                "customerPhone": "+971500000001",
                "extraFields": {"channel": "api", "priority": "vip"},
                "promoCode": "SPRING-LOCAL",
                "createdAt": now - timedelta(minutes=5),
                "updatedAt": now,
            },
            upsert=True,
        )
    logger.info("Inserted test order %s", order_id)

    with mongo_client(sources["users"].service_name) as client:
        client[sources["users"].database_name]["users"].update_one(
            {"_id": "USR-1001"},
            {"$set": {"marketingConsent": True, "updatedAt": now}},
        )

    with mongo_client(sources["products"].service_name) as client:
        client[sources["products"].database_name]["products"].update_one(
            {"_id": "PRD-2001"},
            {"$set": {"supplierMetadata": {"supplierId": "SUP-01", "contractTier": "gold"}, "updatedAt": now}},
        )
    logger.info("Applied schema evolution updates for users and products")


def verify_test_order(logger, order_id: str) -> None:
    table_name = bronze_table_name("orders_service", "orders")
    connection = trino_connection()
    cursor = connection.cursor()
    cursor.execute(
        f"""
        SELECT _id, _record_hash
        FROM delta.bronze."{table_name}"
        WHERE _id = {sql_string_literal(order_id)}
        """
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Order {order_id} not found in Bronze table {table_name}")
    if not row[1]:
        raise RuntimeError("Expected Bronze _record_hash")
    logger.info("Validated test order %s in Bronze table %s with record hash %s", row[0], table_name, row[1])
    cursor.close()
    connection.close()


def main() -> None:
    logger = setup_logging("insert_test_order")
    order_id = f"ORD-TEST-{utc_now():%Y%m%d%H%M%S}"
    insert_test_order(logger, order_id)
    ingest_main()
    run_subprocess(spark_submit_command(), logger)
    verify_test_order(logger, order_id)


if __name__ == "__main__":
    main()
