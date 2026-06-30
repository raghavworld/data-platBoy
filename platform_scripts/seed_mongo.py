from __future__ import annotations

from datetime import timedelta

from common import mongo_client, mongo_sources, setup_logging, utc_now


def user_documents(now):
    return [
        {
            "_id": "USR-1001",
            "name": "Amina Rahman",
            "email": "amina.rahman@example.com",
            "phone": "+971500000001",
            "country": "AE",
            "preferences": {"theme": "light", "notifications": {"email": True, "sms": False}},
            "createdAt": now - timedelta(days=9),
            "updatedAt": now - timedelta(days=2),
        },
        {
            "_id": "USR-1002",
            "name": "Leo Carter",
            "email": "leo.carter@example.com",
            "phone": "+12025550123",
            "country": "US",
            "preferences": {"theme": "dark", "segments": ["vip", "repeat"]},
            "createdAt": now - timedelta(days=7),
            "updatedAt": now - timedelta(days=1, hours=4),
        },
        {
            "_id": "USR-1003",
            "name": "Nora Haddad",
            "email": "nora.haddad@example.com",
            "phone": "+966500000777",
            "country": "SA",
            "preferences": {"preferredCategories": ["home", "outdoor"], "newsletter": True},
            "createdAt": now - timedelta(days=5),
            "updatedAt": now - timedelta(hours=10),
        },
    ]


def product_documents(now):
    return [
        {
            "_id": "PRD-2001",
            "sku": "SKU-CHAIR-001",
            "name": "Ergo Chair",
            "category": "furniture",
            "price": 249.90,
            "attributes": {"color": "sand", "material": "mesh", "dimensions": {"w": 64, "h": 112}},
            "createdAt": now - timedelta(days=12),
            "updatedAt": now - timedelta(days=1, hours=3),
        },
        {
            "_id": "PRD-2002",
            "sku": "SKU-LAMP-002",
            "name": "Halo Desk Lamp",
            "category": "lighting",
            "price": 89.50,
            "attributes": {"lumens": 850, "smart": True, "compatibility": ["alexa", "google-home"]},
            "createdAt": now - timedelta(days=8),
            "updatedAt": now - timedelta(hours=18),
        },
        {
            "_id": "PRD-2003",
            "sku": "SKU-MUG-003",
            "name": "Thermal Mug",
            "category": "kitchen",
            "price": 34.25,
            "attributes": {"capacityMl": 420, "insulated": True, "tags": ["travel", "office"]},
            "createdAt": now - timedelta(days=6),
            "updatedAt": now - timedelta(hours=7),
        },
    ]


def order_documents(now):
    return [
        {
            "_id": "ORD-3001",
            "customerId": "USR-1001",
            "status": "completed",
            "totalAmount": 339.40,
            "currency": "AED",
            "items": [{"sku": "SKU-LAMP-002", "qty": 1}, {"sku": "SKU-MUG-003", "qty": 2}],
            "customerEmail": "amina.rahman@example.com",
            "customerPhone": "+971500000001",
            "extraFields": {"channel": "mobile", "giftWrap": False},
            "createdAt": now - timedelta(days=3),
            "updatedAt": now - timedelta(days=1),
        },
        {
            "_id": "ORD-3002",
            "customerId": "USR-1002",
            "status": "processing",
            "totalAmount": 249.90,
            "currency": "USD",
            "items": [{"sku": "SKU-CHAIR-001", "qty": 1}],
            "customerEmail": "leo.carter@example.com",
            "customerPhone": "+12025550123",
            "extraFields": {"channel": "web", "checkoutExperiment": "B"},
            "createdAt": now - timedelta(days=2),
            "updatedAt": now - timedelta(hours=12),
        },
        {
            "_id": "ORD-3003",
            "customerId": "USR-1003",
            "status": "pending",
            "totalAmount": 123.75,
            "currency": "SAR",
            "items": [{"sku": "SKU-MUG-003", "qty": 3}, {"sku": "SKU-LAMP-002", "qty": 1}],
            "customerEmail": "nora.haddad@example.com",
            "customerPhone": "+966500000777",
            "extraFields": {"channel": "marketplace", "priority": "expedited"},
            "createdAt": now - timedelta(days=1, hours=6),
            "updatedAt": now - timedelta(hours=5),
        },
    ]


def payment_documents(now):
    return [
        {
            "_id": "PAY-4001",
            "orderId": "ORD-3001",
            "method": "card",
            "amount": 339.40,
            "status": "captured",
            "providerRef": "stripe_pi_001",
            "gatewayResponse": {"avs": "Y", "riskScore": 12, "processor": "stripe"},
            "createdAt": now - timedelta(days=3),
            "updatedAt": now - timedelta(days=1),
        },
        {
            "_id": "PAY-4002",
            "orderId": "ORD-3002",
            "method": "paypal",
            "amount": 249.90,
            "status": "authorized",
            "providerRef": "paypal_002",
            "gatewayResponse": {"payerCountry": "US", "fraudCheck": "manual_review"},
            "createdAt": now - timedelta(days=2),
            "updatedAt": now - timedelta(hours=12),
        },
        {
            "_id": "PAY-4003",
            "orderId": "ORD-3003",
            "method": "apple_pay",
            "amount": 123.75,
            "status": "pending",
            "providerRef": "apple_003",
            "gatewayResponse": {"deviceAccount": "active", "network": "mada"},
            "createdAt": now - timedelta(days=1, hours=6),
            "updatedAt": now - timedelta(hours=5),
        },
    ]


def seed_collection(client, database_name, collection_name, docs, logger):
    collection = client[database_name][collection_name]
    collection.create_index("updatedAt")
    for document in docs:
        collection.replace_one({"_id": document["_id"]}, document, upsert=True)
    logger.info("Seeded %s.%s with %s documents", database_name, collection_name, len(docs))


def main() -> None:
    logger = setup_logging("seed_mongo")
    now = utc_now()
    sources = {source.collection_name: source for source in mongo_sources()}

    with mongo_client(sources["users"].service_name) as users_client:
        seed_collection(users_client, sources["users"].database_name, "users", user_documents(now), logger)
    with mongo_client(sources["products"].service_name) as products_client:
        seed_collection(products_client, sources["products"].database_name, "products", product_documents(now), logger)
    with mongo_client(sources["orders"].service_name) as orders_client:
        seed_collection(orders_client, sources["orders"].database_name, "orders", order_documents(now), logger)
    with mongo_client(sources["payments"].service_name) as payments_client:
        seed_collection(payments_client, sources["payments"].database_name, "payments", payment_documents(now), logger)


if __name__ == "__main__":
    main()
