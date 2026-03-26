use nextsql_e2e_tests::generated::runtime::PgClient;
use nextsql_e2e_tests::generated::*;

async fn setup() -> PgClient {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5439 user=nextsql password=password dbname=nextsql_test",
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Each test runs in a transaction that gets rolled back on drop
    client
        .batch_execute("BEGIN")
        .await
        .expect("Failed to begin transaction");

    client
        .batch_execute(
            "
        CREATE TEMP TABLE customers (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email TEXT NOT NULL,
            name TEXT NOT NULL,
            phone TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            is_active BOOLEAN NOT NULL DEFAULT true
        );
        CREATE TEMP TABLE categories (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            parent_id UUID,
            sort_order INT NOT NULL DEFAULT 0,
            is_active BOOLEAN NOT NULL DEFAULT true
        );
        CREATE TEMP TABLE products (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            category_id UUID NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            price DOUBLE PRECISION NOT NULL,
            stock INT NOT NULL DEFAULT 0,
            is_active BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        );
        CREATE TEMP TABLE orders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            customer_id UUID NOT NULL,
            status TEXT NOT NULL,
            total_amount DOUBLE PRECISION NOT NULL,
            ordered_at TIMESTAMP NOT NULL DEFAULT now(),
            shipped_at TIMESTAMP,
            notes TEXT
        );
        CREATE TEMP TABLE order_items (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            order_id UUID NOT NULL,
            product_id UUID NOT NULL,
            quantity INT NOT NULL,
            unit_price DOUBLE PRECISION NOT NULL,
            UNIQUE(order_id, product_id)
        );
        CREATE TEMP TABLE reviews (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            product_id UUID NOT NULL,
            customer_id UUID NOT NULL,
            rating INT NOT NULL,
            comment TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            UNIQUE(product_id, customer_id)
        );
        CREATE TEMP TABLE addresses (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            customer_id UUID NOT NULL,
            label TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            city TEXT NOT NULL,
            street TEXT NOT NULL,
            is_default BOOLEAN NOT NULL DEFAULT false
        );
    ",
        )
        .await
        .expect("Failed to create temp tables");

    PgClient::new(client)
}

#[tokio::test]
async fn test_insert_and_select_customer() {
    let client = setup().await;

    let created = customers::create_customer(
        &client,
        &customers::CreateCustomerParams {
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            phone: Some("090-1234-5678".to_string()),
        },
    )
    .await
    .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].name, "Alice");
    assert_eq!(created[0].email, "alice@example.com");
    assert_eq!(created[0].phone, Some("090-1234-5678".to_string()));
    assert!(created[0].is_active);

    let found = customers::find_customer_by_id(
        &client,
        &customers::FindCustomerByIdParams {
            id: types::CustomerId(created[0].id.0),
        },
    )
    .await
    .unwrap();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].name, "Alice");
}

#[tokio::test]
async fn test_update_customer() {
    let client = setup().await;

    let created = customers::create_customer(
        &client,
        &customers::CreateCustomerParams {
            name: "Bob".to_string(),
            email: "bob@example.com".to_string(),
            phone: None,
        },
    )
    .await
    .unwrap();

    let updated = customers::update_customer(
        &client,
        &customers::UpdateCustomerParams {
            id: types::CustomerId(created[0].id.0),
            name: "Bobby".to_string(),
            email: "bobby@example.com".to_string(),
            phone: Some("080-9999-0000".to_string()),
        },
    )
    .await
    .unwrap();

    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].name, "Bobby");
    assert_eq!(updated[0].email, "bobby@example.com");
    assert_eq!(updated[0].phone, Some("080-9999-0000".to_string()));
}

#[tokio::test]
async fn test_soft_delete_customer() {
    let client = setup().await;

    let created = customers::create_customer(
        &client,
        &customers::CreateCustomerParams {
            name: "Charlie".to_string(),
            email: "charlie@example.com".to_string(),
            phone: None,
        },
    )
    .await
    .unwrap();

    let count = customers::deactivate_customer(
        &client,
        &customers::DeactivateCustomerParams {
            id: types::CustomerId(created[0].id.0),
        },
    )
    .await
    .unwrap();
    assert_eq!(count, 1);

    let found = customers::find_customer_by_id(
        &client,
        &customers::FindCustomerByIdParams {
            id: types::CustomerId(created[0].id.0),
        },
    )
    .await
    .unwrap();
    assert_eq!(found.len(), 1);
    assert!(!found[0].is_active);

    let active = customers::list_customers(
        &client,
        &customers::ListCustomersParams {
            limit: 100,
            offset: 0,
        },
    )
    .await
    .unwrap();
    assert!(active.iter().all(|c| c.id.0 != created[0].id.0));
}

#[tokio::test]
async fn test_order_lifecycle() {
    let client = setup().await;

    let customer = customers::create_customer(
        &client,
        &customers::CreateCustomerParams {
            name: "Dave".to_string(),
            email: "dave@example.com".to_string(),
            phone: None,
        },
    )
    .await
    .unwrap();

    let inner = client.inner();
    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('Electronics', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let prod_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Laptop', 999.99, 10) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);

    let order = orders::create_order(
        &client,
        &orders::CreateOrderParams {
            customer_id: types::CustomerId(customer[0].id.0),
            total_amount: 999.99,
            notes: Some("Please gift wrap".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(order.len(), 1);
    assert_eq!(order[0].status, "pending");
    assert_eq!(order[0].notes, Some("Please gift wrap".to_string()));

    let item = orders::add_order_item(
        &client,
        &orders::AddOrderItemParams {
            order_id: types::OrderId(order[0].id.0),
            product_id: types::ProductId(prod_id),
            quantity: 1,
            unit_price: 999.99,
        },
    )
    .await
    .unwrap();
    assert_eq!(item.len(), 1);
    assert_eq!(item[0].quantity, 1);

    let shipped = orders::ship_order(
        &client,
        &orders::ShipOrderParams {
            id: types::OrderId(order[0].id.0),
        },
    )
    .await
    .unwrap();
    assert_eq!(shipped.len(), 1);
    assert_eq!(shipped[0].status, "shipped");
    assert!(shipped[0].shipped_at.is_some());
}

#[tokio::test]
async fn test_upsert_order_item() {
    let client = setup().await;
    let inner = client.inner();

    let cust_row = inner.query_one("INSERT INTO customers (name, email) VALUES ('Eve', 'eve@example.com') RETURNING id", &[]).await.unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);
    let cat_row = inner.query_one("INSERT INTO categories (name, sort_order) VALUES ('Books', 1) RETURNING id", &[]).await.unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);
    let prod_row = inner.query_one("INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Novel', 19.99, 50) RETURNING id", &[&cat_id]).await.unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);
    let ord_row = inner.query_one("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 19.99) RETURNING id", &[&cust_id]).await.unwrap();
    let ord_id: uuid::Uuid = ord_row.get(0);

    let item1 = orders::upsert_order_item(
        &client,
        &orders::UpsertOrderItemParams {
            order_id: types::OrderId(ord_id),
            product_id: types::ProductId(prod_id),
            quantity: 1,
            unit_price: 19.99,
        },
    )
    .await
    .unwrap();
    assert_eq!(item1[0].quantity, 1);

    let item2 = orders::upsert_order_item(
        &client,
        &orders::UpsertOrderItemParams {
            order_id: types::OrderId(ord_id),
            product_id: types::ProductId(prod_id),
            quantity: 3,
            unit_price: 19.99,
        },
    )
    .await
    .unwrap();
    assert_eq!(item2[0].quantity, 3);

    let count: i64 = inner.query_one("SELECT COUNT(*) FROM order_items WHERE order_id = $1", &[&ord_id]).await.unwrap().get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_delete_review_with_returning() {
    let client = setup().await;
    let inner = client.inner();

    let cust_row = inner.query_one("INSERT INTO customers (name, email) VALUES ('Frank', 'frank@example.com') RETURNING id", &[]).await.unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);
    let cat_row = inner.query_one("INSERT INTO categories (name, sort_order) VALUES ('Food', 1) RETURNING id", &[]).await.unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);
    let prod_row = inner.query_one("INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Chocolate', 5.99, 100) RETURNING id", &[&cat_id]).await.unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);
    let review_row = inner.query_one("INSERT INTO reviews (product_id, customer_id, rating, comment) VALUES ($1, $2, 5, 'Delicious!') RETURNING id", &[&prod_id, &cust_id]).await.unwrap();
    let review_id: uuid::Uuid = review_row.get(0);

    let deleted = reviews::delete_review(
        &client,
        &reviews::DeleteReviewParams {
            id: types::ReviewId(review_id),
        },
    )
    .await
    .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].rating, 5);
    assert_eq!(deleted[0].comment, Some("Delicious!".to_string()));

    let count: i64 = inner.query_one("SELECT COUNT(*) FROM reviews WHERE id = $1", &[&review_id]).await.unwrap().get(0);
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_where_conditions_and_pagination() {
    let client = setup().await;

    for (name, email) in [("Active1", "a1@test.com"), ("Active2", "a2@test.com"), ("Inactive", "inactive@test.com")] {
        customers::create_customer(
            &client,
            &customers::CreateCustomerParams {
                name: name.to_string(),
                email: email.to_string(),
                phone: None,
            },
        )
        .await
        .unwrap();
    }

    let all = customers::list_customers(&client, &customers::ListCustomersParams { limit: 100, offset: 0 }).await.unwrap();
    let inactive = all.iter().find(|c| c.name == "Inactive").unwrap();
    customers::deactivate_customer(&client, &customers::DeactivateCustomerParams { id: types::CustomerId(inactive.id.0) }).await.unwrap();

    let active = customers::list_customers(&client, &customers::ListCustomersParams { limit: 100, offset: 0 }).await.unwrap();
    assert_eq!(active.len(), 2);
    assert!(active.iter().all(|c| c.is_active));

    let page1 = customers::list_customers(&client, &customers::ListCustomersParams { limit: 1, offset: 0 }).await.unwrap();
    assert_eq!(page1.len(), 1);
    let page2 = customers::list_customers(&client, &customers::ListCustomersParams { limit: 1, offset: 1 }).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert_ne!(page1[0].id.0, page2[0].id.0);
}

#[tokio::test]
async fn test_find_order_with_customer_join() {
    let client = setup().await;

    let customer = customers::create_customer(
        &client,
        &customers::CreateCustomerParams {
            name: "Grace".to_string(),
            email: "grace@example.com".to_string(),
            phone: None,
        },
    )
    .await
    .unwrap();

    let order = orders::create_order(
        &client,
        &orders::CreateOrderParams {
            customer_id: types::CustomerId(customer[0].id.0),
            total_amount: 100.0,
            notes: None,
        },
    )
    .await
    .unwrap();

    let found = orders::find_order_by_id(
        &client,
        &orders::FindOrderByIdParams {
            id: types::OrderId(order[0].id.0),
        },
    )
    .await
    .unwrap();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].name, "Grace");
    assert_eq!(found[0].email, "grace@example.com");
    assert_eq!(found[0].total_amount, 100.0);
}

#[tokio::test]
async fn test_products_in_price_range() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner.query_one("INSERT INTO categories (name, sort_order) VALUES ('Gadgets', 1) RETURNING id", &[]).await.unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    for (name, price) in [("Cheap", 10.0_f64), ("Mid", 50.0_f64), ("Expensive", 200.0_f64)] {
        inner.execute("INSERT INTO products (category_id, name, price, stock) VALUES ($1, $2, $3, 10)", &[&cat_id, &name, &price]).await.unwrap();
    }

    let results = products::find_products_in_price_range(
        &client,
        &products::FindProductsInPriceRangeParams { min: 20.0, max: 100.0 },
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Mid");
    assert_eq!(results[0].price, 50.0);
}

#[tokio::test]
async fn test_union_query() {
    let client = setup().await;
    let inner = client.inner();

    let cust_row = inner.query_one("INSERT INTO customers (name, email) VALUES ('Hana', 'hana@example.com') RETURNING id", &[]).await.unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    inner.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 100.0)", &[&cust_id]).await.unwrap();
    inner.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'shipped', 200.0)", &[&cust_id]).await.unwrap();
    inner.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'cancelled', 50.0)", &[&cust_id]).await.unwrap();

    let results = orders::find_all_active_orders(&client).await.unwrap();
    assert_eq!(results.len(), 2);
    let amounts: Vec<f64> = results.iter().map(|r| r.total_amount).collect();
    assert!(amounts.contains(&100.0));
    assert!(amounts.contains(&200.0));
}

#[tokio::test]
async fn test_search_customers_dynamic_filter() {
    let client = setup().await;

    // Create several customers
    for (name, email) in [
        ("Alice Smith", "alice@example.com"),
        ("Bob Jones", "bob@example.com"),
        ("Alice Wonder", "wonder@example.com"),
    ] {
        customers::create_customer(
            &client,
            &customers::CreateCustomerParams {
                name: name.to_string(),
                email: email.to_string(),
                phone: None,
            },
        )
        .await
        .unwrap();
    }

    // Deactivate Bob
    let all = customers::list_customers(
        &client,
        &customers::ListCustomersParams {
            limit: 100,
            offset: 0,
        },
    )
    .await
    .unwrap();
    let bob = all.iter().find(|c| c.name == "Bob Jones").unwrap();
    customers::deactivate_customer(
        &client,
        &customers::DeactivateCustomerParams {
            id: types::CustomerId(bob.id.0),
        },
    )
    .await
    .unwrap();

    // Filter by is_active only -> should return Alice Smith and Alice Wonder
    let active = customers::search_customers(
        &client,
        &customers::SearchCustomersParams {
            name_like: None,
            email_like: None,
            is_active: Some(true),
        },
    )
    .await
    .unwrap();
    assert_eq!(active.len(), 2);
    assert!(active.iter().all(|c| c.is_active));

    // No filters -> should return all 3
    let all_result = customers::search_customers(
        &client,
        &customers::SearchCustomersParams {
            name_like: None,
            email_like: None,
            is_active: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(all_result.len(), 3);

    // Filter by name_like -> should return both Alices (upper() LIKE is used)
    let alices = customers::search_customers(
        &client,
        &customers::SearchCustomersParams {
            name_like: Some("%ALICE%".to_string()),
            email_like: None,
            is_active: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(alices.len(), 2);

    // Combine name_like + is_active -> "Bob" is inactive, filter for active only
    let active_bobs = customers::search_customers(
        &client,
        &customers::SearchCustomersParams {
            name_like: Some("%BOB%".to_string()),
            email_like: None,
            is_active: Some(true),
        },
    )
    .await
    .unwrap();
    assert_eq!(active_bobs.len(), 0); // Bob is deactivated
}

#[tokio::test]
async fn test_dynamic_product_search() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('SearchCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    for (name, price) in [("Laptop", 999.0_f64), ("Mouse", 25.0_f64), ("Keyboard", 75.0_f64), ("Monitor", 300.0_f64)] {
        inner
            .execute(
                "INSERT INTO products (category_id, name, price, stock) VALUES ($1, $2, $3, 10)",
                &[&cat_id, &name, &price],
            )
            .await
            .unwrap();
    }

    // Filter by price range only
    let mid_range = products::search_products(
        &client,
        &products::SearchProductsParams {
            min_price: Some(50.0),
            max_price: Some(500.0),
            category_ids: None,
            name_like: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(mid_range.len(), 2); // Keyboard (75) and Monitor (300)

    // Filter by name only
    let laptops = products::search_products(
        &client,
        &products::SearchProductsParams {
            min_price: None,
            max_price: None,
            category_ids: None,
            name_like: Some("%Lap%".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(laptops.len(), 1);
    assert_eq!(laptops[0].name, "Laptop");

    // No filters -> all active products
    let all = products::search_products(
        &client,
        &products::SearchProductsParams {
            min_price: None,
            max_price: None,
            category_ids: None,
            name_like: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(all.len(), 4);
}

#[tokio::test]
async fn test_find_products_by_ids() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('TestCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let mut prod_ids = Vec::new();
    for name in ["Product A", "Product B", "Product C"] {
        let row = inner
            .query_one(
                "INSERT INTO products (category_id, name, price, stock) VALUES ($1, $2, 10.0, 5) RETURNING id",
                &[&cat_id, &name],
            )
            .await
            .unwrap();
        prod_ids.push(row.get::<_, uuid::Uuid>(0));
    }

    let results = products::find_products_by_ids(
        &client,
        &products::FindProductsByIdsParams {
            ids: vec![
                types::ProductId(prod_ids[0]),
                types::ProductId(prod_ids[1]),
            ],
        },
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 2);
    let result_ids: Vec<uuid::Uuid> = results.iter().map(|p| p.id.0).collect();
    assert!(result_ids.contains(&prod_ids[0]));
    assert!(result_ids.contains(&prod_ids[1]));
    assert!(!result_ids.contains(&prod_ids[2]));
}

#[tokio::test]
async fn test_null_filters() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('NullTest', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    // Product with description
    inner
        .execute(
            "INSERT INTO products (category_id, name, description, price, stock) VALUES ($1, 'WithDesc', 'A great product', 10.0, 5)",
            &[&cat_id],
        )
        .await
        .unwrap();

    // Product without description (NULL)
    inner
        .execute(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'NoDesc', 20.0, 3)",
            &[&cat_id],
        )
        .await
        .unwrap();

    // find_products_with_null_description: should return only 'NoDesc'
    let null_desc = products::find_products_with_null_description(&client).await.unwrap();
    assert_eq!(null_desc.len(), 1);
    assert_eq!(null_desc[0].name, "NoDesc");

    // find_products_with_description: should return only 'WithDesc'
    let with_desc = products::find_products_with_description(&client).await.unwrap();
    assert_eq!(with_desc.len(), 1);
    assert_eq!(with_desc[0].name, "WithDesc");
    assert_eq!(
        with_desc[0].description,
        Some("A great product".to_string())
    );
}

#[tokio::test]
async fn test_exists_subquery() {
    let client = setup().await;
    let inner = client.inner();

    // Create two customers
    let cust1_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('HasOrders', 'has@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust1_id: uuid::Uuid = cust1_row.get(0);

    let cust2_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('NoOrders', 'no@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust2_id: uuid::Uuid = cust2_row.get(0);

    // Create an active (non-cancelled) order for customer 1
    inner
        .execute(
            "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 50.0)",
            &[&cust1_id],
        )
        .await
        .unwrap();

    // Customer 1 should have active orders
    let result1 = customers::has_active_orders(
        &client,
        &customers::HasActiveOrdersParams {
            customer_id: types::CustomerId(cust1_id),
        },
    )
    .await
    .unwrap();
    assert_eq!(result1.len(), 1);
    assert_eq!(result1[0].id.0, cust1_id);

    // Customer 2 should have no active orders
    let result2 = customers::has_active_orders(
        &client,
        &customers::HasActiveOrdersParams {
            customer_id: types::CustomerId(cust2_id),
        },
    )
    .await
    .unwrap();
    assert_eq!(result2.len(), 0);
}

#[tokio::test]
async fn test_distinct() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('SingleCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    // Insert 3 products in the same category
    for name in ["P1", "P2", "P3"] {
        inner
            .execute(
                "INSERT INTO products (category_id, name, price, stock) VALUES ($1, $2, 10.0, 5)",
                &[&cat_id, &name],
            )
            .await
            .unwrap();
    }

    let results = products::find_distinct_category_ids(&client).await.unwrap();
    // All 3 products are in the same category, so DISTINCT should return 1
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].category_id.0, cat_id);
}

#[tokio::test]
async fn test_count_orders_by_status() {
    let client = setup().await;
    let inner = client.inner();

    let cust_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('Counter', 'counter@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    // 2 pending, 1 shipped, 1 cancelled
    for status in ["pending", "pending", "shipped", "cancelled"] {
        inner
            .execute(
                "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, $2, 10.0)",
                &[&cust_id, &status],
            )
            .await
            .unwrap();
    }

    let results = analytics::count_orders_by_status(&client).await.unwrap();
    assert_eq!(results.len(), 3);

    let pending = results.iter().find(|r| r.status == "pending").unwrap();
    assert_eq!(pending.order_count, 2);

    let shipped = results.iter().find(|r| r.status == "shipped").unwrap();
    assert_eq!(shipped.order_count, 1);

    let cancelled = results.iter().find(|r| r.status == "cancelled").unwrap();
    assert_eq!(cancelled.order_count, 1);
}

#[tokio::test]
async fn test_sales_by_category() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('SalesCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let prod_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Widget', 25.0, 100) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);

    let cust_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('Buyer', 'buyer@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    let ord_row = inner
        .query_one(
            "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 75.0) RETURNING id",
            &[&cust_id],
        )
        .await
        .unwrap();
    let ord_id: uuid::Uuid = ord_row.get(0);

    // 2 order items: qty 2 @ 25.0 and qty 1 @ 25.0 = total 75.0, 2 items
    inner
        .execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, 2, 25.0)",
            &[&ord_id, &prod_id],
        )
        .await
        .unwrap();

    // Need a second product for a second order item in same category
    let prod2_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Gadget', 50.0, 50) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod2_id: uuid::Uuid = prod2_row.get(0);

    inner
        .execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, 1, 50.0)",
            &[&ord_id, &prod2_id],
        )
        .await
        .unwrap();

    // total_revenue = (2 * 25.0) + (1 * 50.0) = 100.0, total_items = 2
    let results = analytics::sales_by_category(&client).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].category_id.0, cat_id);
    assert_eq!(results[0].total_revenue, 100.0);
    assert_eq!(results[0].total_items, 2);
}

#[tokio::test]
async fn test_top_customers_with_having() {
    let client = setup().await;
    let inner = client.inner();

    let cust1_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('BigSpender', 'big@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust1_id: uuid::Uuid = cust1_row.get(0);

    let cust2_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('SmallBuyer', 'small@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust2_id: uuid::Uuid = cust2_row.get(0);

    // BigSpender gets 3 orders
    for amount in [100.0_f64, 200.0_f64, 300.0_f64] {
        inner
            .execute(
                "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', $2)",
                &[&cust1_id, &amount],
            )
            .await
            .unwrap();
    }

    // SmallBuyer gets 1 order
    inner
        .execute(
            "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 50.0)",
            &[&cust2_id],
        )
        .await
        .unwrap();

    // min_orders=2: only BigSpender qualifies
    let results = analytics::top_customers(
        &client,
        &analytics::TopCustomersParams { min_orders: 2 },
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].customer_id.0, cust1_id);
    assert_eq!(results[0].order_count, 3);
    assert_eq!(results[0].total_spent, 600.0);
}

#[tokio::test]
async fn test_nested_relation_join() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('Electronics', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let prod_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Phone', 999.0, 10) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);

    let cust_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('Reviewer', 'rev@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    inner
        .execute(
            "INSERT INTO reviews (product_id, customer_id, rating, comment) VALUES ($1, $2, 5, 'Excellent!')",
            &[&prod_id, &cust_id],
        )
        .await
        .unwrap();

    let results = reviews::find_high_rated_reviews(
        &client,
        &reviews::FindHighRatedReviewsParams { min_rating: 4 },
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rating, 5);
    assert_eq!(results[0].product_name, "Phone");
    assert_eq!(results[0].category_name, "Electronics");
    assert_eq!(results[0].comment, Some("Excellent!".to_string()));
}

#[tokio::test]
async fn test_on_conflict_do_nothing() {
    let client = setup().await;
    let inner = client.inner();

    let cust_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('Reviewer', 'rev@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('Cat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let prod_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Item', 10.0, 5) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);

    // First insert: should succeed with count=1
    let count1 = reviews::create_review(
        &client,
        &reviews::CreateReviewParams {
            product_id: types::ProductId(prod_id),
            customer_id: types::CustomerId(cust_id),
            rating: 5,
            comment: Some("Great!".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(count1, 1);

    // Second insert with same product+customer: ON CONFLICT DO NOTHING, count=0
    let count2 = reviews::create_review(
        &client,
        &reviews::CreateReviewParams {
            product_id: types::ProductId(prod_id),
            customer_id: types::CustomerId(cust_id),
            rating: 3,
            comment: Some("Changed my mind".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(count2, 0);

    // Verify only 1 review exists with original data
    let count: i64 = inner
        .query_one(
            "SELECT COUNT(*) FROM reviews WHERE product_id = $1 AND customer_id = $2",
            &[&prod_id, &cust_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);

    let rating: i32 = inner
        .query_one(
            "SELECT rating FROM reviews WHERE product_id = $1 AND customer_id = $2",
            &[&prod_id, &cust_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rating, 5); // Original rating preserved
}

// BUG: The generated FindOrderItemsRow has `price: String` but products.price is DOUBLE PRECISION.
#[tokio::test]
async fn test_find_order_items_with_product_join() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('JoinCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let prod_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'JoinProduct', 42.5, 10) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);

    let cust_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('ItemBuyer', 'item@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    let ord_row = inner
        .query_one(
            "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 85.0) RETURNING id",
            &[&cust_id],
        )
        .await
        .unwrap();
    let ord_id: uuid::Uuid = ord_row.get(0);

    inner
        .execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, 2, 42.5)",
            &[&ord_id, &prod_id],
        )
        .await
        .unwrap();

    let results = orders::find_order_items(
        &client,
        &orders::FindOrderItemsParams {
            order_id: types::OrderId(ord_id),
        },
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].quantity, 2);
    assert_eq!(results[0].unit_price, 42.5);
    assert_eq!(results[0].name, "JoinProduct");
    assert_eq!(results[0].price, 42.5);
}

#[tokio::test]
async fn test_delete_without_returning() {
    let client = setup().await;
    let inner = client.inner();

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('DelCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let prod_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'DelProd', 10.0, 5) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);

    let cust_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('Deleter', 'del@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    let ord_row = inner
        .query_one(
            "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 10.0) RETURNING id",
            &[&cust_id],
        )
        .await
        .unwrap();
    let ord_id: uuid::Uuid = ord_row.get(0);

    let item_row = inner
        .query_one(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, 1, 10.0) RETURNING id",
            &[&ord_id, &prod_id],
        )
        .await
        .unwrap();
    let item_id: uuid::Uuid = item_row.get(0);

    // delete_order_item returns u64 count, not the deleted row
    let count = orders::delete_order_item(
        &client,
        &orders::DeleteOrderItemParams { id: item_id },
    )
    .await
    .unwrap();
    assert_eq!(count, 1);

    // Verify item is gone
    let remaining: i64 = inner
        .query_one(
            "SELECT COUNT(*) FROM order_items WHERE id = $1",
            &[&item_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn test_exists_in_where() {
    let client = setup().await;
    let inner = client.inner();

    let cust_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('ExistsBuyer', 'exists@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('ExistsCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let expensive_prod = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Expensive', 1000.0, 5) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let expensive_prod_id: uuid::Uuid = expensive_prod.get(0);

    let cheap_prod = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'Cheap', 5.0, 100) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let cheap_prod_id: uuid::Uuid = cheap_prod.get(0);

    // Order with expensive item
    let ord1_row = inner
        .query_one(
            "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 1000.0) RETURNING id",
            &[&cust_id],
        )
        .await
        .unwrap();
    let ord1_id: uuid::Uuid = ord1_row.get(0);
    inner
        .execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, 1, 1000.0)",
            &[&ord1_id, &expensive_prod_id],
        )
        .await
        .unwrap();

    // Order with cheap item
    let ord2_row = inner
        .query_one(
            "INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'pending', 5.0) RETURNING id",
            &[&cust_id],
        )
        .await
        .unwrap();
    let ord2_id: uuid::Uuid = ord2_row.get(0);
    inner
        .execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, 1, 5.0)",
            &[&ord2_id, &cheap_prod_id],
        )
        .await
        .unwrap();

    // min_price=100: should only return the order with the expensive item
    let results = orders::find_orders_with_high_value_items(
        &client,
        &orders::FindOrdersWithHighValueItemsParams { min_price: 100.0 },
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id.0, ord1_id);
    assert_eq!(results[0].total_amount, 1000.0);
}

#[tokio::test]
async fn test_find_reviews_without_comments() {
    let client = setup().await;
    let inner = client.inner();

    let cust1_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('Commenter', 'comm@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust1_id: uuid::Uuid = cust1_row.get(0);

    let cust2_row = inner
        .query_one(
            "INSERT INTO customers (name, email) VALUES ('Silent', 'silent@test.com') RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cust2_id: uuid::Uuid = cust2_row.get(0);

    let cat_row = inner
        .query_one(
            "INSERT INTO categories (name, sort_order) VALUES ('ReviewCat', 1) RETURNING id",
            &[],
        )
        .await
        .unwrap();
    let cat_id: uuid::Uuid = cat_row.get(0);

    let prod_row = inner
        .query_one(
            "INSERT INTO products (category_id, name, price, stock) VALUES ($1, 'ReviewProd', 15.0, 20) RETURNING id",
            &[&cat_id],
        )
        .await
        .unwrap();
    let prod_id: uuid::Uuid = prod_row.get(0);

    // Review with comment
    inner
        .execute(
            "INSERT INTO reviews (product_id, customer_id, rating, comment) VALUES ($1, $2, 4, 'Nice product!')",
            &[&prod_id, &cust1_id],
        )
        .await
        .unwrap();

    // Review without comment (NULL)
    inner
        .execute(
            "INSERT INTO reviews (product_id, customer_id, rating) VALUES ($1, $2, 3)",
            &[&prod_id, &cust2_id],
        )
        .await
        .unwrap();

    let results = reviews::find_reviews_without_comments(
        &client,
        &reviews::FindReviewsWithoutCommentsParams {
            product_id: types::ProductId(prod_id),
        },
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rating, 3);
    assert_eq!(results[0].name, "Silent"); // Customer name from JOIN
}

#[tokio::test]
async fn test_order_status_breakdown_with_filter() {
    let client = setup().await;
    let inner = client.inner();

    let cust_row = inner.query_one(
        "INSERT INTO customers (name, email) VALUES ('FilterTest', 'filter@example.com') RETURNING id",
        &[],
    ).await.unwrap();
    let cust_id: uuid::Uuid = cust_row.get(0);

    // Insert orders with different statuses
    inner.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'active', 100.0)", &[&cust_id]).await.unwrap();
    inner.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'active', 200.0)", &[&cust_id]).await.unwrap();
    inner.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'shipped', 150.0)", &[&cust_id]).await.unwrap();
    inner.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES ($1, 'cancelled', 50.0)", &[&cust_id]).await.unwrap();

    let results = analytics::order_status_breakdown(&client).await.unwrap();
    assert_eq!(results.len(), 1);

    let row = &results[0];
    assert_eq!(row.total_orders, 4);
    assert_eq!(row.active_orders, 2);
    assert_eq!(row.shipped_orders, 1);
    assert_eq!(row.cancelled_orders, 1);
    assert_eq!(row.active_revenue, 300.0); // 100 + 200
}
