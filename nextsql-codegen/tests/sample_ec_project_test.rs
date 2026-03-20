use std::path::PathBuf;
use nextsql_codegen::{CodegenConfig, generate};
use nextsql_core::schema::DatabaseSchema;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn load_schema() -> DatabaseSchema {
    let schema_path = project_root().join("examples/sample-ec-project/schema.json");
    let content = std::fs::read_to_string(&schema_path).unwrap();
    serde_json::from_str(&content).unwrap()
}

fn run_codegen(test_name: &str) -> (nextsql_codegen::CodegenResult, PathBuf) {
    let schema = load_schema();
    let source_dir = project_root().join("examples/sample-ec-project");
    let temp_dir = std::env::temp_dir().join(format!("nextsql-test-ec-{}", test_name));
    let _ = std::fs::remove_dir_all(&temp_dir);
    let src_dir = temp_dir.join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    let config = CodegenConfig {
        source_dir,
        output_dir: src_dir.clone(),
        backend: "rust".to_string(),
        insert_params_pattern: None,
        update_params_pattern: None,
        package_name: None,
        type_files: vec!["types.nsql".to_string()],
        runtime_crate_path: None,
    };

    let result = generate(&config, &schema);
    let generated_dir = src_dir.join("generated");
    (result, generated_dir)
}

#[test]
fn test_sample_ec_project_generates_successfully() {
    let (result, _output_dir) = run_codegen("gen");
    assert!(
        result.errors.is_empty(),
        "Generation errors: {:?}",
        result.errors
    );
    // 5 .nsql files (analytics, customers, orders, products, reviews) + types.rs + lib.rs = 7
    assert!(
        result.generated_files.len() >= 7,
        "Expected at least 7 files, got {}",
        result.generated_files.len()
    );
}

#[test]
fn test_customers_model_struct_with_schema() {
    let (result, output_dir) = run_codegen("model");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("customers.rs")).unwrap();

    // When schema is present, a Customer model struct should be generated
    assert!(
        content.contains("pub struct Customer {"),
        "Should generate Customer model struct"
    );
    // CustomerId valtype is in types.rs (shared across modules)
    let types_content = std::fs::read_to_string(output_dir.join("types.rs")).unwrap();
    assert!(
        types_content.contains("pub struct CustomerId(pub uuid::Uuid)"),
        "Should generate CustomerId valtype in types.rs"
    );
    // Module should import from types
    assert!(
        content.contains("use super::types::*"),
        "Module should import from shared types"
    );
    // Customer model uses CustomerId for id field
    assert!(
        content.contains("pub id: CustomerId"),
        "Customer model should use CustomerId for id field"
    );
    // Nullable phone field should be Option
    assert!(
        content.contains("pub phone: Option<String>"),
        "Nullable phone should be Option<String>"
    );
    // Bool field
    assert!(
        content.contains("pub is_active: bool"),
        "Should have is_active bool field"
    );
    // Timestamp field
    assert!(
        content.contains("pub created_at: chrono::NaiveDateTime"),
        "Should have created_at timestamp field"
    );
}

#[test]
fn test_orders_model_structs_with_schema() {
    let (result, output_dir) = run_codegen("orders-model");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("orders.rs")).unwrap();

    // Both Order and OrderItem model structs should be present
    assert!(
        content.contains("pub struct Order {"),
        "Should generate Order model struct"
    );
    assert!(
        content.contains("pub struct OrderItem {"),
        "Should generate OrderItem model struct"
    );

    // Order should reference CustomerId
    assert!(
        content.contains("pub customer_id: CustomerId"),
        "Order should use CustomerId for customer_id field"
    );
    // OrderItem should reference OrderId and ProductId
    assert!(
        content.contains("pub order_id: OrderId"),
        "OrderItem should use OrderId"
    );
    assert!(
        content.contains("pub product_id: ProductId"),
        "OrderItem should use ProductId"
    );
    // Nullable shipped_at
    assert!(
        content.contains("pub shipped_at: Option<chrono::NaiveDateTime>"),
        "Nullable shipped_at should be Option"
    );
}

#[test]
fn test_orders_sql_contains_joins() {
    let (result, output_dir) = run_codegen("joins");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("orders.rs")).unwrap();

    // findOrderById should JOIN customers via relation
    assert!(
        content.contains("INNER JOIN customers"),
        "findOrderById should auto-join customers table"
    );
    // Should have proper ON clause
    assert!(
        content.contains("ON customers.id = orders.customer_id"),
        "JOIN should have correct ON clause"
    );
}

#[test]
fn test_orders_has_upsert_and_union() {
    let (result, output_dir) = run_codegen("upsert");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("orders.rs")).unwrap();

    assert!(
        content.contains("ON CONFLICT"),
        "Should have ON CONFLICT for upsert"
    );
    assert!(
        content.contains("DO UPDATE SET"),
        "Should have DO UPDATE for upsert"
    );
    assert!(
        content.contains("DO NOTHING"),
        "Should have DO NOTHING"
    );
    assert!(
        content.contains("UNION"),
        "Should have UNION"
    );
}

#[test]
fn test_orders_between_and_eqany() {
    let (result, output_dir) = run_codegen("ops");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("orders.rs")).unwrap();

    assert!(
        content.contains("BETWEEN"),
        "Should have BETWEEN operator"
    );
    assert!(
        content.contains("= ANY("),
        "Should have ANY() for eqAny"
    );
}

#[test]
fn test_products_has_distinct_and_is_null() {
    let (result, output_dir) = run_codegen("products-ops");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("products.rs")).unwrap();

    assert!(
        content.contains("IS NULL"),
        "Should have IS NULL"
    );
    assert!(
        content.contains("DISTINCT"),
        "Should have DISTINCT"
    );
    assert!(
        content.contains("IS NOT NULL"),
        "Should have IS NOT NULL"
    );
    assert!(
        content.contains("BETWEEN"),
        "Should have BETWEEN for price range"
    );
    assert!(
        content.contains("= ANY("),
        "Should have ANY() for eqAny"
    );
}

#[test]
fn test_products_model_struct() {
    let (result, output_dir) = run_codegen("products-model");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("products.rs")).unwrap();

    assert!(
        content.contains("pub struct Product {"),
        "Should generate Product model struct"
    );
    assert!(
        content.contains("pub id: ProductId"),
        "Product should use ProductId"
    );
    assert!(
        content.contains("pub category_id: CategoryId"),
        "Product should use CategoryId"
    );
    assert!(
        content.contains("pub description: Option<String>"),
        "Nullable description should be Option<String>"
    );
    assert!(
        content.contains("pub price: f64"),
        "Price should be f64"
    );
    assert!(
        content.contains("pub stock: i32"),
        "Stock should be i32"
    );
}

#[test]
fn test_analytics_has_aggregations() {
    let (result, output_dir) = run_codegen("agg");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("analytics.rs")).unwrap();

    assert!(
        content.contains("GROUP BY"),
        "Should have GROUP BY clause"
    );
    assert!(
        content.contains("COUNT("),
        "Should have COUNT"
    );
    assert!(
        content.contains("SUM("),
        "Should have SUM"
    );
    assert!(
        content.contains("HAVING"),
        "Should have HAVING clause"
    );
    assert!(
        content.contains("AVG("),
        "Should have AVG"
    );
    assert!(
        content.contains("MIN("),
        "Should have MIN"
    );
    assert!(
        content.contains("MAX("),
        "Should have MAX"
    );
}

#[test]
fn test_reviews_has_relation_joins() {
    let (result, output_dir) = run_codegen("reviews-rel");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("reviews.rs")).unwrap();

    // findHighRatedReviews uses reviews.product.name -> should join products
    assert!(
        content.contains("INNER JOIN products"),
        "Should join products via relation"
    );
    // findHighRatedReviews uses reviews.product.category.name -> should also join categories
    assert!(
        content.contains("INNER JOIN categories"),
        "Should join categories via nested relation (product -> category)"
    );
    assert!(
        content.contains("categories.name"),
        "Should resolve nested relation to categories.name"
    );
    // findProductReviews uses reviews.reviewer.name -> should join customers
    assert!(
        content.contains("INNER JOIN customers"),
        "Should join customers via reviewer relation"
    );

    // Review model struct should be generated with schema
    assert!(
        content.contains("pub struct Review {"),
        "Should generate Review model struct"
    );
    assert!(
        content.contains("pub rating: i32"),
        "Review should have rating as i32"
    );
    assert!(
        content.contains("pub comment: Option<String>"),
        "Review should have comment as Option<String>"
    );
}

#[test]
fn test_reviews_on_conflict_do_nothing() {
    let (result, output_dir) = run_codegen("reviews-conflict");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("reviews.rs")).unwrap();

    assert!(
        content.contains("ON CONFLICT (product_id, customer_id) DO NOTHING"),
        "createReview should have ON CONFLICT DO NOTHING"
    );
}

#[test]
fn test_mod_rs_has_all_modules() {
    let (result, output_dir) = run_codegen("mod");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    // generated/mod.rs should declare all modules
    let mod_content = std::fs::read_to_string(output_dir.join("mod.rs")).unwrap();
    assert!(mod_content.contains("pub mod types;"), "Should declare types (shared valtypes)");
    assert!(mod_content.contains("pub mod analytics;"), "Should declare analytics");
    assert!(mod_content.contains("pub mod customers;"), "Should declare customers");
    assert!(mod_content.contains("pub mod orders;"), "Should declare orders");
    assert!(mod_content.contains("pub mod products;"), "Should declare products");
    assert!(mod_content.contains("pub mod reviews;"), "Should declare reviews");

    // lib.rs should re-export from generated module
    let lib_content = std::fs::read_to_string(output_dir.parent().unwrap().join("lib.rs")).unwrap();
    assert!(lib_content.contains("mod generated;"), "Should declare generated module");
    assert!(lib_content.contains("pub use generated::types;"), "Should re-export types");
    assert!(lib_content.contains("pub use generated::analytics;"), "Should re-export analytics");
    assert!(lib_content.contains("pub use generated::customers;"), "Should re-export customers");
}

#[test]
fn test_schema_json_deserialization() {
    // Verify schema.json can be properly deserialized
    let schema = load_schema();

    assert!(schema.get_table("customers").is_some(), "Should have customers table");
    assert!(schema.get_table("categories").is_some(), "Should have categories table");
    assert!(schema.get_table("products").is_some(), "Should have products table");
    assert!(schema.get_table("orders").is_some(), "Should have orders table");
    assert!(schema.get_table("order_items").is_some(), "Should have order_items table");
    assert!(schema.get_table("reviews").is_some(), "Should have reviews table");
    assert!(schema.get_table("addresses").is_some(), "Should have addresses table");

    let customers = schema.get_table("customers").unwrap();
    assert_eq!(customers.columns.len(), 6, "customers should have 6 columns");
    assert!(customers.get_column("id").unwrap().primary_key);
    assert!(customers.get_column("id").unwrap().has_default);
    assert!(!customers.get_column("email").unwrap().nullable);
    assert!(customers.get_column("phone").unwrap().nullable);
}

#[test]
fn test_params_use_valtypes() {
    let (result, output_dir) = run_codegen("params-valtype");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("customers.rs")).unwrap();

    // findCustomerById should use CustomerId valtype for the id parameter, not uuid::Uuid
    assert!(
        content.contains("id: &CustomerId,"),
        "findCustomerById param should use CustomerId type, got:\n{}",
        content
    );
}

#[test]
fn test_mutation_returning_uses_model() {
    let (result, output_dir) = run_codegen("returning");
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let content = std::fs::read_to_string(output_dir.join("customers.rs")).unwrap();

    // create_customer with RETURNING should return Vec<Customer>
    assert!(
        content.contains("Result<Vec<Customer>, Box<dyn std::error::Error"),
        "create_customer should return Vec<Customer>"
    );
    // deactivate_customer without RETURNING should return u64
    assert!(
        content.contains("Result<u64, Box<dyn std::error::Error"),
        "deactivate_customer should return u64"
    );
}
