use std::path::PathBuf;
use std::process::Command;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn load_schema() -> nextsql_core::schema::DatabaseSchema {
    let schema_path = project_root().join("examples/sample-ec-project/schema.json");
    let content = std::fs::read_to_string(&schema_path).unwrap();
    serde_json::from_str(&content).unwrap()
}

#[test]
fn test_generated_code_compiles() {
    // Generate code
    let schema = load_schema();
    let source_dir = project_root().join("examples/sample-ec-project");

    let temp_dir = std::env::temp_dir().join("nextsql-compile-test");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let src_dir = temp_dir.join("src");
    let generated_dir = src_dir.join("generated");
    std::fs::create_dir_all(&generated_dir).unwrap();

    // Generate into the src/generated directory
    let config = nextsql_codegen::CodegenConfig {
        source_dir: source_dir.clone(),
        output_dir: generated_dir.clone(),
        backend: "rust".to_string(),
    };
    let result = nextsql_codegen::generate(&config, &schema);
    assert!(
        result.errors.is_empty(),
        "Generation errors: {:?}",
        result.errors
    );

    // Write Cargo.toml with dependencies needed by the generated runtime.rs
    let cargo_toml = r#"[package]
name = "nextsql-compile-test"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio-postgres = { version = "0.7", features = ["with-uuid-1", "with-chrono-0_4", "with-serde_json-1"] }
bytes = "1"
uuid = { version = "1", features = ["v4"] }
chrono = { version = "0.4" }
rust_decimal = { version = "1", features = ["db-tokio-postgres"] }
serde_json = "1"
"#;

    std::fs::write(temp_dir.join("Cargo.toml"), cargo_toml).unwrap();

    // Write lib.rs that includes generated modules
    let lib_rs = "pub mod generated;\n";
    std::fs::write(src_dir.join("lib.rs"), lib_rs).unwrap();

    // Run cargo check to verify the generated code compiles
    let output = Command::new("cargo")
        .arg("check")
        .current_dir(&temp_dir)
        .output()
        .expect("Failed to run cargo check");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Generated code failed to compile:\n{}",
        stderr
    );
}

#[test]
fn test_generated_function_signatures() {
    // Generate code and verify function signatures have correct types
    let schema = load_schema();
    let source_dir = project_root().join("examples/sample-ec-project");

    let output_dir = std::env::temp_dir().join("nextsql-sig-test");
    let _ = std::fs::remove_dir_all(&output_dir);

    let config = nextsql_codegen::CodegenConfig {
        source_dir,
        output_dir: output_dir.clone(),
        backend: "rust".to_string(),
    };
    let result = nextsql_codegen::generate(&config, &schema);
    assert!(result.errors.is_empty(), "Errors: {:?}", result.errors);

    let customers = std::fs::read_to_string(output_dir.join("customers.rs")).unwrap();

    // find_customer_by_id returns Vec<Customer> (model struct reuse)
    assert!(
        customers.contains("-> Result<Vec<Customer>,"),
        "find_customer_by_id should return Vec<Customer>, got:\n{}",
        customers
    );
    // create_customer is present
    assert!(
        customers.contains("pub async fn create_customer("),
        "Should have create_customer function"
    );
    // deactivate_customer returns u64 (no RETURNING)
    assert!(
        customers.contains("-> Result<u64,"),
        "deactivate_customer should return u64"
    );
    // Params uses valtype
    assert!(
        customers.contains("pub id: CustomerId"),
        "Params should use CustomerId valtype"
    );

    let orders = std::fs::read_to_string(output_dir.join("orders.rs")).unwrap();
    // upsert returns Vec<OrderItem>
    assert!(
        orders.contains("-> Result<Vec<OrderItem>,"),
        "upsert should return Vec<OrderItem>"
    );
    // cancel_order is present
    assert!(
        orders.contains("pub async fn cancel_order("),
        "Should have cancel_order function"
    );
}
