#[cfg(test)]
mod integration_tests {
    use crate::*;
    use crate::schema::{DatabaseSchema, TableSchema, ColumnSchema};
    use crate::type_validator::TypeValidator;
    use crate::type_system::TypeSystem;

    fn create_test_schema() -> DatabaseSchema {
        let mut schema = DatabaseSchema::new();
        
        let mut users_table = TableSchema::new("users".to_string());
        
        // Auto-generated ID with default
        users_table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        
        // Required email field
        users_table.add_column(ColumnSchema {
            name: "email".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        
        // Optional name field
        users_table.add_column(ColumnSchema {
            name: "name".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: true,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        
        // Age field (integer)
        users_table.add_column(ColumnSchema {
            name: "age".to_string(),
            column_type: Type::BuiltIn(BuiltInType::I32),
            nullable: true,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        
        // Created at with default
        users_table.add_column(ColumnSchema {
            name: "created_at".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Timestamp),
            nullable: false,
            primary_key: false,
            has_default: true,
            default_value: Some("CURRENT_TIMESTAMP".to_string()),
        });
        
        schema.add_table(users_table);
        schema
    }

    #[test]
    fn test_valid_insert_with_all_required_fields() {
        let schema = create_test_schema();
        let mut validator = TypeValidator::new(&schema);
        
        let nextsql_code = r#"
            mutation createUser($name: string, $email: string) {
                insert(users).value({
                    name: $name,
                    email: $email,
                })
            }
        "#;
        
        let module = parse_module(nextsql_code).unwrap();
        let errors = validator.validate_module(&module);
        
        // Should have no errors
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_insert_missing_required_field() {
        let schema = create_test_schema();
        let mut validator = TypeValidator::new(&schema);
        
        let nextsql_code = r#"
            mutation createUser($name: string) {
                insert(users).value({
                    name: $name,
                })
            }
        "#;
        
        let module = parse_module(nextsql_code).unwrap();
        let errors = validator.validate_module(&module);
        
        // Should have error about missing email
        assert!(errors.iter().any(|e| e.message.contains("Required field 'email' is missing")));
    }

    #[test]
    fn test_insert_with_unknown_field() {
        let schema = create_test_schema();
        let mut validator = TypeValidator::new(&schema);
        
        let nextsql_code = r#"
            mutation createUser($email: string) {
                insert(users).value({
                    email: $email,
                    unknown_field: "test",
                })
            }
        "#;
        
        let module = parse_module(nextsql_code).unwrap();
        let errors = validator.validate_module(&module);
        
        // Should have error about unknown field
        assert!(errors.iter().any(|e| e.message.contains("Unknown field 'unknown_field'")));
    }

    #[test]
    fn test_type_mismatch_numeric_field() {
        let schema = create_test_schema();
        let mut validator = TypeValidator::new(&schema);
        
        let nextsql_code = r#"
            mutation createUser($email: string, $age: string) {
                insert(users).value({
                    email: $email,
                    age: $age,
                })
            }
        "#;
        
        let module = parse_module(nextsql_code).unwrap();
        println!("Module: {:?}", module);
        let errors = validator.validate_module(&module);
        
        // Should have type mismatch error
        println!("Errors: {:?}", errors);
        assert!(!errors.is_empty(), "Expected type validation errors but got none");
        assert!(errors.iter().any(|e| e.message.contains("Type mismatch for field 'age'")), 
                "Expected type mismatch error for age field, but got: {:?}", errors);
    }

    #[test]
    fn test_insertable_type_generation() {
        let schema = create_test_schema();
        let mut type_system = TypeSystem::new();
        type_system.derive_from_schema(&schema);
        
        // Check that Insertable<users> was generated
        let insertable_users = type_system.get_type("Insertable<users>").unwrap();
        
        // Should include all fields except auto-generated id (primary key with default)
        // created_at is included but optional since it has a default
        assert_eq!(insertable_users.fields.len(), 4); // email, name, age, created_at
        
        // Email should be required
        let email_field = insertable_users.fields.iter().find(|f| f.name == "email").unwrap();
        assert!(!email_field.optional);
        
        // Name should be optional (nullable)
        let name_field = insertable_users.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.optional);
        
        // Age should be optional (nullable)
        let age_field = insertable_users.fields.iter().find(|f| f.name == "age").unwrap();
        assert!(age_field.optional);
        
        // Created_at should be optional (has default)
        let created_at_field = insertable_users.fields.iter().find(|f| f.name == "created_at").unwrap();
        assert!(created_at_field.optional);
    }

    #[test]
    fn test_update_with_valid_fields() {
        let schema = create_test_schema();
        let mut validator = TypeValidator::new(&schema);
        
        let nextsql_code = r#"
mutation updateUser($id: uuid, $name: string) {
    update(users)
    .where(users.id == $id)
    .set({
        name: $name
    })
}"#;
        
        let module = parse_module(nextsql_code).unwrap();
        let errors = validator.validate_module(&module);
        
        // Should have no errors
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_undefined_variable() {
        let schema = create_test_schema();
        let mut validator = TypeValidator::new(&schema);
        
        let nextsql_code = r#"
mutation createUser($email: string) {
    insert(users).value({
        email: $email,
        name: $undefined_var
    })
}"#;
        
        let module = parse_module(nextsql_code).unwrap();
        println!("Undefined var module: {:?}", module);
        let errors = validator.validate_module(&module);
        
        // Should have error about undefined variable
        println!("Undefined var errors: {:?}", errors);
        assert!(!errors.is_empty(), "Expected undefined variable error but got none");
        assert!(errors.iter().any(|e| e.message.contains("Undefined variable") && e.message.contains("undefined_var")), 
                "Expected undefined variable error, but got: {:?}", errors);
    }
}