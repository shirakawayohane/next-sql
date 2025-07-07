use nextsql_core::parse_module;

fn main() {
    let content = r#"query test() {
  from(users)
  .select(users.*)
}"#;

    println!("Attempting to parse:");
    println!("{}", content);
    println!();

    match parse_module(content) {
        Ok(module) => {
            println!("Parse successful!");
            println!("Module: {:?}", module);
        }
        Err(e) => {
            println!("Parse error: {:?}", e);
        }
    }
}