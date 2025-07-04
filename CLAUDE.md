# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NextSQL is a modern SQL-compatible language with type safety, designed to provide secure and readable database queries. It's implemented as a parser in Rust using the Pest parsing library.

## Development Commands

### Build and Test
- `cargo build` - Build the project
- `cargo test` - Run all tests
- `cargo run` - Run the main binary (currently empty)

### Individual Test Commands
- `cargo test simple_select_test` - Test simple SELECT parsing
- `cargo test insert_test` - Test INSERT parsing
- `cargo test update_test` - Test UPDATE parsing
- `cargo test delete_test` - Test DELETE parsing
- `cargo test test_parse_expression` - Test expression parsing

## Architecture

### Core Components

1. **Grammar Definition (`src/nextsql.pest`)**
   - Pest grammar file defining NextSQL syntax
   - Supports queries, mutations, expressions, types, and migrations
   - Key constructs: `query`, `mutation`, `expression`, `type`, `target`

2. **AST Definitions (`src/ast.rs`)**
   - Complete AST node definitions for all language constructs
   - Main types: `Module`, `Query`, `Mutation`, `Expression`, `Type`
   - Supports both queries (SELECT) and mutations (INSERT/UPDATE/DELETE)

3. **Parser Implementation (`src/parser.rs`)**
   - Converts Pest parse trees to AST nodes
   - Extensive test coverage for all parsing scenarios
   - Function pattern: `parse_*` functions for each AST node type

### Language Features

- **Type System**: Built-in types (i16, i32, i64, f32, f64, string, bool, uuid, timestamp, date), utility types (Insertable<T>), optional types (T?), arrays ([T]), user-defined types
- **Queries**: SELECT with FROM, WHERE, JOIN operations, subqueries
- **Mutations**: INSERT, UPDATE, DELETE with optional RETURNING clauses
- **Expressions**: Binary operations, function calls, literals, variables, index access
- **Variables**: Prefixed with `$` (e.g., `$id`, `$name`)
- **Aliases**: Table aliases using `<alias>` syntax (e.g., `users<u>`)

### File Structure

- `src/main.rs` - Entry point (currently minimal)
- `src/parser.rs` - Main parser logic with comprehensive tests
- `src/ast.rs` - AST node definitions
- `src/nextsql.pest` - Grammar specification
- `examples/` - Example NextSQL files demonstrating language features

### Testing Strategy

Tests are embedded in `src/parser.rs` using `#[test]` attributes. Each major parsing function has corresponding tests that verify both successful parsing and correct AST generation.

## Example NextSQL Syntax

```nsql
// Query example
query findUserById($id: uuid) {
  from(users)
  .where(users.id == $id)
  .select(users.*)
}

// Mutation example
mutation insertUser($name: string, $email: string) {
  insert(users)
  .value({
    name: $name,
    email: $email,
  })
}
```

## Development Notes

- The parser uses recursive descent parsing with operator precedence
- Expression parsing follows standard precedence rules (logical -> equality -> relational -> additive -> multiplicative -> unary -> atomic)
- All parsing functions return `Result` types for error handling
- The codebase contains Japanese comments in test functions