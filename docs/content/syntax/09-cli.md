# CLI Reference

The `nsql` command-line tool provides project initialization, validation, code generation, and migration management.

## Installation

Download pre-built binaries from [GitHub Releases](https://github.com/shirakawayohane/next-sql/releases), or install with cargo:

```bash
cargo install nextsql-cli
```

## Commands

### init

Create a new NextSQL project with default configuration:

```bash
# Initialize in current directory
nsql init

# Initialize in a specific directory
nsql init my-project
```

This creates a `next-sql.toml` file and the recommended directory structure.

### parse

Parse and validate a single `.nsql` file:

```bash
nsql parse queries.nsql
```

### check

Validate all `.nsql` files without generating code:

```bash
nsql check
```

### gen

Generate code from `.nsql` files:

```bash
nsql gen
```

### validate-config

Validate a NextSQL configuration file:

```bash
nsql validate-config
nsql validate-config path/to/next-sql.toml
```

## Migration Commands

### Initialize

```bash
nsql migration init
```

### Generate

```bash
nsql migration generate create_users_table -d "Create users table"
```

### List

```bash
nsql migration list
```

### Run Migrations

```bash
# File-based
nsql migration up
nsql migration up <timestamp>
nsql migration down <timestamp>

# Against database
nsql migration db-up --host localhost --port 5432 --database mydb --username user --password pass
nsql migration db-down <timestamp> --host localhost --port 5432 --database mydb --username user --password pass
nsql migration db-status --host localhost --port 5432 --database mydb --username user --password pass
```

## VSCode Extension

Install the **NextSQL Language Support** extension from the VS Code Marketplace for:

- Syntax highlighting for `.nsql` files
- Real-time error detection via the language server
- Context-aware autocompletion (triggered by `.`)
- Schema viewer and diagnostics
