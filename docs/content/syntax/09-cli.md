# CLI Reference

The `nextsql` command-line tool provides project initialization, validation, code generation, and migration management.

## Installation

Install from [GitHub Releases](https://github.com/shirakawayohane/next-sql/releases) or build from source:

```bash
cargo install nextsql-cli
```

## Commands

### init

Create a new NextSQL project with default configuration:

```bash
# Initialize in current directory
nextsql init

# Initialize in a specific directory
nextsql init my-project
```

This creates a `next-sql.toml` file and the recommended directory structure.

### parse

Parse and validate a single `.nsql` file:

```bash
nextsql parse queries.nsql
```

### check

Validate all `.nsql` files without generating code:

```bash
nextsql check -s src/
nextsql check -s src/ --schema schema.json
```

### generate

Generate code from `.nsql` files:

```bash
nextsql generate -s src/ -o generated/ -b rust
nextsql generate -s src/ -o generated/ --schema schema.json
```

| Flag | Description |
|------|-------------|
| `-s, --source` | Source directory containing `.nsql` files |
| `-o, --output` | Output directory for generated code |
| `-b, --backend` | Target language backend (default: `rust`) |
| `--schema` | Path to database schema JSON file |

## Migration Commands

### Initialize

```bash
nextsql migration init
```

### Generate

```bash
nextsql migration generate create_users_table -d "Create users table"
```

### List

```bash
nextsql migration list
```

### Run Migrations

```bash
# File-based
nextsql migration up
nextsql migration up <timestamp>
nextsql migration down <timestamp>

# Against database
nextsql migration db-up --host localhost --port 5432 --database mydb --username user --password pass
nextsql migration db-down <timestamp> --host localhost --port 5432 --database mydb --username user --password pass
nextsql migration db-status --host localhost --port 5432 --database mydb --username user --password pass
```

## VSCode Extension

Install the **NextSQL Language Support** extension from the VS Code Marketplace for:

- Syntax highlighting for `.nsql` files
- Real-time error detection via the language server
- Context-aware autocompletion (triggered by `.`)
- Schema viewer and diagnostics
