# CLI Reference

The `nsql` command-line tool provides project initialization, validation, code generation, and migration management.

## Installation

**macOS / Linux:**

```bash
curl -fsSL https://github.com/shirakawayohane/next-sql/releases/latest/download/nextsql-cli-installer.sh | bash
```

**Windows PowerShell:**

```powershell
irm https://github.com/shirakawayohane/next-sql/releases/latest/download/nextsql-cli-installer.ps1 | iex
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

### watch

Watch for file changes and automatically re-run code generation:

```bash
nsql watch
```

Monitors `.nsql` files, `next-sql.toml`, and `schema.json` for changes. Errors during generation are reported but do not stop the watcher.

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
