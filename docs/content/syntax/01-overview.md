# Overview

NextSQL is a modern SQL-compatible DSL (Domain Specific Language) that generates type-safe database modules from expressive query definitions. Instead of writing raw SQL strings or using heavy ORMs, you define queries as typed functions in `.nsql` files and generate idiomatic code for your target language.

## Design Principles

- **Secure and Modern Syntax** — Type checking eliminates runtime errors. Schema-aware validation catches mistakes at build time.
- **Readability** — Method-chained clauses read top to bottom. No nested string templates, no magic.
- **Full SQL Expressiveness** — JOINs, subqueries, CTEs, aggregations, UPSERT — if SQL can do it, NextSQL can express it.
- **IDE-first** — The syntax is designed for autocompletion. Dot-access on tables and relations gives your editor the context it needs.
- **Portable Knowledge** — Learn NextSQL once, generate code for any supported backend.

## File Structure

NextSQL files use the `.nsql` extension. A typical project looks like this:

```
my-project/
├── next-sql.toml          # Project configuration
├── src/
│   ├── types.nsql         # Shared types, relations, aggregations
│   ├── users.nsql         # User queries and mutations
│   ├── orders.nsql        # Order queries and mutations
│   └── analytics.nsql     # Analytics queries
└── generated/             # Output directory for generated code
```

## Configuration

Projects are configured via `next-sql.toml`:

```toml
# Optional: database URL for schema-aware validation
database_url = "postgresql://user:pass@localhost:5432/mydb"

[files]
includes = ["**"]            # Glob patterns for .nsql files

[target]
target_language = "rust"        # Target language (currently: "rust")
target_directory = "../generated"  # Output directory
```

## Supported Backends

| Language | Status |
|----------|--------|
| Rust     | Stable |

More backends are planned. The architecture is designed for multi-language support.
