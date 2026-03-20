---
name: learn-next-sql
description: This skill should be used when the user asks to "write NextSQL code", "generate .nsql files", "create NextSQL queries", "convert SQL to NextSQL", or "learn NextSQL syntax". Use this to understand the NextSQL DSL before generating any .nsql code.
---

# NextSQL Language Reference

NextSQL is a modern SQL-compatible DSL with type safety. This reference covers all current features. Always refer to `nextsql-core/src/nextsql.pest` as the authoritative grammar source.

## Query Syntax

Queries are declared with `query` and use method-chained clauses starting from `from()`.

```nsql
query findUsers($minAge: i32, $limit: i32?) {
  from(users)
  .where(users.age >= $minAge)
  .select(users.id, users.name, users.email)
}
```

### Clauses (chained with `.`)

- `from(table)` - FROM clause (required). Supports explicit joins via method calls on the table.
- `.where(condition)` - WHERE filter.
- `.select(expr, ...)` - SELECT columns. Supports `alias: expr` for aliased columns and `table.*` for all columns.
- `.distinct()` - SELECT DISTINCT.
- `.groupBy(expr, ...)` - GROUP BY.
- `.having(condition)` - HAVING (used after groupBy).
- `.aggregate(alias: expr, ...)` - Named aggregation columns.
- `.orderBy(col.asc(), col.desc())` - ORDER BY with direction methods.
- `.limit(n)` - LIMIT.
- `.offset(n)` - OFFSET.
- `.when(condition, clause)` - Conditional clause application (dynamic queries).
- `.union(from(...).select(...))` - UNION.
- `.unionAll(from(...).select(...))` - UNION ALL.
- `.forUpdate()` - SELECT FOR UPDATE.

### Explicit JOINs

JOINs are expressed as method calls on tables inside `from()`:

```nsql
from(table1
  .innerJoin(table2, table1.id == table2.table1_id)
  .leftJoin(table3, table2.id == table3.table2_id)
  .rightJoin(table4, table3.id == table4.table3_id)
  .fullOuterJoin(table5, table4.id == table5.table4_id)
  .crossJoin(table6))
```

### WITH (CTE)

Common Table Expressions are defined with `with` before the main query body:

```nsql
query example() {
  with activePosts = {
    from(posts).where(posts.is_active == true).select(posts.*)
  }
  from(activePosts)
  .select(activePosts.title)
}
```

## Mutation Syntax

Mutations are declared with `mutation` and support INSERT, UPDATE, and DELETE.

### INSERT

```nsql
mutation createUser($name: string, $email: string) {
  insert(users)
  .value({
    name: $name,
    email: $email,
  })
  .returning(users.*)
}
```

**Multi-row insert** with `.values()`:

```nsql
mutation createUsers($records: [Insertable<users>]) {
  insert(users)
  .values($records)
  .returning(users.*)
}
```

### UPDATE

```nsql
mutation updateUser($id: uuid, $name: string) {
  update(users)
  .where(users.id == $id)
  .set({
    name: $name,
  })
  .returning(users.*)
}
```

Use `ChangeSet<T>` for partial updates with a variable:

```nsql
mutation updateUser($id: uuid, $data: ChangeSet<users>) {
  update(users)
  .where(users.id == $id)
  .set($data)
  .returning(users.*)
}
```

### DELETE

```nsql
mutation deleteUser($id: uuid) {
  delete(users)
  .where(users.id == $id)
  .returning(users.*)
}
```

### onConflict (UPSERT)

```nsql
mutation upsertUser($email: string, $name: string) {
  insert(users)
  .value({
    email: $email,
    name: $name,
  })
  .onConflict(email).doUpdate({
    name: excluded(name),
  })
  .returning(users.*)
}
```

Use `.doNothing()` to ignore conflicts:

```nsql
.onConflict(email).doNothing()
```

### RETURNING

All mutation types support `.returning(table.*, col1, col2)`.

## Parameters

### Individual Parameters (Default)

By default, each parameter declared in a query or mutation signature becomes an individual function argument in the generated code:

```nsql
query findUserById($id: uuid) {
  from(users).where(users.id == $id).select(users.*)
}
```

This generates a function like `fn find_user_by_id(client, id: &uuid::Uuid)` with `id` as a direct parameter.

### Input Types

Use `input` to group multiple parameters into a named struct:

```nsql
input CreateUserInput {
  name: string,
  email: string
}
```

Reference an input type as a parameter type:

```nsql
mutation createUser($input: CreateUserInput) {
  insert(users)
  .value({
    name: $input.name,
    email: $input.email,
  })
  .returning(users.*)
}
```

Access input fields using dot notation: `$input.name`, `$input.email`.

You can mix individual parameters with input parameters:

```nsql
query findByOwner($orgId: uuid, $filter: UserFilter) {
  from(users)
  .where(users.org_id == $orgId && users.status == $filter.status)
  .select(users.*)
}
```

## Type System

### Built-in Types

`i16`, `i32`, `i64`, `f32`, `f64`, `string`, `bool`, `uuid`, `timestamp`, `timestamptz`, `date`

### Modifiers

- **Optional**: `type?` (e.g., `string?`) - parameter may be null
- **Array**: `[type]` (e.g., `[uuid]`) - array of values

### Utility Types

- `Insertable<TableName>` - represents a full row for insertion. Generates a struct (default: `Insert{Table}Params`) with required/optional fields based on schema, plus a builder pattern.
- `ChangeSet<TableName>` - represents a partial row for updates. Generates a struct (default: `Update{Table}Params`) with all fields wrapped in `UpdateField<T>`, plus `Default` impl.

Naming patterns are configurable in `next-sql.toml`:

```toml
[codegen]
insert_params = "Insert{Table}Params"
update_params = "Update{Table}Params"
```

### Value Types (valtype)

Define named type aliases:

```nsql
// With column binding
valtype UserId = uuid for users.id
valtype Email = string for users.email

// Standalone named type
valtype Amount = f64
```

Use valtypes as parameter types:

```nsql
query findUser($id: UserId) {
  from(users).where(users.id == $id).select(users.*)
}
```

### The `types.nsql` Convention

Projects should place shared type definitions (valtypes, input types, relations, aggregations) in a `types.nsql` file to centralize reusable definitions across query/mutation files.

## Expressions

### Variables

Prefixed with `$`: `$id`, `$name`, `$values`

### Operators

- **Logical**: `&&` (AND), `||` (OR)
- **Equality**: `==`, `!=`
- **Comparison**: `<`, `<=`, `>`, `>=`
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Unary**: `!` (NOT)

### Method Expressions

Called on columns or expressions with dot notation:

- `col.isNull()` - IS NULL
- `col.isNotNull()` - IS NOT NULL
- `col.like(pattern)` - LIKE pattern matching
- `col.ilike(pattern)` - Case-insensitive LIKE
- `col.between(from, to)` - BETWEEN range
- `col.eqAny($arr)` - = ANY (array contains)
- `col.neAny($arr)` - != ANY (array not contains)
- `col.in([val1, val2])` - IN values list
- `col.asc()` - ORDER BY ascending (inside `.orderBy()`)
- `col.desc()` - ORDER BY descending (inside `.orderBy()`)

### Aggregate Functions

`SUM(expr)`, `COUNT(expr)`, `AVG(expr)`, `MIN(expr)`, `MAX(expr)`

### Subqueries

Subqueries are wrapped in `$()`:

```nsql
.where(users.id.in($(from(active_users).select(active_users.id))))
```

### EXISTS

```nsql
.where(exists($(from(orders).where(orders.user_id == users.id).select(orders.id))))
```

### Conditional Expressions

**when** - conditional clause application:

```nsql
.when($name != null, .where(users.name == $name))
```

**switch/case** - pattern matching in select:

```nsql
switch(users.status) {
  case "active": "Active User"
  case "inactive": "Inactive User"
  default: "Unknown"
}
```

### cast()

Type casting:

```nsql
cast(expr, typename)
```

## Relations (Auto-JOIN)

Relations define table connections and enable automatic JOIN generation via dot-access syntax.

### Defining Relations

```nsql
// Basic relation (INNER JOIN)
relation author for posts returning users {
  users.id == posts.author_id
}

// Optional relation (LEFT JOIN, values may be null)
optional relation profile for users returning user_profiles {
  user_profiles.user_id == users.id
}

// Public relation (accessible from other modules)
public relation category for products returning categories {
  categories.id == products.category_id
}

// Public optional relation
public optional relation metadata for posts returning post_metadata {
  post_metadata.post_id == posts.id
}
```

### Using Relations (auto-JOIN via dot access)

Access related columns through the relation name as a property of the source table:

```nsql
query getPostWithAuthor($postId: uuid) {
  from(posts)
  .where(posts.id == $postId)
  .select(posts.title, posts.author.name, posts.author.email)
  // posts.author.name auto-joins users via the "author" relation
}
```

### Nested Relation Access

Relations chain for multi-level navigation:

```nsql
// posts -> author (users) -> organization -> parent_org
.select(posts.author.organization.parent_org.name)
```

### Aggregation Relations

Define computed aggregate values as virtual columns:

```nsql
aggregation post_count for users returning i32 {
  count(posts.id)
}

public aggregation avg_rating for products returning f64 {
  avg(reviews.rating)
}
```

Use like regular columns:

```nsql
.select(users.name, users.post_count)
```

## CLI Usage

### Project Setup

```bash
# Initialize a new NextSQL project (creates next-sql.toml)
nsql init [dir]
```

### Configuration (`next-sql.toml`)

```toml
# Optional: database connection URL for schema-aware validation
database_url = "postgresql://user:pass@localhost:5432/mydb"

[files]
includes = ["**"]  # Glob patterns for .nsql files

[target]
target_language = "rust"        # Currently only "rust" is supported
target_directory = "../generated"  # Output directory for generated code
```

### Code Generation

```bash
# Generate code from .nsql files
nsql gen

# Check .nsql files for errors without generating code
nsql check

# Watch for changes and auto-regenerate
nsql watch

# Parse and validate a single .nsql file
nsql parse <file.nsql>
```

### Migrations

```bash
# Initialize migrations directory
nsql migration init

# Generate a new migration
nsql migration generate <name> [-d "description"]

# List migrations
nsql migration list

# Run migrations (file-based)
nsql migration up [timestamp]
nsql migration down <timestamp>

# Run migrations against database
nsql migration db-up [timestamp] --host localhost --port 5432 --database mydb --username user --password pass
nsql migration db-down <timestamp> --host localhost --port 5432 --database mydb --username user --password pass

# Show database migration status
nsql migration db-status --host localhost --port 5432 --database mydb --username user --password pass
```

## References

See the following for the full grammar and comprehensive examples:

- Grammar and AST: @references/grammar.md
- Example files: @references/examples.md
