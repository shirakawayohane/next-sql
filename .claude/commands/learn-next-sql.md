---
description: Learn NextSQL DSL syntax to generate correct .nsql code
allowed-tools: Read, Glob, Grep
---

# NextSQL Language Reference

Read the grammar definition and examples to learn the NextSQL DSL before generating .nsql code.

## Core Syntax Files
@nextsql-core/src/nextsql.pest
@nextsql-core/src/ast.rs

## Example Files - Basic
@examples/simple-select.nsql
@examples/simple-delete.nsql
@examples/update.nsql
@examples/insert.nsql
@examples/insert-many.nsql
@examples/insert-insertable.nsql
@examples/insert-many-with-variable.nsql

## Example Files - Joins & Relations
@examples/simple-select-and-join.nsql
@examples/relation-basic.nsql
@examples/relation-complex.nsql
@examples/relation-nested.nsql
@examples/relation-optional.nsql
@examples/relation-with-aggregation.nsql

## Example Files - Dynamic/Conditional
@examples/dynamic-joins.nsql
@examples/dynamic-conditional-clauses.nsql
@examples/dynamic-field-selection.nsql
@examples/dynamic-array-conditions.nsql
@examples/dynamic-switch-case.nsql
@examples/method-chaining.nsql

## Example Files - Advanced Features
@examples/distinct-and-pagination.nsql
@examples/order-by-multiple.nsql
@examples/group-by-having.nsql
@examples/upsert.nsql
@examples/union.nsql
@examples/method-expressions.nsql
@examples/mogok-like-queries.nsql
@examples/delete-with-subquery.nsql
@examples/sample-project/migrations/20250704180205_create_users_table/with-and-aggregation.nsql

## Quick Syntax Summary

### Query Declaration
```nsql
query queryName($param: type, $param2: type?) {
  from(table)
  .where(condition)
  .select(columns)
}
```

### Mutation Declaration
```nsql
mutation mutationName($param: type) {
  insert(table).value({...}).returning(table.*)
  update(table).where(...).set({...}).returning(table.*)
  delete(table).where(...).returning(table.*)
}
```

### Types
- Built-in: i16, i32, i64, f32, f64, string, bool, uuid, timestamp, timestamptz, date
- Optional: `type?` (e.g., `string?`)
- Array: `[type]` (e.g., `[uuid]`)
- Utility: `Insertable<TableName>`
- User-defined: any identifier (including valtype names)

### Value Types (valtype)
Define named type aliases for specific columns or standalone types. Use these in a shared `types.nsql` file for project-wide type definitions.
```nsql
// With column binding - ties the type to a specific table column
valtype UserId = uuid for users.id
valtype Email = string for users.email

// Without column binding - standalone named type
valtype Amount = f64
```
Valtypes can then be used as parameter types in queries and mutations:
```nsql
query findUserById($id: UserId) {
  from(users)
  .where(users.id == $id)
  .select(users.*)
}
```

### The `types.nsql` Convention
Projects should place shared type definitions (valtypes, relations, aggregations) in a `types.nsql` file. This keeps type definitions centralized and reusable across multiple query/mutation files.

### Variables
- Prefixed with `$`: `$id`, `$name`, `$values`

### Operators
- Logical: `&&` (AND), `||` (OR)
- Equality: `==`, `!=`
- Comparison: `<`, `<=`, `>`, `>=`
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Unary: `!` (NOT)

### Method Expressions
- `col.isNull()` - IS NULL
- `col.isNotNull()` - IS NOT NULL
- `col.like(pattern)` - LIKE pattern matching
- `col.ilike(pattern)` - Case-insensitive LIKE
- `col.between(from, to)` - BETWEEN range
- `col.eqAny($arr)` - IN array (eq_any)
- `col.in([val1, val2])` - IN values list
- `col.asc()` - ORDER BY ASC (use inside .orderBy())
- `col.desc()` - ORDER BY DESC (use inside .orderBy())

### Special Expressions
- `SUM(expr)`, `COUNT(expr)`, `AVG(expr)`, `MIN(expr)`, `MAX(expr)` - Aggregates
- `$(from(...).select(...))` - Subquery
- `exists($(subquery))` - EXISTS check
- `when(condition, expr)` - Conditional expression
- `switch(expr) { case val: result default: fallback }` - Switch/case

### Query Clauses (chained with `.`)
- `from(table)` - FROM (required), supports `.innerJoin()`, `.leftJoin()`, `.rightJoin()`, `.fullOuterJoin()`, `.crossJoin()`
- `.where(condition)` - WHERE filter
- `.distinct()` - SELECT DISTINCT
- `.select(expr, ...)` - SELECT columns (supports `alias: expr`)
- `.groupBy(expr, ...)` - GROUP BY
- `.having(condition)` - HAVING (after GROUP BY)
- `.aggregate(alias: expr, ...)` - Named aggregations
- `.orderBy(col.asc(), col.desc())` - ORDER BY with direction
- `.limit(n)` - LIMIT
- `.offset(n)` - OFFSET
- `.when(condition, clause)` - Conditional clause application
- `.union(from(...).select(...))` - UNION
- `.unionAll(from(...).select(...))` - UNION ALL

### Mutation-specific Clauses
- `insert(table).value({col: expr})` - Single INSERT
- `insert(table).values($records)` - Multi INSERT
- `.onConflict(col1, col2).doUpdate({col: expr})` - UPSERT
- `.onConflict(col1).doNothing()` - INSERT OR IGNORE
- `update(table).set({col: expr})` - UPDATE
- `delete(table)` - DELETE
- `.returning(table.*, col1, col2)` - RETURNING

### Relations (Auto-JOIN)
Relations define how tables are connected and enable automatic JOIN generation through property-access syntax. Instead of writing explicit JOINs, you define the relationship once and access related columns using dot notation.

#### Defining Relations
```nsql
// Basic relation: navigates from one table to another
relation author for posts returning users {
  users.id == posts.author_id
}

// Optional relation: the related record may not exist (generates LEFT JOIN, values may be null)
optional relation featured_image for posts returning images {
  images.id == posts.featured_image_id
}

// Public relation: accessible from other modules
public relation pubRel for parent returning child {
  parent.id == child.parent_id
}

// Public optional relation: both modifiers can be combined
public optional relation metadata for posts returning post_metadata {
  post_metadata.post_id == posts.id
}
```

#### Using Relations in Queries (auto-JOIN via dot access)
Once defined, access related columns through the relation name as a property:
```nsql
query getPostWithAuthor($postId: uuid) {
  from(posts)
  .where(posts.id == $postId)
  .select(posts.title, posts.author.name, posts.author.email)
  // posts.author.name auto-joins users table via the "author" relation
}
```

#### Nested Relation Access
Relations can be chained for multi-level navigation:
```nsql
// posts -> author (users) -> organization -> parent_org
query getPostsByParentOrg($parentOrgId: uuid) {
  from(posts)
  .where(posts.author.organization.parent_org.id == $parentOrgId)
  .select(
    posts.title,
    posts.author.name,
    posts.author.organization.name,
    posts.author.organization.parent_org.name
  )
}
```

#### Aggregation Relations
Define computed aggregate values that appear as virtual columns on a table:
```nsql
aggregation post_count for users returning i32 {
  count(posts.id)
}

aggregation comment_count for posts returning i32 {
  count(comments.id)
}

aggregation avg_rating for posts returning f64 {
  avg(post_ratings.rating)
}

// Public aggregation
public aggregation engagement_score for posts returning f64 {
  (count(comments.id) * 2) + count(post_likes.id)
}
```
Use aggregation relations like regular columns:
```nsql
query getUserStats($userId: uuid) {
  from(users)
  .where(users.id == $userId)
  .select(users.name, users.post_count)
}
```

### WITH (CTE)
```nsql
with cteName = {
  from(table).where(...).select(...)
}
```

### JOINs (Explicit)
For cases where you need explicit JOIN control instead of relation-based auto-JOINs:
```nsql
from(table1
  .innerJoin(table2, table1.id == table2.table1_id)
  .leftJoin(table3, table2.id == table3.table2_id))
```

Always refer to nextsql.pest as the authoritative grammar source.
