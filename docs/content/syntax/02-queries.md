# Queries

Queries are declared with the `query` keyword. They define SELECT operations using method-chained clauses starting from `from()`.

## Basic Query

```nsql
query findUserById($id: uuid) {
  from(users)
  .where(users.id == $id)
  .select(users.*)
}
```

- **Parameters** are prefixed with `$` and require a type annotation.
- **Optional parameters** use `?` (e.g., `$name: string?`).
- Clauses are chained with `.` — each clause returns the query for further chaining.

## Clauses

### from

The entry point of every query. Specifies the primary table.

```nsql
from(users)
```

### where

Filters rows with a boolean expression.

```nsql
.where(users.age >= 18 && users.is_active == true)
```

### select

Specifies which columns to return. Supports wildcards and aliases.

```nsql
// All columns
.select(users.*)

// Specific columns
.select(users.id, users.name, users.email)

// Aliased columns
.select(
  users.id,
  full_name: users.name,
  mail: users.email
)
```

### distinct

Returns only unique rows.

```nsql
query findDistinctEmails() {
  from(users)
  .distinct()
  .select(users.email)
}
```

### orderBy

Sorts results. Optionally specify direction with `.asc()` or `.desc()`; without a direction, the default is ascending.

```nsql
.orderBy(users.name.asc(), users.created_at.desc())

// Direction is optional — defaults to ascending
.orderBy(users.name)
```

### limit / offset

Pagination support.

```nsql
query findUsersWithPagination($limit: i32, $offset: i32) {
  from(users)
  .select(users.*)
  .orderBy(users.created_at.desc())
  .limit($limit)
  .offset($offset)
}
```

### groupBy / having / aggregate

Group rows and compute aggregates.

```nsql
query salesByCategory() {
  from(order_items
    .innerJoin(products, order_items.product_id == products.id))
  .groupBy(products.category_id)
  .having(COUNT(order_items.id) > 10)
  .aggregate(
    total_revenue: SUM(order_items.unit_price * order_items.quantity),
    total_items: COUNT(order_items.id)
  )
  .select(products.category_id, total_revenue, total_items)
  .orderBy(total_revenue.desc())
}
```

### union / unionAll

Combine results from multiple queries.

```nsql
query findAllContactEmails() {
  from(employees)
  .select(employees.email)
  .unionAll(
    from(contractors)
    .select(contractors.email)
  )
}
```

`union` removes duplicates; `unionAll` keeps them.

### forUpdate

Acquires a row-level lock.

```nsql
.forUpdate()
```

