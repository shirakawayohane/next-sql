# Expressions

NextSQL supports a rich expression language for building conditions, computed columns, and dynamic queries.

## Variables

Parameters are prefixed with `$`:

```nsql
$id        // simple variable
$name      // another variable
$values    // can hold arrays
```

## Operators

### Logical

| Operator | SQL Equivalent |
|----------|----------------|
| `&&` | AND |
| `\|\|` | OR |
| `!` | NOT |

```nsql
.where(users.is_active == true && users.age >= 18)
.where(users.role == "admin" || users.role == "moderator")
.where(!users.is_deleted)
```

### Comparison

| Operator | Meaning |
|----------|---------|
| `==` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |

### Arithmetic

| Operator | Meaning |
|----------|---------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |
| `^` | Power |

```nsql
.aggregate(total: SUM(order_items.price * order_items.quantity))
```

## Method Expressions

Called on columns or expressions with dot notation:

| Method | SQL Equivalent |
|--------|----------------|
| `.isNull()` | IS NULL |
| `.isNotNull()` | IS NOT NULL |
| `.like(pattern)` | LIKE |
| `.ilike(pattern)` | ILIKE (case-insensitive) |
| `.between(from, to)` | BETWEEN |
| `.eqAny($arr)` | = ANY(...) |
| `.neAny($arr)` | != ANY(...) |
| `.in($arr)` | IN (...) |
| `.asc()` | ASC (in orderBy) |
| `.desc()` | DESC (in orderBy) |

```nsql
.where(
  users.deleted_at.isNull()
  && users.name.like("John%")
  && users.age.between(18, 65)
  && users.status.eqAny($allowedStatuses)
)
```

### String Methods

```nsql
users.name.toLowerCase()
users.name.trim()
users.bio.substring(0, 100)
users.bio.substring(0, 100).toLowerCase().includes($term)
```

### Array / Length Methods

```nsql
.when($tags.length > 0, where(posts.tags.contains($tags[0])))
.when($statuses.length > 0, where(users.status.in($statuses)))
```

## Aggregate Functions

Used within `.aggregate()` or `.having()`:

| Function | Description |
|----------|-------------|
| `COUNT(expr)` | Count of rows |
| `SUM(expr)` | Sum of values |
| `AVG(expr)` | Average |
| `MIN(expr)` | Minimum |
| `MAX(expr)` | Maximum |

```nsql
.groupBy(orders.customer_id)
.having(COUNT(orders.id) > 5)
.aggregate(
  order_count: COUNT(orders.id),
  total: SUM(orders.amount)
)
```

### Aggregate Filter

Append `.filter(condition)` to an aggregate function to apply a conditional filter before aggregating. Only rows matching the condition are included in the computation.

```nsql
.aggregate(
  active_count: COUNT(users.id).filter(users.is_active == true),
  paid_total: SUM(orders.amount).filter(orders.status == "paid")
)
```

## Subqueries

Subqueries are wrapped in `$()`:

```nsql
.where(users.id.in($(
  from(active_users)
  .select(active_users.id)
)))
```

### EXISTS

```nsql
.where(exists($(
  from(orders)
  .where(orders.user_id == users.id)
  .select(orders.id)
)))
```

## Conditional Expressions

### when (Dynamic Clauses)

Apply clauses conditionally based on parameter values:

```nsql
query findUsers($name: string?, $age: i32?, $sortBy: string?) {
  from(users)
  .when($name != null, where(users.name == $name))
  .when($age != null, where(users.age >= $age))
  .when($sortBy == "name", orderBy(users.name.asc()))
  .when($sortBy == "age", orderBy(users.age.asc()))
  .select(users.*)
}
```

### when in SELECT

Conditionally include fields:

```nsql
query getUser($id: uuid, $includeEmail: bool, $includePhone: bool) {
  from(users)
  .where(users.id == $id)
  .select(
    users.id,
    users.name,
    when($includeEmail, users.email),
    when($includePhone, users.phone)
  )
}
```

## Type Casting

```nsql
cast(expr, typename)
```

## Built-in Functions

| Function | Description |
|----------|-------------|
| `now()` | Current timestamp |
| `upper(expr)` | Uppercase string |
| `lower(expr)` | Lowercase string |
| `split(expr, delimiter)` | Split string into array |
| `exists(subquery)` | EXISTS check |
| `excluded(column)` | Reference excluded row in UPSERT |
