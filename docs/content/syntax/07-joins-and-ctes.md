# JOINs & CTEs

## Explicit JOINs

JOINs are expressed as method calls on tables inside `from()`.

### Join Types

| Method | SQL |
|--------|-----|
| `.innerJoin(table, condition)` | INNER JOIN |
| `.leftJoin(table, condition)` | LEFT JOIN |
| `.rightJoin(table, condition)` | RIGHT JOIN |
| `.fullOuterJoin(table, condition)` | FULL OUTER JOIN |
| `.crossJoin(table)` | CROSS JOIN |

### Basic Join

```nsql
query findUserPosts($userId: uuid) {
  from(users.innerJoin(posts, users.id == posts.user_id))
  .where(users.id == $userId)
  .select(users.name, posts.title, posts.content)
}
```

### Multiple Joins

Chain multiple joins in the `from()` clause:

```nsql
query getOrderDetails($orderId: uuid) {
  from(orders
    .innerJoin(order_items, orders.id == order_items.order_id)
    .innerJoin(products, order_items.product_id == products.id))
  .where(orders.id == $orderId)
  .select(
    orders.id,
    products.name,
    order_items.quantity,
    order_items.unit_price
  )
}
```

### With Aliases

```nsql
query example() {
  alias u = users
  alias p = posts
  alias c = comments
  from(users
    .innerJoin(posts, u.id == p.user_id)
    .leftJoin(comments, p.id == c.post_id))
  .select(u.name, p.title, c.content)
}
```

### JOIN vs Relations

Use **explicit JOINs** when:
- You need a one-off join that isn't a core relationship
- You need fine-grained control over the join condition
- The join is part of a complex analytical query

Use **relations** when:
- The relationship is reused across multiple queries
- You want cleaner dot-notation access
- You want the join condition defined once, used everywhere

## WITH (Common Table Expressions)

CTEs are defined with `with` before the query body. They allow you to name intermediate result sets.

### Basic CTE

```nsql
with activePosts = {
  from(posts)
  .where(posts.is_active == true)
  .select(posts.*)
}

query getActivePosts() {
  from(activePosts)
  .select(activePosts.title, activePosts.created_at)
  .orderBy(activePosts.created_at.desc())
}
```

### CTE with Aggregation

```nsql
with amountByProducts = {
  from(transaction)
  .groupBy(transaction.product_id)
  .aggregate(
    total_amounts: SUM(transaction.amounts)
  )
  .select(transaction.product_id, total_amounts)
}

query aggregateResult() {
  from(amountByProducts
    .innerJoin(products, amountByProducts.product_id == products.id))
  .select(
    products.name,
    total_sales: products.price * amountByProducts.total_amounts
  )
  .orderBy(total_sales.desc())
}
```

CTEs can be joined with other tables and used just like any regular table reference.
