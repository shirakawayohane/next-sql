# Mutations

Mutations are declared with the `mutation` keyword and support INSERT, UPDATE, and DELETE operations.

## INSERT

### Single Row

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

### Optional Fields

Optional parameters map to nullable columns:

```nsql
mutation insertUser($name: string, $email: string, $profile: string?) {
  insert(users)
  .value({
    name: $name,
    email: $email,
    profile: $profile,
  })
}
```

### Using Insertable Type

`Insertable<TableName>` generates a typed struct with all required and optional fields for the table:

```nsql
mutation insertUser($new: Insertable<users>) {
  insert(users)
  .values($new)
  .returning(*)
}
```

### Multi-row Insert

Insert multiple rows with an array literal or a variable:

```nsql
// Array literal
mutation insertManyUsers() {
  insert(users)
  .values([{ name: "John" }, { name: "Jane" }])
  .returning(users.id, users.name)
}

// Variable
mutation insertManyUsers($records: [Insertable<users>]) {
  insert(users)
  .values($records)
  .returning(users.*)
}
```

## UPDATE

### Basic Update

```nsql
mutation updateOrderStatus($id: uuid, $status: string) {
  update(orders)
  .where(orders.id == $id)
  .set({ status: $status })
  .returning(orders.*)
}
```

### Using ChangeSet

`ChangeSet<TableName>` generates a struct with all fields as optional for partial updates:

```nsql
mutation updateUser($id: uuid, $data: ChangeSet<users>) {
  update(users)
  .where(users.id == $id)
  .set($data)
  .returning(users.*)
}
```

### Update with Expressions

You can use expressions and function calls in `.set()`:

```nsql
mutation shipOrder($id: uuid) {
  update(orders)
  .where(orders.id == $id && orders.status == "pending")
  .set({ status: "shipped", shipped_at: now() })
  .returning(orders.*)
}
```

## DELETE

### Basic Delete

```nsql
mutation deleteOrderItem($id: uuid) {
  delete(order_items)
  .where(order_items.id == $id)
}
```

### Delete with RETURNING

```nsql
mutation deleteUser($id: uuid) {
  delete(users)
  .where(users.id == $id)
  .returning(users.*)
}
```

### Delete with Subquery

```nsql
mutation deleteInactiveUsers() {
  delete(users)
  .where(exists($(
    from(user_action_logs)
    .where(
      user_action_logs.user_id == users.id
      && user_action_logs.deleted == true
    )
    .select(*)
  )))
}
```

## UPSERT (ON CONFLICT)

### doUpdate

Specify conflict columns and the update set:

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

### doNothing

Ignore conflicting rows:

```nsql
mutation insertIgnore($name: string, $email: string) {
  insert(users)
  .value({ name: $name, email: $email })
  .onConflict(email)
  .doNothing()
}
```

## RETURNING

All mutation types support `.returning()`:

```nsql
.returning(users.*)            // All columns
.returning(users.id, users.name) // Specific columns
.returning(*)                  // Shorthand for all
```
