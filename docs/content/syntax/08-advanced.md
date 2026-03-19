# Advanced Features

## Dynamic Queries

NextSQL's `.when()` clause enables fully dynamic query construction at runtime.

### Conditional WHERE Clauses

```nsql
query searchUsers($name: string?, $email: string?, $minAge: i32?) {
  from(users)
  .when($name != null, where(users.name == $name))
  .when($email != null, where(users.email == $email))
  .when($minAge != null, where(users.age >= $minAge))
  .select(users.*)
}
```

Each `.when()` only applies its clause if the condition is true. This generates efficient SQL — omitted conditions are not included in the query at all.

### Conditional ORDER BY

```nsql
query listUsers($sortBy: string?) {
  from(users)
  .when($sortBy == "name", orderBy(users.name.asc()))
  .when($sortBy == "created", orderBy(users.created_at.desc()))
  .when($sortBy == "age", orderBy(users.age.asc()))
  .select(users.*)
}
```

### Dynamic Array Conditions

```nsql
query filterUsers($statuses: [string], $departments: [string]?) {
  from(users)
  .when($statuses.length > 0, where(users.status.in($statuses)))
  .when(
    $departments != null && $departments.length > 0,
    where(users.department.in($departments))
  )
  .select(users.*)
}
```

### Dynamic Field Selection

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

## Cursor-based Pagination

Combine `.when()` with ordering for efficient cursor pagination:

```nsql
query listItems($cursor: i64?, $limit: i64) {
  from(items)
  .where(items.is_active == true)
  .when($cursor.isNotNull(), where(items.sequence_id > $cursor))
  .select(items.*)
  .orderBy(items.sequence_id.asc())
  .limit($limit)
}
```

## Soft Delete Pattern

Use UPDATE instead of DELETE to preserve data:

```nsql
mutation deactivateUser($id: uuid) {
  update(users)
  .where(users.id == $id)
  .set({ is_active: false })
}

query findActiveUsers() {
  from(users)
  .where(users.is_active == true)
  .select(users.*)
}
```

## Complex Subquery Patterns

### EXISTS with Correlated Subquery

```nsql
query findUsersWithOrders() {
  from(users)
  .where(exists($(
    from(orders)
    .where(orders.user_id == users.id && orders.status != "cancelled")
    .select(orders.id)
  )))
  .select(users.*)
}
```

### IN with Subquery

```nsql
query findActiveUserEmails() {
  from(users)
  .where(users.id.in($(
    from(active_sessions)
    .select(active_sessions.user_id)
  )))
  .select(users.email)
}
```

## UNION Patterns

### Combine Different Sources

```nsql
query findAllContacts() {
  from(employees)
  .select(employees.name, employees.email)
  .union(
    from(contractors)
    .select(contractors.name, contractors.email)
  )
  .unionAll(
    from(freelancers)
    .select(freelancers.name, freelancers.email)
  )
}
```

## FOR UPDATE

Lock selected rows within a transaction:

```nsql
query lockUser($id: uuid) {
  from(users)
  .where(users.id == $id)
  .select(users.*)
  .forUpdate()
}
```
