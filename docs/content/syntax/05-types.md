# Type System

NextSQL has a comprehensive type system that ensures type safety from query definition through code generation.

## Built-in Types

| Type | Description |
|------|-------------|
| `i16` | 16-bit integer |
| `i32` | 32-bit integer |
| `i64` | 64-bit integer |
| `f32` | 32-bit float |
| `f64` | 64-bit float |
| `string` | Text / varchar |
| `bool` | Boolean |
| `uuid` | UUID |
| `timestamp` | Timestamp without timezone |
| `timestamptz` | Timestamp with timezone |
| `date` | Date |
| `numeric` | Arbitrary precision number |
| `decimal` | Decimal number |
| `json` | JSON value |

## Type Modifiers

### Optional Types

Append `?` to make a type nullable:

```nsql
query findUsers($name: string?, $minAge: i32?) {
  from(users)
  .when($name != null, where(users.name == $name))
  .when($minAge != null, where(users.age >= $minAge))
  .select(users.*)
}
```

### Array Types

Wrap a type in `[]` for arrays:

```nsql
query filterByStatuses($statuses: [string]) {
  from(users)
  .where(users.status.in($statuses))
  .select(users.*)
}
```

## Utility Types

### Insertable\<T\>

Generates a struct with all required and optional fields for inserting into a table. Fields with defaults or nullable columns become optional.

```nsql
mutation createUser($input: Insertable<users>) {
  insert(users)
  .values($input)
  .returning(users.*)
}
```

In the generated Rust code, this produces a struct with a builder pattern.

### ChangeSet\<T\>

Generates a struct for partial updates where every field is optional, wrapped in an `UpdateField<T>` enum:

```nsql
mutation updateUser($id: uuid, $changes: ChangeSet<users>) {
  update(users)
  .where(users.id == $id)
  .set($changes)
  .returning(users.*)
}
```

### Configuring Generated Names

Customize the generated type names in `next-sql.toml`:

```toml
[codegen]
insert_params = "Insert{Table}Params"
update_params = "Update{Table}Params"
```

## Value Types (valtype)

Define named type aliases for semantic clarity:

```nsql
// Bind to a specific column
valtype UserId = uuid for users.id
valtype Email = string for users.email

// Standalone named type
valtype Amount = f64
```

Use them as parameter types:

```nsql
query findUser($id: UserId) {
  from(users)
  .where(users.id == $id)
  .select(users.*)
}
```

Value types generate dedicated newtype wrappers in the target language, providing stronger type guarantees than raw primitives.
