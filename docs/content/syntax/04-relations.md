# Relations & Aggregations

Relations and aggregations are NextSQL's most powerful feature. They let you declare table relationships once and use them everywhere via dot notation, with automatic JOIN generation.

## Defining Relations

A relation declares a connection between two tables:

```nsql
relation author for posts returning users {
  users.id == posts.author_id
}
```

This means: "for the `posts` table, there is a relation called `author` that returns rows from `users` where the condition holds."

### Optional Relations

When the foreign key may be NULL, use `optional`:

```nsql
optional relation featured_image for posts returning images {
  images.id == posts.featured_image_id
}
```

Optional relations generate LEFT JOINs and nullable return types.

### Public Relations

Add `public` to make a relation accessible from other modules:

```nsql
public relation category for products returning categories {
  categories.id == products.category_id
}

public optional relation metadata for posts returning post_metadata {
  post_metadata.post_id == posts.id
}
```

### One-to-Many Relations

The direction is determined by the condition:

```nsql
// Many posts belong to one author
relation author for posts returning users {
  users.id == posts.author_id
}

// One user has many posts
relation posts for users returning posts {
  posts.author_id == users.id
}
```

## Using Relations

Once defined, access related columns with dot notation:

```nsql
query getPostWithAuthor($postId: uuid) {
  from(posts)
  .where(posts.id == $postId)
  .select(
    posts.title,
    posts.author.name,
    posts.author.email
  )
}
```

NextSQL automatically generates the appropriate JOIN based on the relation definition.

### Nested Relations

Relations chain for multi-level navigation:

```nsql
// posts -> author (users) -> organization -> parent_org
query getPostHierarchy($postId: uuid) {
  from(posts)
  .where(posts.id == $postId)
  .select(
    posts.title,
    author_name: posts.author.name,
    org_name: posts.author.organization.name,
    parent_org: posts.author.organization.parent_org.name
  )
}
```

## Aggregation Relations

Aggregations define computed values as virtual columns on a table:

```nsql
aggregation post_count for users returning i32 {
  count(posts.id)
}

aggregation avg_rating for products returning f64 {
  avg(reviews.rating)
}

public aggregation engagement_score for posts returning f64 {
  (count(comments.id) * 2) + count(post_likes.id)
}
```

### Supported Functions

| Function | Description |
|----------|-------------|
| `count(expr)` | Count of rows |
| `sum(expr)` | Sum of values |
| `avg(expr)` | Average of values |
| `min(expr)` | Minimum value |
| `max(expr)` | Maximum value |

### Using Aggregations

Aggregations are accessed like regular columns:

```nsql
query getActiveUsers() {
  from(users)
  .where(users.post_count > 0)
  .select(users.name, users.email, users.post_count)
  .orderBy(users.post_count.desc())
}
```

## The `types.nsql` Convention

Projects should centralize shared definitions in a `types.nsql` file:

```nsql
// types.nsql
valtype UserId = uuid for users.id
valtype ProductId = uuid for products.id

relation author for posts returning users {
  users.id == posts.author_id
}

optional relation profile for users returning user_profiles {
  user_profiles.user_id == users.id
}

aggregation post_count for users returning i32 {
  count(posts.id)
}
```

This keeps query files focused on their domain logic.
