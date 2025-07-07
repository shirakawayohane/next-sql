<div align="center">
  <img src="https://github.com/shirakawayohane/next-sql/assets/7351910/4e6e7e61-8dc8-4e5f-b9c1-97b28de5a1e5" width="200" />
</div>
An altSQL offers a more sophisticated syntax with type safety and a modern perspective, designed for building applications across various environments.

## Goals
1. **Secure and Modern Syntax**: Provide a SQL-compatible language with a secure and modern syntax, including type checking to eliminate runtime errors.
2. **Enhance Readability**: Improve the readability and maintainability of SQL queries.
3. **Support Complex Queries**: Enable the creation of complex queries with ease, including joins, subqueries, and more, with expressiveness comparable to SQL, as much as possible.
4. **Multi-language Support**: Ensure that nsql can be ported to various programming languages and databases, allowing developers to utilize nsql in different environments, to make the knowledge of nsql applicable everywhere.
5. **Design for IDE Autocompletion**: Prioritize design that facilitates autocompletion in IDEs.

# Contributing
We are still in the early stages, but we welcome contributions.
We have a goal to port nsql to various languages and databases, so we especially welcome the creation of drivers for different languages.

Feel free to use Issues or PRs. There are no specific rules for now.
Please refer to the following **Roadmap** when contributing.

## Development

### Prerequisites
- Rust (latest stable)
- Node.js (for VSCode extension development)
- VSCode (for testing the extension)

### Project Structure
```
next-sql/
├── nextsql-core/         # Core parser and AST library
├── nextsql-cli/          # Command-line interface
├── nextsql-lsp/          # Language Server Protocol implementation
├── nextsql-vscode/       # VSCode extension
├── examples/             # Example NextSQL files
└── migrations/           # Database migration files
```

### Building

#### CLI and Core
```bash
# Build all components
cargo build

# Run tests
cargo test

# Build specific component
cargo build -p nextsql-cli
cargo build -p nextsql-lsp
```

#### VSCode Extension Development

1. **Build the Language Server:**
   ```bash
   cargo build -p nextsql-lsp
   ```

2. **Install extension dependencies:**
   ```bash
   cd nextsql-vscode
   npm install
   ```

3. **Compile the extension:**
   ```bash
   npm run compile
   ```

4. **Test the extension:**
   - Open VSCode
   - Open the `nextsql-vscode` folder
   - Press `F5` to launch Extension Development Host
   - In the new window, open or create a `.nsql` file
   - Test syntax highlighting, error detection, and autocompletion

### VSCode Extension Features
- **Syntax Highlighting**: Color coding for NextSQL keywords, types, and operators
- **Error Detection**: Real-time syntax error highlighting
- **Autocompletion**: Context-aware method suggestions when typing `.`
- **Language Server**: Full LSP integration for enhanced development experience

### CLI Usage
```bash
# Initialize a new NextSQL project
cargo run -- init [--dir <directory>]

# Run a NextSQL file
cargo run -- <file.nsql>

# Migration commands
cargo run -- migration init
cargo run -- migration generate <name> --description "<description>"
cargo run -- migration list
cargo run -- migration up
cargo run -- migration down <timestamp>

# Database operations
cargo run -- migration db-up
cargo run -- migration db-down <timestamp>
cargo run -- migration db-status
```

### Project Configuration

NextSQL projects use a `next-sql.toml` configuration file to define project settings:

```toml
# Files to include in parsing (supports glob patterns)
includes = ["src/**"]

# Target language for code generation
target_language = "rust"

# Output directory for generated code
target_directory = "../generated"
```

The `nextsql init` command creates a new NextSQL project with default configuration:

```bash
# Initialize in current directory
cargo run -- init

# Initialize in specific directory
cargo run -- init --dir my-project
```

You can see a complete example project in the `examples/sample-project` directory.

### Example NextSQL Syntax
```nsql
query findUserById($id: uuid) {
  from(users)
  .where(users.id == $id)
  .select(users.*)
}

query findUserWithPosts($id: uuid) {
  alias u = users
  alias p = posts
  from(users.innerJoin(posts, u.id == p.user_id))
  .where(u.id == $id)
  .select(u.name, p.title, p.content)
}

mutation insertUser($name: string, $email: string) {
  insert(users)
  .value({
    name: $name,
    email: $email,
  })
  .returning(users.*)
}
```

### Key Syntax Features

#### Table Aliases
NextSQL uses explicit `alias` statements for better IDE autocompletion support:
```nsql
query example() {
  alias u = users
  alias p = posts
  from(users.innerJoin(posts, u.id == p.user_id))
  .select(u.name, p.title)
}
```

#### Join Operations
Join operations are part of the from clause:
- `from(table1.innerJoin(table2, condition))`
- `from(table1.leftJoin(table2, condition))`
- `from(table1.rightJoin(table2, condition))`
- `from(table1.fullOuterJoin(table2, condition))`
- `from(table1.crossJoin(table2, condition))`

Multiple joins can be chained:
```nsql
from(users.innerJoin(posts, u.id == p.user_id).leftJoin(comments, p.id == c.post_id))
```

#### Type System
NextSQL includes a comprehensive type system:
- Built-in types: `i16`, `i32`, `i64`, `f32`, `f64`, `string`, `bool`, `uuid`, `timestamp`, `date`
- Optional types: `T?` (e.g., `string?`)
- Array types: `[T]` (e.g., `[string]`)
- Utility types: `Insertable<T>` for insert operations
