# NextSQL Language Support for VS Code

This extension provides syntax highlighting and basic language support for NextSQL files.

## Features

- **Syntax Highlighting**: Full syntax highlighting for NextSQL language constructs
- **File Association**: Automatic detection of `.nsql` files
- **Code Folding**: Support for folding code blocks
- **Auto-closing Brackets**: Automatic closing of brackets, quotes, and parentheses
- **Comment Support**: Block and line comment support

## NextSQL Language Features Supported

- **Keywords**: `query`, `mutation`, `from`, `where`, `select`, `insert`, `update`, `delete`, etc.
- **Types**: `i16`, `i32`, `i64`, `f32`, `f64`, `string`, `bool`, `uuid`, `timestamp`, `date`
- **Variables**: Parameters prefixed with `$` (e.g., `$id`, `$name`)
- **Operators**: Comparison (`==`, `!=`, `<`, `>`, `<=`, `>=`), logical (`&&`, `||`, `!`), arithmetic (`+`, `-`, `*`, `/`, `%`, `^`)
- **Comments**: Line comments (`//`) and block comments (`/* */`)
- **Strings**: Double and single quoted strings with escape sequences

## Installation

1. Open VS Code
2. Go to Extensions view (`Ctrl+Shift+X` or `Cmd+Shift+X`)
3. Search for "NextSQL Language Support"
4. Install the extension

## Usage

Simply open any `.nsql` file and enjoy syntax highlighting automatically.

## Example

```nsql
query findUserById($id: uuid) {
  from(users)
  .where(users.id == $id)
  .select(users.*)
}

mutation insertUser($name: string, $email: string) {
  insert(users)
  .value({
    name: $name,
    email: $email,
  })
}
```

## Development

To contribute to this extension:

1. Clone the repository
2. Open in VS Code
3. Press `F5` to launch Extension Development Host
4. Test your changes

## License

This extension is released under the MIT License.