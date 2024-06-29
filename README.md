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

## Roadmap
- parsing mutation
  - insert
  - update
  - delete
- migration
  - create
  - comment
  - drop
  - alter
    - index
    - constraint

- fragment
- with subquery
- type check
- configuration
  - locale
    - postgres
    - ...and more
  - lang
    - rust
    - ...and more
  - connection
- generation
  - run
  - watch
- linter
- formatter
- lsp
- extension
  - vscode
    - syntax highlight
    - language server
