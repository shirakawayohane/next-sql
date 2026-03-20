<div align="center">
  <img src="https://github.com/shirakawayohane/next-sql/assets/7351910/4e6e7e61-8dc8-4e5f-b9c1-97b28de5a1e5" width="200" />
</div>

NextSQLは、型安全性とモダンな視点を持つ洗練された構文を提供するSQL互換言語で、様々な環境でのアプリケーション構築のために設計されています。

## 目標
1. **安全でモダンな構文**: 型チェックによってランタイムエラーを排除し、安全でモダンな構文を持つSQL互換言語を提供
2. **可読性の向上**: SQLクエリの可読性と保守性を向上
3. **複雑なクエリのサポート**: 結合、サブクエリなどを含む複雑なクエリを簡単に作成でき、可能な限りSQLに匹敵する表現力を提供
4. **多言語サポート**: 様々なプログラミング言語とデータベースにnsqlを移植し、開発者が異なる環境でnsqlを活用できるようにし、nsqlの知識をどこでも適用可能にする
5. **IDE自動補完のための設計**: IDEでの自動補完を促進する設計を優先

# コントリビューション
まだ初期段階ですが、コントリビューションを歓迎しています。
nsqlを様々な言語とデータベースに移植することを目標としているため、特に異なる言語用のドライバーの作成を歓迎します。

IssuesやPRsを自由にご利用ください。現在のところ、特定のルールはありません。

## 開発

### 前提条件
- Rust（最新安定版）
- Node.js（VSCode拡張機能開発用）
- VSCode（拡張機能テスト用）

### プロジェクト構造
```
next-sql/
├── nextsql-core/         # コアパーサーとASTライブラリ
├── nextsql-cli/          # コマンドラインインターフェース
├── nextsql-lsp/          # Language Server Protocol実装
├── nextsql-vscode/       # VSCode拡張機能
├── examples/             # NextSQLサンプルファイル
└── migrations/           # データベースマイグレーションファイル
```

### ビルド

#### CLIとCore
```bash
# 全コンポーネントのビルド
cargo build

# テスト実行
cargo test

# 特定コンポーネントのビルド
cargo build -p nextsql-cli
cargo build -p nextsql-lsp
```

#### VSCode拡張機能開発

1. **Language Serverのビルド:**
   ```bash
   cargo build -p nextsql-lsp
   ```

2. **拡張機能の依存関係をインストール:**
   ```bash
   cd nextsql-vscode
   npm install
   ```

3. **拡張機能のコンパイル:**
   ```bash
   npm run compile
   ```

4. **拡張機能のテスト:**
   - VSCodeを開く
   - `nextsql-vscode`フォルダを開く
   - `F5`キーを押してExtension Development Hostを起動
   - 新しいウィンドウで`.nsql`ファイルを開くか作成
   - シンタックスハイライト、エラー検出、自動補完をテスト

### VSCode拡張機能の機能
- **シンタックスハイライト**: NextSQLキーワード、型、演算子の色分け
- **エラー検出**: リアルタイムでの構文エラーハイライト
- **自動補完**: `.`を入力時の文脈認識メソッド提案
- **Language Server**: 強化された開発体験のための完全なLSP統合

### CLI使用法
```bash
# 新しいNextSQLプロジェクトの初期化
cargo run -- init [--dir <ディレクトリ>]

# NextSQLファイルの実行
cargo run -- <file.nsql>

# マイグレーションコマンド
cargo run -- migration init
cargo run -- migration generate <name> --description "<説明>"
cargo run -- migration list
cargo run -- migration up
cargo run -- migration down <timestamp>

# データベース操作
cargo run -- migration db-up
cargo run -- migration db-down <timestamp>
cargo run -- migration db-status
```

### プロジェクト設定

NextSQLプロジェクトは`next-sql.toml`設定ファイルを使用してプロジェクト設定を定義します：

```toml
# パース対象に含めるファイル（globパターンをサポート）
includes = ["src/**"]

# コード生成のターゲット言語
target_language = "rust"

# 生成されたコードの出力ディレクトリ
target_directory = "../generated"
```

`nsql init`コマンドは、デフォルト設定で新しいNextSQLプロジェクトを作成します：

```bash
# 現在のディレクトリで初期化
cargo run -- init

# 特定のディレクトリで初期化
cargo run -- init --dir my-project
```

完全なサンプルプロジェクトは`examples/sample-project`ディレクトリで確認できます。

### NextSQL構文例
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
  .returning(users.*)
}
```

## 機能

### 型システム
- **プリミティブ型**: `i16`, `i32`, `i64`, `f32`, `f64`, `string`, `bool`, `uuid`, `timestamp`, `date`
- **ユーティリティ型**: `Insertable<T>`
- **オプション型**: `T?`
- **配列型**: `[T]`
- **ユーザー定義型**: カスタム型定義

### クエリ機能
- **SELECT文**: FROM、WHERE、JOIN操作、サブクエリ
- **ミューテーション**: INSERT、UPDATE、DELETE（RETURNING句対応）
- **式**: 二項演算、関数呼び出し、リテラル、変数、インデックスアクセス
- **変数**: `$`プレフィックス（例: `$id`, `$name`）
- **エイリアス**: `<alias>`構文を使用したテーブルエイリアス（例: `users<u>`）

### データベース統合
- PostgreSQL接続とトランザクション管理
- `nextsql_migrations`追跡テーブルの自動作成
- エラー時のロールバック機能付き安全なマイグレーション実行
- タイムスタンプとエラーログ付きマイグレーション状況追跡