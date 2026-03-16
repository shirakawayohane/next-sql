import * as path from "path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";

let client: LanguageClient;

export function activate(context: vscode.ExtensionContext) {
  console.log("NextSQL extension is being activated");
  console.log(
    "Workspace folders:",
    vscode.workspace.workspaceFolders?.map((f) => f.uri.fsPath)
  );

  // LSPサーバーのパスを取得
  const serverCommand = getServerPath();
  console.log("LSP server path:", serverCommand);

  // ファイル存在確認
  const fs = require("fs");
  console.log("LSP server exists:", fs.existsSync(serverCommand));

  // サーバーオプション
  const serverOptions: ServerOptions = {
    run: { command: serverCommand, transport: TransportKind.stdio },
    debug: { command: serverCommand, transport: TransportKind.stdio },
  };

  // クライアントオプション
  const clientOptions: LanguageClientOptions = {
    documentSelector: [
      { scheme: "file", language: "nextsql" },
      { scheme: "file", language: "nextsql-toml" },
      { scheme: "file", pattern: "**/next-sql.toml" },
    ],
    synchronize: {
      fileEvents: vscode.workspace.createFileSystemWatcher(
        "**/{*.nsql,next-sql.toml}"
      ),
    },
  };

  console.log("Client options:", JSON.stringify(clientOptions, null, 2));

  // Language Clientを作成
  client = new LanguageClient(
    "nextsqlLanguageServer",
    "NextSQL Language Server",
    serverOptions,
    clientOptions
  );

  // クライアントを開始
  client.start();
  console.log("Language client started");

  // 補完が呼ばれているかを確認するため、VSCodeの補完イベントを監視（デバッグ用）
  const completionDisposable = vscode.languages.registerCompletionItemProvider(
    { scheme: "file", language: "nextsql" },
    {
      provideCompletionItems(_document, position, _token, context) {
        console.log("VSCode completion requested at:", {
          line: position.line,
          character: position.character,
          triggerKind: context.triggerKind,
          triggerCharacter: context.triggerCharacter,
        });
        // LSPクライアントに処理を委譲するため、undefinedを返す
        return undefined;
      },
    },
    "." // トリガー文字
  );

  // Restart Server コマンド
  const restartCommand = vscode.commands.registerCommand(
    "nextsql.restartServer",
    async () => {
      if (client) {
        await client.stop();
        await client.start();
        vscode.window.showInformationMessage("NextSQL Language Server restarted.");
      }
    }
  );

  // 拡張機能が非アクティブ化されるときにクライアントを停止
  context.subscriptions.push(completionDisposable);
  context.subscriptions.push(restartCommand);
  context.subscriptions.push({
    dispose: () => {
      if (client) {
        client.stop();
      }
    },
  });
}

export function deactivate(): Thenable<void> | undefined {
  if (!client) {
    return undefined;
  }
  return client.stop();
}

function getServerPath(): string {
  // 開発時とパッケージ時の両方に対応
  const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;

  if (workspaceRoot) {
    // プロジェクトルートを探す（Cargo.tomlがあるディレクトリ）
    let projectRoot = workspaceRoot;

    // 現在のフォルダまたは親フォルダでCargo.tomlを探す
    const fs = require("fs");
    while (projectRoot !== path.dirname(projectRoot)) {
      if (fs.existsSync(path.join(projectRoot, "Cargo.toml"))) {
        break;
      }
      projectRoot = path.dirname(projectRoot);
    }

    // 開発時: プロジェクトルートからLSPバイナリを探す
    const debugPath = path.join(projectRoot, "target", "debug", "nextsql-lsp");

    console.log("Checking LSP server paths:");
    console.log("Debug path:", debugPath);
    console.log("Project root:", projectRoot);

    if (fs.existsSync(debugPath)) {
      console.log("Using debug build");
      return debugPath;
    }

    console.error("LSP server binary not found!");
    return debugPath;
  }

  // パッケージされた拡張機能の場合
  return path.join(__dirname, "..", "bin", "nextsql-lsp");
}
