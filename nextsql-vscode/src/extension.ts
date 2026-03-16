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
  const serverCommand = getServerPath(context);
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
        if (client.isRunning()) {
          await client.stop();
        }
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

function getServerPath(context: vscode.ExtensionContext): string {
  const fs = require("fs");

  // 設定からパスを取得
  const configPath = vscode.workspace.getConfiguration("nextsql").get<string>("serverPath");
  if (configPath && fs.existsSync(configPath)) {
    console.log("Using configured server path:", configPath);
    return configPath;
  }

  // 拡張機能のインストールパスからバイナリを探す
  const extensionBinPath = path.join(context.extensionPath, "bin", "nextsql-lsp");
  if (fs.existsSync(extensionBinPath)) {
    console.log("Using extension bundled server:", extensionBinPath);
    return extensionBinPath;
  }

  // 開発時: 拡張機能のパスから上にCargo.tomlを探す
  let current = context.extensionPath;
  while (current !== path.dirname(current)) {
    if (fs.existsSync(path.join(current, "Cargo.toml"))) {
      const debugPath = path.join(current, "target", "debug", "nextsql-lsp");
      if (fs.existsSync(debugPath)) {
        console.log("Using debug build:", debugPath);
        return debugPath;
      }
      break;
    }
    current = path.dirname(current);
  }

  // PATHから探す
  console.log("Falling back to nextsql-lsp from PATH");
  return "nextsql-lsp";
}
