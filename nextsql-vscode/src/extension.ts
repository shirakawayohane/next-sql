import * as path from "path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";

let client: LanguageClient;

const SCHEMA_SCHEME = "nextsql-schema";

class NextSqlSchemaProvider implements vscode.TextDocumentContentProvider {
  private _onDidChange = new vscode.EventEmitter<vscode.Uri>();
  readonly onDidChange = this._onDidChange.event;

  refresh(uri: vscode.Uri): void {
    this._onDidChange.fire(uri);
  }

  async provideTextDocumentContent(
    _uri: vscode.Uri,
    _token: vscode.CancellationToken
  ): Promise<string> {
    if (!client || !client.isRunning()) {
      return "// Language server is not running\n";
    }

    // Get the active .nsql file path to identify the project
    const activeEditor = vscode.window.activeTextEditor;
    const filePath = activeEditor?.document.uri.fsPath ?? "";

    try {
      const result = await client.sendRequest<{ content: string }>(
        "nextsql/getSchemaDocument",
        { filePath }
      );
      return result.content;
    } catch (e) {
      return `// Failed to load schema: ${e}\n`;
    }
  }
}

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
      { scheme: "file", pattern: "**/next-sql.toml" },
    ],
    synchronize: {
      fileEvents: vscode.workspace.createFileSystemWatcher(
        "**/{*.nsql,next-sql.toml}"
      ),
    },
    middleware: {
      provideDefinition: async (document, position, token, next) => {
        const result = await next(document, position, token);
        if (!result) return result;

        // nextsql-schema URIの場合、仮想ドキュメントを開いてlanguage設定する
        const locations = Array.isArray(result) ? result : [result];
        for (const loc of locations) {
          const uri = "targetUri" in loc ? loc.targetUri : ("uri" in loc ? loc.uri : undefined);
          if (uri && uri.scheme === SCHEMA_SCHEME) {
            const doc = await vscode.workspace.openTextDocument(uri);
            await vscode.languages.setTextDocumentLanguage(doc, "nextsql");
          }
        }
        return result;
      },
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

  // 仮想スキーマドキュメントプロバイダーの登録
  const schemaProvider = new NextSqlSchemaProvider();
  const providerDisposable = vscode.workspace.registerTextDocumentContentProvider(
    SCHEMA_SCHEME,
    schemaProvider
  );
  context.subscriptions.push(providerDisposable);

  // スキーマキャッシュ無効化時にプロバイダーを更新
  const schemaUri = vscode.Uri.parse(`${SCHEMA_SCHEME}:///schema.nsql`);
  const configWatcher = vscode.workspace.createFileSystemWatcher("**/next-sql.toml");
  configWatcher.onDidChange(() => schemaProvider.refresh(schemaUri));
  context.subscriptions.push(configWatcher);

  // スキーマビューアーを開くコマンド
  const openSchemaCommand = vscode.commands.registerCommand(
    "nextsql.openSchema",
    async () => {
      const doc = await vscode.workspace.openTextDocument(schemaUri);
      await vscode.window.showTextDocument(doc, { preview: true });
      await vscode.languages.setTextDocumentLanguage(doc, "nextsql");
    }
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
  context.subscriptions.push(openSchemaCommand);
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
