"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.activate = activate;
exports.deactivate = deactivate;
const path = require("path");
const vscode = require("vscode");
const node_1 = require("vscode-languageclient/node");
let client;
function activate(context) {
    console.log("NextSQL extension is being activated");
    console.log("Workspace folders:", vscode.workspace.workspaceFolders?.map((f) => f.uri.fsPath));
    // LSPサーバーのパスを取得
    const serverCommand = getServerPath();
    console.log("LSP server path:", serverCommand);
    // ファイル存在確認
    const fs = require("fs");
    console.log("LSP server exists:", fs.existsSync(serverCommand));
    // サーバーオプション
    const serverOptions = {
        run: { command: serverCommand, transport: node_1.TransportKind.stdio },
        debug: { command: serverCommand, transport: node_1.TransportKind.stdio },
    };
    // クライアントオプション
    const clientOptions = {
        documentSelector: [
            { scheme: "file", language: "nextsql" },
            { scheme: "file", language: "nextsql-toml" },
            { scheme: "file", pattern: "**/next-sql.toml" },
        ],
        synchronize: {
            fileEvents: vscode.workspace.createFileSystemWatcher("**/{*.nsql,next-sql.toml}"),
        },
    };
    console.log("Client options:", JSON.stringify(clientOptions, null, 2));
    // Language Clientを作成
    client = new node_1.LanguageClient("nextsqlLanguageServer", "NextSQL Language Server", serverOptions, clientOptions);
    // クライアントを開始
    client.start();
    console.log("Language client started");
    // 補完が呼ばれているかを確認するため、VSCodeの補完イベントを監視（デバッグ用）
    const completionDisposable = vscode.languages.registerCompletionItemProvider({ scheme: "file", language: "nextsql" }, {
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
    }, "." // トリガー文字
    );
    // 拡張機能が非アクティブ化されるときにクライアントを停止
    context.subscriptions.push(completionDisposable);
    context.subscriptions.push({
        dispose: () => {
            if (client) {
                client.stop();
            }
        },
    });
}
function deactivate() {
    if (!client) {
        return undefined;
    }
    return client.stop();
}
function getServerPath() {
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
//# sourceMappingURL=extension.js.map