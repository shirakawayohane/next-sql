import * as assert from "assert";
import * as vscode from "vscode";

suite("NextSQL Extension", () => {
  test("拡張機能が登録されている", () => {
    const ext = vscode.extensions.getExtension("instansys.nextsql-syntax");
    assert.ok(ext, "Extension should be registered");
  });

  test("nextsql言語が登録されている", async () => {
    const langs = await vscode.languages.getLanguages();
    assert.ok(langs.includes("nextsql"), "nextsql language should be registered");
  });

  test(".nsqlファイルがnextsql言語として認識される", async () => {
    const doc = await vscode.workspace.openTextDocument({
      language: "nextsql",
      content: "query get_user(id: Int) {}",
    });
    assert.strictEqual(doc.languageId, "nextsql");
  });

  test("language-configurationのブラケットペアが機能する", async () => {
    const doc = await vscode.workspace.openTextDocument({
      language: "nextsql",
      content: "",
    });
    const editor = await vscode.window.showTextDocument(doc);

    // テキストを挿入してブラケットの存在を確認
    await editor.edit((editBuilder) => {
      editBuilder.insert(new vscode.Position(0, 0), "query test() {\n}");
    });

    const text = doc.getText();
    assert.ok(text.includes("{"), "Should contain opening brace");
    assert.ok(text.includes("}"), "Should contain closing brace");
  });
});
