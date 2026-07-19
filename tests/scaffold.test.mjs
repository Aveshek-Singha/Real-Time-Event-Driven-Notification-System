import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, it } from "node:test";

const root = process.cwd();

function readJson(path) {
  return JSON.parse(readFileSync(join(root, path), "utf8"));
}

describe("T1 monorepo scaffold", () => {
  it("exposes the expected root developer commands", () => {
    const pkg = readJson("package.json");

    assert.equal(pkg.private, true);
    assert.match(pkg.packageManager, /^pnpm@/);
    assert.deepEqual(
      Object.keys(pkg.scripts).filter((script) =>
        ["build", "lint", "typecheck", "test"].includes(script),
      ),
      ["build", "lint", "typecheck", "test"],
    );
  });

  it("declares the service, web app, and contracts workspace packages", () => {
    const workspace = readFileSync(join(root, "pnpm-workspace.yaml"), "utf8");

    assert.match(workspace, /apps\/\*/);
    assert.match(workspace, /packages\/\*/);

    for (const path of [
      "apps/service/package.json",
      "apps/web/package.json",
      "packages/contracts/package.json",
    ]) {
      assert.equal(existsSync(join(root, path)), true, `${path} should exist`);
    }
  });
});
