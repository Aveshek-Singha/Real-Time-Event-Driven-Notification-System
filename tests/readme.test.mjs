import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, it } from "node:test";

describe("T1 README", () => {
  it("documents local bring-up and validation commands", () => {
    const readme = readFileSync(join(process.cwd(), "README.md"), "utf8");

    for (const expected of [
      "pnpm install",
      "pnpm build",
      "pnpm lint",
      "pnpm typecheck",
      "pnpm test",
      "pnpm test:integration",
      "docker compose up -d --wait",
      "docker compose down -v",
    ]) {
      assert.match(readme, new RegExp(expected.replaceAll(" ", "\\s+")));
    }
  });
});
