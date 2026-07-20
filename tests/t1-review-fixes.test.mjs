import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, it } from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

describe("T1 review fixes", () => {
  it("keeps the contracts placeholder out of Event terminology", () => {
    const source = read("packages/contracts/src/index.ts");
    const testSource = read("packages/contracts/src/index.test.ts");

    assert.doesNotMatch(source, /eventPlaceholderSchema|EventPlaceholder/);
    assert.doesNotMatch(testSource, /Event-shaped|event placeholder/i);
  });

  it("keeps the web hello-world out of Event terminology", () => {
    const page = read("apps/web/app/page.tsx");

    assert.doesNotMatch(page, /sampleEvent|demo\.notification|Event/);
  });

  it("configures Prometheus to scrape the service metrics endpoint once metrics exist", () => {
    const prometheus = read("infra/prometheus/prometheus.yml");

    assert.match(prometheus, /job_name:\s+"notification-service"/);
    assert.match(prometheus, /host\.docker\.internal:3001/);
    assert.match(prometheus, /metrics_path:\s+"\/metrics"/);
  });

  it("ignores generated TypeScript build state", () => {
    const gitignore = read(".gitignore");

    assert.match(gitignore, /^\*.tsbuildinfo$/m);
  });
});
