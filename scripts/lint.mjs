import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const roots = process.argv.slice(2);
const startPoints = roots.length > 0 ? roots : ["."];
const ignoredDirectories = new Set([
  ".git",
  ".next",
  "coverage",
  "dist",
  "node_modules",
]);
const checkedExtensions = new Set([
  ".cjs",
  ".js",
  ".json",
  ".md",
  ".mjs",
  ".ts",
  ".tsx",
  ".yaml",
  ".yml",
]);

function hasCheckedExtension(path) {
  return [...checkedExtensions].some((extension) => path.endsWith(extension));
}

function walk(path, files = []) {
  const stat = statSync(path);

  if (stat.isDirectory()) {
    for (const entry of readdirSync(path)) {
      if (!ignoredDirectories.has(entry)) {
        walk(join(path, entry), files);
      }
    }
    return files;
  }

  if (hasCheckedExtension(path)) {
    files.push(path);
  }

  return files;
}

const failures = [];

for (const startPoint of startPoints) {
  for (const file of walk(startPoint)) {
    const text = readFileSync(file, "utf8");
    const lines = text.split(/\r?\n/);

    lines.forEach((line, index) => {
      if (/\s+$/.test(line)) {
        failures.push(`${relative(process.cwd(), file)}:${index + 1} trailing whitespace`);
      }
      if (line.includes("\t")) {
        failures.push(`${relative(process.cwd(), file)}:${index + 1} tab character`);
      }
    });

    if (text.length > 0 && !text.endsWith("\n")) {
      failures.push(`${relative(process.cwd(), file)} missing final newline`);
    }
  }
}

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}
