import { spawn } from "node:child_process";

export function runPnpmScript(args, env) {
  const child = spawn(pnpmCommand(args), pnpmArgs(args), {
    env,
    shell: process.platform === "win32",
    stdio: "inherit",
  });

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
      child.kill(signal);
    });
  }

  child.on("exit", (code, signal) => {
    if (signal) {
      process.exit(0);
    }

    process.exit(code ?? 0);
  });
}

function pnpmCommand(command) {
  return process.platform === "win32" ? `pnpm ${command.join(" ")}` : "pnpm";
}

function pnpmArgs(args) {
  return process.platform === "win32" ? [] : args;
}
