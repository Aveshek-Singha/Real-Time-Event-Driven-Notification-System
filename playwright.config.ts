import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  expect: {
    timeout: 15_000,
  },
  fullyParallel: false,
  projects: [{
    name: "chromium",
    use: devices["Desktop Chrome"],
  }],
  reporter: [["list"]],
  testDir: "./tests/e2e",
  timeout: 120_000,
  use: {
    baseURL: "http://localhost:3002",
    trace: "retain-on-failure",
  },
  webServer: [{
    command: "node scripts/e2e-service.mjs",
    reuseExistingServer: false,
    timeout: 120_000,
    url: "http://localhost:3001/health",
  }, {
    command: "node scripts/e2e-web.mjs",
    reuseExistingServer: false,
    timeout: 120_000,
    url: "http://localhost:3002",
  }],
  workers: 1,
});
