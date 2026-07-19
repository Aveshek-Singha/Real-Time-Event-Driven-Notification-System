import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { buildService } from "./server.js";

const service = buildService();

after(async () => {
  await service.close();
});

describe("service hello world", () => {
  it("reports health at the HTTP edge", async () => {
    const response = await service.inject({ method: "GET", url: "/health" });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), { status: "ok" });
  });

  it("does not expose an Event ingest surface in the T1 scaffold", async () => {
    const response = await service.inject({
      method: "POST",
      url: "/events/validate-placeholder",
      payload: { id: "evt_1", type: "demo.event" },
    });

    assert.equal(response.statusCode, 404);
  });
});
