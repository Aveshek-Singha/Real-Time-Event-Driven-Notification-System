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

  it("rejects malformed Events with validation details", async () => {
    const response = await service.inject({
      method: "POST",
      url: "/events",
      payload: { id: "evt_1", type: "demo.event", recipients: [] },
    });

    assert.equal(response.statusCode, 400);
    assert.equal(response.json().error, "invalid_event");
    assert.match(JSON.stringify(response.json().details), /recipients/);
  });

  it("rejects Events addressed to the same Recipient more than once", async () => {
    const response = await service.inject({
      method: "POST",
      url: "/events",
      payload: {
        id: "evt_duplicate_recipient",
        type: "demo.event",
        recipients: ["recipient-a", "recipient-a"],
        title: "Duplicate recipient",
        body: "This Event addresses one Recipient twice.",
        payload: { example: true },
        occurredAt: "2026-07-19T10:00:00.000Z",
      },
    });

    assert.equal(response.statusCode, 400);
    assert.equal(response.json().error, "invalid_event");
    assert.match(JSON.stringify(response.json().details), /unique/);
  });
});
