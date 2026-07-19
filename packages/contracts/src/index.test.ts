import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { scaffoldMessageSchema } from "./index.js";

describe("scaffold message contract", () => {
  it("accepts a minimal shared scaffold value", () => {
    assert.deepEqual(scaffoldMessageSchema.parse({ message: "hello" }), {
      message: "hello",
    });
  });
});
