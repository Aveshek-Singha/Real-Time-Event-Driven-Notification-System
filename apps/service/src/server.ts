import Fastify from "fastify";

export function buildService() {
  const app = Fastify({ logger: true });

  app.get("/", async () => ({
    service: "notification-service",
    status: "ok",
  }));

  app.get("/health", async () => ({
    status: "ok",
  }));

  return app;
}
