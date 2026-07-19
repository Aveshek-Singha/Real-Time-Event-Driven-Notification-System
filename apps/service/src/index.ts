import { pathToFileURL } from "node:url";
import { buildService } from "./server.js";

const isEntrypoint = import.meta.url === pathToFileURL(process.argv[1] ?? "").href;

if (isEntrypoint) {
  const service = buildService();
  const port = Number(process.env.PORT ?? 3001);
  const host = process.env.HOST ?? "0.0.0.0";

  await service.listen({ port, host });
}
