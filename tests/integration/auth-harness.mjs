import assert from "node:assert/strict";
import { resolve } from "node:path";
import { GenericContainer, Wait } from "testcontainers";

const realm = "notifications";
const audience = "notification-service";
const producerClientId = "notification-producer";
const producerClientSecret = "producer-secret";
const recipientClientId = "notification-web";
const recipientPassword = "password";

export async function startAuthHarness() {
  const existingKeycloak = await tryExistingKeycloak();
  const keycloak = existingKeycloak ?? await startContainerizedKeycloak();
  const previous = captureOidcEnvironment();
  const recipientUsers = new Map();
  const createdUserIds = [];

  process.env.OIDC_AUDIENCE = audience;
  process.env.OIDC_ISSUER = keycloak.issuer;
  process.env.OIDC_PRODUCER_CLIENT_ID = producerClientId;
  process.env.OIDC_RECIPIENT_CLIENT_ID = recipientClientId;
  process.env.OIDC_RECIPIENT_ID_CLAIM = "recipient_id";

  return {
    async producerHeaders() {
      return {
        authorization: `Bearer ${await fetchProducerToken(keycloak.baseUrl)}`,
      };
    },
    async recipientHeaders(recipientId) {
      const username = await ensureRecipientUser(keycloak.baseUrl, recipientId, recipientUsers, createdUserIds);

      return {
        authorization: `Bearer ${await fetchRecipientToken(keycloak.baseUrl, username)}`,
      };
    },
    async stop() {
      await deleteCreatedUsers(keycloak.baseUrl, createdUserIds);
      restoreOidcEnvironment(previous);
      await keycloak.stop();
    },
  };
}

async function tryExistingKeycloak() {
  const baseUrl = "http://127.0.0.1:8080";

  if (!await keycloakIsReady(baseUrl, 1_000)) {
    return undefined;
  }

  return {
    baseUrl,
    issuer: `${baseUrl}/realms/${realm}`,
    async stop() {},
  };
}

async function startContainerizedKeycloak() {
  const container = await new GenericContainer("quay.io/keycloak/keycloak:26.0")
    .withCommand(["start-dev", "--import-realm"])
    .withEnvironment({
      KC_BOOTSTRAP_ADMIN_PASSWORD: "admin",
      KC_BOOTSTRAP_ADMIN_USERNAME: "admin",
      KC_HEALTH_ENABLED: "true",
    })
    .withCopyFilesToContainer([{
      source: resolve("infra/keycloak/realm-export.json"),
      target: "/opt/keycloak/data/import/realm-export.json",
    }])
    .withExposedPorts(8080)
    .withStartupTimeout(480_000)
    .withWaitStrategy(
      Wait.forHttp(`/realms/${realm}/.well-known/openid-configuration`, 8080)
        .forStatusCode(200)
        .withReadTimeout(10_000),
    )
    .start();
  const baseUrl = `http://${container.getHost()}:${container.getMappedPort(8080)}`;

  await assertKeycloakReady(baseUrl);

  return {
    baseUrl,
    issuer: `${baseUrl}/realms/${realm}`,
    async stop() {
      await container.stop();
    },
  };
}

async function keycloakIsReady(baseUrl, timeoutMs) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/realms/${realm}/.well-known/openid-configuration`);

      if (response.ok) {
        return true;
      }
    } catch {
      await delay(100);
    }
  }

  return false;
}

async function assertKeycloakReady(baseUrl) {
  assert.equal(
    await keycloakIsReady(baseUrl, 60_000),
    true,
    `Keycloak did not expose the ${realm} realm at ${baseUrl}`,
  );
}

async function fetchProducerToken(baseUrl) {
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: producerClientId,
    client_secret: producerClientSecret,
  });

  return await fetchAccessToken(`${baseUrl}/realms/${realm}/protocol/openid-connect/token`, body);
}

async function fetchRecipientToken(baseUrl, username) {
  const body = new URLSearchParams({
    grant_type: "password",
    client_id: recipientClientId,
    username,
    password: recipientPassword,
  });

  return await fetchAccessToken(`${baseUrl}/realms/${realm}/protocol/openid-connect/token`, body);
}

async function fetchAccessToken(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  const tokenResponse = await response.json();

  assert.equal(response.status, 200, JSON.stringify(tokenResponse));
  assert.equal(typeof tokenResponse.access_token, "string");

  return tokenResponse.access_token;
}

async function ensureRecipientUser(baseUrl, recipientId, recipientUsers, createdUserIds) {
  const cached = recipientUsers.get(recipientId);

  if (cached) {
    return cached;
  }

  const username = recipientId;
  const email = `recipient-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}@example.com`;
  const adminToken = await fetchAdminToken(baseUrl);
  const response = await fetch(`${baseUrl}/admin/realms/${realm}/users`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${adminToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      username,
      enabled: true,
      email,
      emailVerified: true,
      firstName: "Integration",
      lastName: "Recipient",
      requiredActions: [],
      credentials: [{
        type: "password",
        value: recipientPassword,
        temporary: false,
      }],
      attributes: {
        recipient_id: [recipientId],
      },
    }),
  });

  assert.equal(response.status, 201, await response.text());

  const userId = response.headers.get("location")?.split("/").pop();

  if (userId) {
    createdUserIds.push(userId);
    await setRecipientAttribute(baseUrl, adminToken, userId, username, email, recipientId);
    await resetRecipientPassword(baseUrl, adminToken, userId);
  }

  recipientUsers.set(recipientId, username);

  return username;
}

async function setRecipientAttribute(baseUrl, adminToken, userId, username, email, recipientId) {
  const response = await fetch(`${baseUrl}/admin/realms/${realm}/users/${userId}`, {
    method: "PUT",
    headers: {
      authorization: `Bearer ${adminToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      username,
      enabled: true,
      email,
      emailVerified: true,
      firstName: "Integration",
      lastName: "Recipient",
      requiredActions: [],
      attributes: {
        recipient_id: [recipientId],
      },
    }),
  });

  assert.equal(response.status, 204, await response.text());
}

async function resetRecipientPassword(baseUrl, adminToken, userId) {
  const response = await fetch(`${baseUrl}/admin/realms/${realm}/users/${userId}/reset-password`, {
    method: "PUT",
    headers: {
      authorization: `Bearer ${adminToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      type: "password",
      value: recipientPassword,
      temporary: false,
    }),
  });

  assert.equal(response.status, 204, await response.text());
}

async function fetchAdminToken(baseUrl) {
  const body = new URLSearchParams({
    grant_type: "password",
    client_id: "admin-cli",
    username: "admin",
    password: "admin",
  });

  return await fetchAccessToken(`${baseUrl}/realms/master/protocol/openid-connect/token`, body);
}

async function deleteCreatedUsers(baseUrl, userIds) {
  if (userIds.length === 0) {
    return;
  }

  const adminToken = await fetchAdminToken(baseUrl).catch(() => undefined);

  if (!adminToken) {
    return;
  }

  await Promise.allSettled(userIds.map((userId) =>
    fetch(`${baseUrl}/admin/realms/${realm}/users/${userId}`, {
      method: "DELETE",
      headers: { authorization: `Bearer ${adminToken}` },
    })
  ));
}

function captureOidcEnvironment() {
  return {
    OIDC_AUDIENCE: process.env.OIDC_AUDIENCE,
    OIDC_ISSUER: process.env.OIDC_ISSUER,
    OIDC_JWKS_URI: process.env.OIDC_JWKS_URI,
    OIDC_PRODUCER_CLIENT_ID: process.env.OIDC_PRODUCER_CLIENT_ID,
    OIDC_RECIPIENT_CLIENT_ID: process.env.OIDC_RECIPIENT_CLIENT_ID,
    OIDC_RECIPIENT_ID_CLAIM: process.env.OIDC_RECIPIENT_ID_CLAIM,
  };
}

function restoreOidcEnvironment(previous) {
  for (const [name, value] of Object.entries(previous)) {
    if (value === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = value;
    }
  }
}

async function delay(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
