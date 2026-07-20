import { expect, test, type APIRequestContext } from "@playwright/test";

const serviceBaseUrl = "http://localhost:3001";
const tokenUrl = "http://localhost:8080/realms/notifications/protocol/openid-connect/token";

test("demo Recipient receives a live Notification and clears the unread badge", async ({ page, request }) => {
  await page.goto("/");

  await page.getByRole("link", { name: "Open Keycloak" }).click();
  await page.locator("#username").fill("recipient-demo");
  await page.locator("#password").fill("password");
  await page.locator("#kc-login").click();

  await expect(page.getByRole("heading", { name: "Notification Center" })).toBeVisible();
  await expect(page.locator(".status-pill", { hasText: /^live$/ })).toBeVisible({ timeout: 30_000 });

  const unreadBadge = page.getByLabel(/unread Notifications/);
  const markAllRead = page.getByRole("button", { name: "Mark all read" });

  if (await markAllRead.isEnabled()) {
    await markAllRead.click();
  }
  await expect(unreadBadge).toHaveText("0");

  const event = demoEvent();
  const response = await request.post(`${serviceBaseUrl}/events`, {
    data: event,
    headers: {
      authorization: `Bearer ${await producerAccessToken(request)}`,
    },
  });

  expect(response.status()).toBe(202);
  await expect(page.getByRole("heading", { name: event.title })).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole("status")).toContainText(event.title);
  await expect(unreadBadge).toHaveText("1");

  const notification = page.locator("article").filter({ hasText: event.title });

  await notification.getByRole("button", { name: "Mark read" }).click();
  await expect(notification.getByRole("button", { name: "Read" })).toBeDisabled();
  await expect(unreadBadge).toHaveText("0");
});

function demoEvent() {
  const stamp = Date.now();

  return {
    body: "Your demo order is on the way.",
    id: `evt_t8_demo_order_${stamp}`,
    occurredAt: new Date(stamp).toISOString(),
    payload: { orderId: `demo-order-${stamp}` },
    recipients: ["recipient-demo"],
    title: `E2E order shipped ${stamp}`,
    type: "order.shipped",
  };
}

async function producerAccessToken(request: APIRequestContext) {
  const response = await request.post(tokenUrl, {
    form: {
      client_id: "notification-producer",
      client_secret: "producer-secret",
      grant_type: "client_credentials",
    },
  });
  const body = await response.json() as { access_token?: unknown };

  expect(response.status()).toBe(200);
  expect(typeof body.access_token).toBe("string");

  return body.access_token as string;
}
