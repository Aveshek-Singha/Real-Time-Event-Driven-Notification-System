import {
  inboxPageSchema,
  markAllReadResponseSchema,
  markReadResponseSchema,
  notificationSchema,
  unreadCountSchema,
  type InboxPageResponse,
  type MarkAllReadResponse,
  type MarkReadResponse,
  type Notification,
  type UnreadCountResponse,
} from "@notification-system/contracts";

export type NotificationCenterSession = {
  accessToken: string;
  recipientId: string;
};

export type NotificationCenterState = {
  notifications: Notification[];
  unread: number;
  status: "loading" | "live" | "reconnecting" | "offline";
  toast: Notification | null;
};

export type NotificationCenterAction =
  | { type: "backlog_loaded"; notifications: Notification[]; unread: number }
  | { type: "notification_arrived"; notification: Notification }
  | { type: "notification_marked_read"; notificationId: string }
  | { type: "all_notifications_marked_read" }
  | { type: "connection_status_changed"; status: NotificationCenterState["status"] }
  | { type: "toast_cleared" };

export type NotificationCenterClient = {
  fetchInbox(): Promise<InboxPageResponse>;
  fetchUnreadCount(): Promise<UnreadCountResponse>;
  markRead(notificationId: string): Promise<MarkReadResponse>;
  markAllRead(): Promise<MarkAllReadResponse>;
};

type StorageLike = {
  getItem(key: string): string | null;
  removeItem?(key: string): void;
  setItem(key: string, value: string): void;
};

type ClientOptions = NotificationCenterSession & {
  apiBaseUrl: string;
  fetcher?: typeof fetch;
};

type LoginUrlOptions = {
  authorizationUrl: string;
  clientId: string;
  redirectUri: string;
};

type LiveConnectionUrlOptions = {
  apiBaseUrl: string;
  accessToken: string;
};

const sessionTokenKey = "notification-center:access-token";
const sessionRecipientKey = "notification-center:recipient-id";

const initialState: NotificationCenterState = {
  notifications: [],
  unread: 0,
  status: "loading",
  toast: null,
};

export function resolveSessionFromUrl(url: string, storage?: StorageLike): NotificationCenterSession | null {
  const parsedUrl = new URL(url);
  const hash = new URLSearchParams(parsedUrl.hash.replace(/^#/, ""));
  const search = parsedUrl.searchParams;
  const accessToken = hash.get("access_token") ?? search.get("access_token") ?? storage?.getItem(sessionTokenKey);

  if (!accessToken) {
    return null;
  }

  const recipientId = hash.get("recipient_id") ??
    search.get("recipient_id") ??
    recipientIdFromAccessToken(accessToken) ??
    storage?.getItem(sessionRecipientKey);

  if (!recipientId) {
    return null;
  }

  storage?.setItem(sessionTokenKey, accessToken);
  storage?.setItem(sessionRecipientKey, recipientId);

  return { accessToken, recipientId };
}

export function clearStoredSession(storage: Pick<StorageLike, "setItem">) {
  if ("removeItem" in storage && typeof storage.removeItem === "function") {
    storage.removeItem(sessionTokenKey);
    storage.removeItem(sessionRecipientKey);
    return;
  }

  storage.setItem(sessionTokenKey, "");
  storage.setItem(sessionRecipientKey, "");
}

export function buildLoginUrl(options: LoginUrlOptions) {
  const url = new URL(options.authorizationUrl);

  url.searchParams.set("client_id", options.clientId);
  url.searchParams.set("redirect_uri", options.redirectUri);
  url.searchParams.set("response_type", "token");
  url.searchParams.set("scope", "openid profile");

  return url;
}

export function buildLiveConnectionUrls(options: LiveConnectionUrlOptions) {
  const sse = serviceUrl(options.apiBaseUrl, "/connections/sse");
  sse.searchParams.set("access_token", options.accessToken);

  const websocket = serviceUrl(options.apiBaseUrl, "/connections/ws");
  websocket.protocol = websocket.protocol === "https:" ? "wss:" : "ws:";
  websocket.searchParams.set("access_token", options.accessToken);

  return {
    websocket: websocket.toString(),
    sse: sse.toString(),
  };
}

export function createNotificationCenterClient(options: ClientOptions): NotificationCenterClient {
  const fetcher = options.fetcher ?? fetch;

  async function request<T>(path: string, schema: { parse(value: unknown): T }, init: RequestInit = {}) {
    const response = await fetcher(serviceUrl(options.apiBaseUrl, path), {
      ...init,
      headers: {
        ...init.headers,
        authorization: `Bearer ${options.accessToken}`,
      },
    });

    if (!response.ok) {
      throw new Error(`Notification service request failed with ${response.status}`);
    }

    return schema.parse(await response.json());
  }

  const recipientPath = `/recipients/${encodeURIComponent(options.recipientId)}/inbox`;

  return {
    async fetchInbox() {
      return await request(recipientPath, inboxPageSchema);
    },
    async fetchUnreadCount() {
      return await request(`${recipientPath}/unread-count`, unreadCountSchema);
    },
    async markRead(notificationId: string) {
      return await request(
        `${recipientPath}/${encodeURIComponent(notificationId)}/read`,
        markReadResponseSchema,
        { method: "POST" },
      );
    },
    async markAllRead() {
      return await request(`${recipientPath}/read-all`, markAllReadResponseSchema, { method: "POST" });
    },
  };
}

export function reduceNotificationCenter(
  state: NotificationCenterState = initialState,
  action: NotificationCenterAction,
): NotificationCenterState {
  if (action.type === "backlog_loaded") {
    return {
      ...state,
      notifications: orderNewestFirst(dedupeNotifications(action.notifications)),
      unread: action.unread,
      status: "live",
    };
  }

  if (action.type === "notification_arrived") {
    if (state.notifications.some((notification) => notification.id === action.notification.id)) {
      return state;
    }

    return {
      ...state,
      notifications: orderNewestFirst([action.notification, ...state.notifications]),
      unread: state.unread + (action.notification.read ? 0 : 1),
      toast: action.notification,
    };
  }

  if (action.type === "notification_marked_read") {
    let unreadDelta = 0;
    const notifications = state.notifications.map((notification) => {
      if (notification.id !== action.notificationId || notification.read) {
        return notification;
      }

      unreadDelta = -1;
      return { ...notification, read: true };
    });

    return {
      ...state,
      notifications,
      unread: Math.max(0, state.unread + unreadDelta),
    };
  }

  if (action.type === "all_notifications_marked_read") {
    return {
      ...state,
      notifications: state.notifications.map((notification) => ({ ...notification, read: true })),
      unread: 0,
    };
  }

  if (action.type === "connection_status_changed") {
    return { ...state, status: action.status };
  }

  if (action.type === "toast_cleared") {
    return { ...state, toast: null };
  }

  return state;
}

export function parseLiveNotification(data: string) {
  return notificationSchema.parse(JSON.parse(data));
}

function serviceUrl(apiBaseUrl: string, path: string) {
  return new URL(path, apiBaseUrl.replace(/\/$/, "") + "/");
}

function dedupeNotifications(notifications: Notification[]) {
  const seen = new Set<string>();
  const deduped: Notification[] = [];

  for (const notification of notifications) {
    if (!seen.has(notification.id)) {
      seen.add(notification.id);
      deduped.push(notification);
    }
  }

  return deduped;
}

function orderNewestFirst(notifications: Notification[]) {
  return [...notifications].sort((left, right) => right.createdAt.localeCompare(left.createdAt));
}

function recipientIdFromAccessToken(accessToken: string) {
  try {
    const [, payload] = accessToken.split(".");

    if (!payload) {
      return undefined;
    }

    const claims = JSON.parse(decodeBase64Url(payload)) as {
      preferred_username?: unknown;
      recipient_id?: unknown;
      sub?: unknown;
    };
    const recipientId = claims.recipient_id ?? claims.preferred_username ?? claims.sub;

    return typeof recipientId === "string" && recipientId.length > 0 ? recipientId : undefined;
  } catch {
    return undefined;
  }
}

function decodeBase64Url(value: string) {
  const normalized = value.replaceAll("-", "+").replaceAll("_", "/");
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");

  if (typeof atob === "function") {
    return atob(padded);
  }

  return Buffer.from(padded, "base64").toString("utf8");
}
