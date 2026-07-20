"use client";

import { useCallback, useEffect, useMemo, useReducer, useState, type FormEvent } from "react";
import type { Notification } from "@notification-system/contracts";
import {
  buildLiveConnectionUrls,
  buildLoginUrl,
  clearStoredSession,
  createNotificationCenterClient,
  parseLiveNotification,
  reduceNotificationCenter,
  resolveSessionFromUrl,
  type NotificationCenterSession,
} from "./notification-center";

type TransportMode = "auto" | "ws" | "sse";

type AppConfig = {
  apiBaseUrl: string;
  authorizationUrl: string;
  clientId: string;
  tokenUrl: string;
};

type BroadcastPayload =
  | { type: "notification"; notification: Notification }
  | { type: "refresh" };

const broadcastChannelName = "notification-center";

const config: AppConfig = {
  apiBaseUrl: process.env.NEXT_PUBLIC_NOTIFICATION_API_BASE_URL ?? "http://localhost:3001",
  authorizationUrl: process.env.NEXT_PUBLIC_KEYCLOAK_AUTHORIZATION_URL ??
    "http://localhost:8080/realms/notifications/protocol/openid-connect/auth",
  clientId: process.env.NEXT_PUBLIC_KEYCLOAK_CLIENT_ID ?? "notification-web",
  tokenUrl: process.env.NEXT_PUBLIC_KEYCLOAK_TOKEN_URL ??
    "http://localhost:8080/realms/notifications/protocol/openid-connect/token",
};

export default function NotificationCenterApp() {
  const [session, setSession] = useState<NotificationCenterSession | null>(null);
  const [sessionReady, setSessionReady] = useState(false);

  useEffect(() => {
    const resolved = resolveSessionFromUrl(window.location.href, window.localStorage);

    if (resolved) {
      setSession(resolved);
    }

    if (window.location.hash.includes("access_token")) {
      window.history.replaceState({}, "", window.location.pathname);
    }

    setSessionReady(true);
  }, []);

  if (!sessionReady) {
    return <main className="shell" aria-busy="true" />;
  }

  if (!session) {
    return <SignIn config={config} onSession={setSession} />;
  }

  return (
    <NotificationCenter
      config={config}
      session={session}
      onSignOut={() => {
        clearStoredSession(window.localStorage);
        setSession(null);
      }}
    />
  );
}

function SignIn({
  config,
  onSession,
}: {
  config: AppConfig;
  onSession(session: NotificationCenterSession): void;
}) {
  const [username, setUsername] = useState("recipient-demo");
  const [password, setPassword] = useState("password");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const loginUrl = buildLoginUrl({
    authorizationUrl: config.authorizationUrl,
    clientId: config.clientId,
    redirectUri: typeof window === "undefined" ? "http://localhost:3002/" : window.location.origin + "/",
  });

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);

    try {
      const response = await fetch(config.tokenUrl, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          grant_type: "password",
          client_id: config.clientId,
          username,
          password,
        }),
      });
      const payload = await response.json() as { access_token?: unknown; error_description?: unknown };

      if (!response.ok || typeof payload.access_token !== "string") {
        throw new Error(typeof payload.error_description === "string" ? payload.error_description : "Sign-in failed");
      }

      const session = resolveSessionFromUrl(
        `${window.location.origin}/#access_token=${encodeURIComponent(payload.access_token)}`,
        window.localStorage,
      );

      if (!session) {
        throw new Error("Token did not include a Recipient identity");
      }

      onSession(session);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Sign-in failed");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="shell auth-shell">
      <section className="auth-panel" aria-labelledby="sign-in-title">
        <p className="eyebrow">Notification Center</p>
        <h1 id="sign-in-title">Sign in</h1>
        <form onSubmit={submit} className="auth-form">
          <label>
            <span>Recipient</span>
            <input value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" />
          </label>
          <label>
            <span>Password</span>
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              type="password"
              autoComplete="current-password"
            />
          </label>
          {error ? <p className="form-error" role="alert">{error}</p> : null}
          <button type="submit" className="primary-action" disabled={submitting}>
            {submitting ? "Signing in" : "Sign in with Keycloak"}
          </button>
        </form>
        <a className="secondary-link" href={loginUrl.toString()}>Open Keycloak</a>
      </section>
    </main>
  );
}

function NotificationCenter({
  config,
  onSignOut,
  session,
}: {
  config: AppConfig;
  onSignOut(): void;
  session: NotificationCenterSession;
}) {
  const [state, dispatch] = useReducer(
    reduceNotificationCenter,
    undefined,
    () => reduceNotificationCenter(undefined, { type: "connection_status_changed", status: "loading" }),
  );
  const [transportMode, setTransportMode] = useState<TransportMode>("auto");
  const client = useMemo(() => createNotificationCenterClient({
    apiBaseUrl: config.apiBaseUrl,
    accessToken: session.accessToken,
    recipientId: session.recipientId,
  }), [config.apiBaseUrl, session.accessToken, session.recipientId]);

  const broadcast = useCallback((payload: BroadcastPayload) => {
    if ("BroadcastChannel" in window) {
      const channel = new BroadcastChannel(broadcastChannelName);
      channel.postMessage(payload);
      channel.close();
    }
  }, []);

  const loadBacklog = useCallback(async () => {
    const [inbox, count] = await Promise.all([
      client.fetchInbox(),
      client.fetchUnreadCount(),
    ]);

    dispatch({
      type: "backlog_loaded",
      notifications: inbox.notifications,
      unread: count.unread,
    });
  }, [client]);

  useEffect(() => {
    loadBacklog().catch(() => dispatch({ type: "connection_status_changed", status: "offline" }));
  }, [loadBacklog]);

  useEffect(() => {
    if (!("BroadcastChannel" in window)) {
      return;
    }

    const channel = new BroadcastChannel(broadcastChannelName);
    channel.onmessage = (event: MessageEvent<BroadcastPayload>) => {
      if (event.data.type === "notification") {
        dispatch({ type: "notification_arrived", notification: event.data.notification });
      } else {
        loadBacklog().catch(() => dispatch({ type: "connection_status_changed", status: "offline" }));
      }
    };

    return () => channel.close();
  }, [loadBacklog]);

  useEffect(() => {
    const urls = buildLiveConnectionUrls({
      apiBaseUrl: config.apiBaseUrl,
      accessToken: session.accessToken,
    });
    let closed = false;
    let eventSource: EventSource | null = null;
    let reconnectTimer: number | undefined;
    let socket: WebSocket | null = null;

    dispatch({ type: "connection_status_changed", status: "reconnecting" });

    function scheduleReconnect() {
      if (closed) {
        return;
      }

      dispatch({ type: "connection_status_changed", status: "reconnecting" });
      reconnectTimer = window.setTimeout(() => {
        connect();
      }, 750);
    }

    function connect() {
      if (closed) {
        return;
      }

      if (transportMode === "sse") {
        openSse();
      } else {
        openWebSocket();
      }
    }

    function receive(notification: Notification) {
      dispatch({ type: "notification_arrived", notification });
      broadcast({ type: "notification", notification });
    }

    function openWebSocket() {
      let opened = false;
      socket = new WebSocket(urls.websocket);
      socket.onopen = () => {
        opened = true;
        dispatch({ type: "connection_status_changed", status: "live" });
        loadBacklog().catch(() => dispatch({ type: "connection_status_changed", status: "offline" }));
      };
      socket.onmessage = (event) => receive(parseLiveNotification(event.data.toString()));
      socket.onerror = () => {
        socket?.close();
      };
      socket.onclose = () => {
        if (transportMode === "auto" && !opened) {
          openSse();
        } else {
          scheduleReconnect();
        }
      };
    }

    function openSse() {
      eventSource = new EventSource(urls.sse);
      eventSource.onopen = () => {
        dispatch({ type: "connection_status_changed", status: "live" });
        loadBacklog().catch(() => dispatch({ type: "connection_status_changed", status: "offline" }));
      };
      eventSource.addEventListener("notification", (event) => receive(parseLiveNotification(event.data)));
      eventSource.onerror = () => {
        eventSource?.close();
        scheduleReconnect();
      };
    }

    connect();

    return () => {
      closed = true;
      if (reconnectTimer !== undefined) {
        window.clearTimeout(reconnectTimer);
      }
      eventSource?.close();
      socket?.close();
    };
  }, [broadcast, config.apiBaseUrl, loadBacklog, session.accessToken, transportMode]);

  async function markRead(notificationId: string) {
    await client.markRead(notificationId);
    dispatch({ type: "notification_marked_read", notificationId });
    broadcast({ type: "refresh" });
  }

  async function markAllRead() {
    await client.markAllRead();
    dispatch({ type: "all_notifications_marked_read" });
    broadcast({ type: "refresh" });
  }

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Signed in as {session.recipientId}</p>
          <h1>Notification Center</h1>
        </div>
        <div className="header-actions">
          <a className="secondary-link header-link" href="/ops">Ops</a>
          <span className="unread-badge" aria-label={`${state.unread} unread Notifications`}>
            {state.unread}
          </span>
          <button type="button" className="ghost-action" onClick={onSignOut}>Sign out</button>
        </div>
      </header>

      <section className="control-row" aria-label="Notification controls">
        <div className="segmented" aria-label="Transport">
          {(["auto", "ws", "sse"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              aria-pressed={transportMode === mode}
              onClick={() => setTransportMode(mode)}
            >
              {mode.toUpperCase()}
            </button>
          ))}
        </div>
        <span className={`status-pill status-${state.status}`}>{state.status}</span>
        <button type="button" className="secondary-action" onClick={markAllRead} disabled={state.unread === 0}>
          Mark all read
        </button>
      </section>

      {state.toast ? (
        <aside className="toast" role="status">
          <strong>{state.toast.title}</strong>
          <button type="button" onClick={() => dispatch({ type: "toast_cleared" })}>Dismiss</button>
        </aside>
      ) : null}

      <section className="notification-list" aria-label="Inbox">
        {state.notifications.length === 0 ? (
          <p className="empty-state">No Notifications</p>
        ) : state.notifications.map((notification) => (
          <article className={notification.read ? "notification read" : "notification unread"} key={notification.id}>
            <div>
              <p className="notification-type">{notification.type}</p>
              <h2>{notification.title}</h2>
              <p>{notification.body}</p>
              <time dateTime={notification.createdAt}>{formatTimestamp(notification.createdAt)}</time>
            </div>
            <button
              type="button"
              className="secondary-action"
              onClick={() => markRead(notification.id)}
              disabled={notification.read}
            >
              {notification.read ? "Read" : "Mark read"}
            </button>
          </article>
        ))}
      </section>
    </main>
  );
}

function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}
