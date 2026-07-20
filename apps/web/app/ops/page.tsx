import React from "react";

type OpsLink = {
  description: string;
  href: string;
  label: string;
};

const defaultGrafanaDashboardUrl = "http://localhost:3000/d/notification-system/notification-system";
const defaultKafkaUiUrl = "http://localhost:8081";

export function opsLinks(): OpsLink[] {
  return [{
    description: "Service metrics, delivery rate, consumer lag, DLQ depth, and live Connection count.",
    href: process.env.NEXT_PUBLIC_GRAFANA_DASHBOARD_URL ?? defaultGrafanaDashboardUrl,
    label: "Grafana dashboard",
  }, {
    description: "Kafka topics, notification messages, and parked Events on the DLQ.",
    href: process.env.NEXT_PUBLIC_KAFKA_UI_URL ?? defaultKafkaUiUrl,
    label: "Kafka-UI",
  }];
}

export default function OpsPage() {
  return (
    <main className="shell ops-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Operations</p>
          <h1>System health</h1>
        </div>
        <a className="secondary-link ops-home-link" href="/">Notification Center</a>
      </header>

      <section className="ops-link-list" aria-label="Operations links">
        {opsLinks().map((link) => (
          <a className="ops-link" href={link.href} key={link.label}>
            <span>
              <strong>{link.label}</strong>
              <span>{link.description}</span>
            </span>
            <span aria-hidden="true">Open</span>
          </a>
        ))}
      </section>
    </main>
  );
}
