import { config } from "../config.js";
import type { NewLog } from "../db/schema.js";

export function notifyErrorThreshold(entries: NewLog[], tenantId: string) {
  if (!config.alertsEnabled || !config.alertWebhookUrl) return;
  const errors = entries.filter((entry) => entry.level === "error");
  if (errors.length < config.alertErrorThreshold) return;
  void fetch(config.alertWebhookUrl, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ tenant_id: tenantId, error_count: errors.length, observed_at: new Date().toISOString() }),
  }).catch((error) => console.error("Alert webhook failed:", error));
}
