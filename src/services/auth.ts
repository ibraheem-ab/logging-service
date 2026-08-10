import { createHash } from "node:crypto";
import { config } from "../config.js";
import { client } from "../db/index.js";

export type Principal = { tenantId: string; scopes: string[]; seeded: boolean };

function hash(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

export async function seedLoadGeneratorKey() {
  if (!config.authEnabled || !config.loadgenApiKey) return;
  await client`
    INSERT INTO api_keys (key_hash, tenant_id, scopes)
    VALUES (${hash(config.loadgenApiKey)}, 'loadgen', ARRAY['ingest', 'query'])
    ON CONFLICT (key_hash) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, scopes = EXCLUDED.scopes
  `;
}

export async function authenticate(credential: string | undefined): Promise<Principal | null> {
  if (!credential) return null;
  const rows = await client<{ tenant_id: string; scopes: string[] }[]>`
    SELECT tenant_id, scopes FROM api_keys WHERE key_hash = ${hash(credential)}
  `;
  const row = rows[0];
  return row ? { tenantId: row.tenant_id, scopes: row.scopes, seeded: row.tenant_id === "loadgen" } : null;
}
