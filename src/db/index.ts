import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import postgres from "postgres";
import path from "node:path";
import { config } from "../config.js";
import * as schema from "./schema.js";

export const client = postgres(config.databaseUrl, {
  max: config.databasePoolSize,
  idle_timeout: 20,
  connect_timeout: 10,
});

export const db = drizzle(client, { schema });

export async function runMigrations() {
  await migrate(db, { migrationsFolder: path.resolve(process.cwd(), "drizzle") });
  await client`SELECT 1`;
}

export async function closeDatabase() {
  await client.end({ timeout: 5 });
}
