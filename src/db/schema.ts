import { jsonb, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import type { LogAttributes, LogLevel } from "../types.js";

export const logs = pgTable("logs", {
  id: uuid("id").defaultRandom().primaryKey(),
  tenantId: text("tenant_id").notNull().default("default"),
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow().notNull(),
  level: text("level").$type<LogLevel>().notNull(),
  service: text("service").notNull(),
  message: text("message").notNull(),
  attributes: jsonb("attributes").$type<LogAttributes>().notNull().default({}),
});

export type NewLog = typeof logs.$inferInsert;
export type Log = typeof logs.$inferSelect;
