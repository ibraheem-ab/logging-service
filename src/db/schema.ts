import { jsonb, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import type { LogAttributes, LogLevel } from "../types.js";

export const logs = pgTable("logs", {
  // إضافة defaultRandom() لتوليد الـ ID تلقائياً
  id: uuid("id").defaultRandom().primaryKey(), 
  tenantId: text("tenant_id").notNull().default("default"),
  
  // إضافة defaultNow() لتسجيل وقت الإضافة تلقائياً
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow().notNull(), 
  
  level: text("level").$type<LogLevel>().notNull(),
  service: text("service").notNull(),
  message: text("message").notNull(),
  attributes: jsonb("attributes").$type<LogAttributes>().notNull().default({}),
});

// هذا السطر يحل الخطأ مباشرة ويحدد نوع البيانات المطلوبة عند إضافة سجل جديد
export type NewLog = typeof logs.$inferInsert;

// هذا السطر إضافي ومفيد جداً لتعريف شكل البيانات عند قراءتها من القاعدة
export type Log = typeof logs.$inferSelect;
