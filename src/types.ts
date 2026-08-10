export const logLevels = ["debug", "info", "warn", "error"] as const;
export type LogLevel = (typeof logLevels)[number];
export type AttributeValue = string | number | boolean;
export type LogAttributes = Record<string, AttributeValue>;
