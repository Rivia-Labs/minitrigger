// Connection strings for the local Postgres/Redis used by integration tests.
// Override via env vars when pointing at something other than docker-compose.yml.

export const TEST_DATABASE_URL =
	process.env["TEST_DATABASE_URL"] ??
	"postgres://minitrigger:minitrigger@localhost:54329/minitrigger";

export const TEST_REDIS_URL = process.env["TEST_REDIS_URL"] ?? "redis://localhost:63799";
