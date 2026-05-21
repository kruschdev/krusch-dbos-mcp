import { Effect, Layer, Redacted } from "effect";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";

let migrationsRan = false;
const initializedTestNames = new Set<string>();
let globalTruncated = false;

export const makeTestPgPersistenceLive = Effect.fn("makeTestPgPersistenceLive")(function* (
  databaseUrl: string,
) {
  const parsed = new URL(
    databaseUrl.includes("5435")
      ? databaseUrl
      : "postgres://kruschdb:password@localhost:5435/kruschdb_test",
  );
  if (!parsed.pathname.endsWith("_test")) {
    parsed.pathname = parsed.pathname + "_test";
  }
  const finalUrl = parsed.toString();

  const pgLayer = PgClient.layer({
    url: Redacted.make(finalUrl),
    maxConnections: 3,
    spanAttributes: { "service.name": "kd-server-test" },
  });

  const setup = Layer.effectDiscard(
    Effect.gen(function* () {
      const currentTestName = process.env.__current_test_name__;
      console.log(`[PostgresTest] process.env.__current_test_name__ retrieved as: ${currentTestName}`);
      const sql = yield* SqlClient.SqlClient;

      const shouldTruncate = currentTestName
        ? !initializedTestNames.has(currentTestName)
        : !globalTruncated;

      if (shouldTruncate) {
        console.log(
          "[PostgresTest] Setting up DB. Url:",
          finalUrl,
          "migrationsRan:",
          migrationsRan,
          "testName:",
          currentTestName || "global",
        );
        if (!migrationsRan) {
          yield* runMigrations();
          migrationsRan = true;
        }
        const tables = yield* sql<{ table_name: string }>`
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name != 'effect_sql_migrations';
        `;
        if (tables.length > 0) {
          const tableNames = tables.map((t) => t.table_name).join(", ");
          console.log("TRUNCATING TABLES: " + tableNames);
          yield* sql.unsafe(`TRUNCATE TABLE ${tableNames} RESTART IDENTITY CASCADE;`);
          try {
            yield* sql`DROP TRIGGER IF EXISTS fail_thread_messages_projection_state_update ON projection_state`;
            yield* sql`DROP FUNCTION IF EXISTS fail_thread_messages_projection_state_update_fn`;
          } catch (_) {}
          console.log("TABLES TRUNCATED!");
        }

        if (currentTestName) {
          initializedTestNames.add(currentTestName);
        } else {
          globalTruncated = true;
        }
      } else {
        console.log(
          "[PostgresTest] Database already initialized for test/run:",
          currentTestName || "global",
          "- skipping truncation.",
        );
      }
    }),
  );

  const sqlLayer = Layer.effect(SqlClient.SqlClient, Effect.service(PgClient.PgClient));

  const combined = Layer.merge(pgLayer, Layer.provide(sqlLayer, pgLayer));

  return Layer.provideMerge(setup, combined);
}, Layer.unwrap);
