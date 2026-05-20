import { Effect, Layer, Redacted } from "effect";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";

export const makeTestPgPersistenceLive = Effect.fn("makeTestPgPersistenceLive")(function* (
  databaseUrl: string,
) {
  const pgLayer = PgClient.layer({
    url: Redacted.make(databaseUrl),
    maxConnections: 20,
    spanAttributes: { "service.name": "kd-server-test" },
  });

  const setup = Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const tables = yield* sql<{ table_name: string }>`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name != 'effect_sql_migrations';
      `;
      if (tables.length > 0) {
        const tableNames = tables.map((t) => t.table_name).join(", ");
        console.log("TRUNCATING TABLES: " + tableNames);
        yield* sql.unsafe(`TRUNCATE TABLE ${tableNames} RESTART IDENTITY CASCADE;`);
        console.log("TABLES TRUNCATED!");
      }
    }),
  );

  const sqlLayer = Layer.effect(SqlClient.SqlClient, Effect.service(PgClient.PgClient));

  const combined = Layer.merge(pgLayer, Layer.provide(sqlLayer, pgLayer));

  return Layer.provideMerge(setup, combined);
}, Layer.unwrap);
