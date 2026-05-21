import { Effect, Layer, Redacted } from "effect";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import { ServerConfig } from "../../config.ts";

const setup = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* runMigrations();
  }),
);

export const makePgPersistenceLive = Effect.fn("makePgPersistenceLive")(function* (
  databaseUrl: string,
) {
  const maxPoolConnections = parseInt(process.env.DB_POOL_MAX || "50", 10);
  const pgLayer = PgClient.layer({
    url: Redacted.make(databaseUrl),
    maxConnections: maxPoolConnections,
    spanAttributes: {
      "service.name": "kd-server",
    },
  });

  const sqlLayer = Layer.effect(SqlClient.SqlClient, Effect.service(PgClient.PgClient));

  const combined = Layer.merge(pgLayer, Layer.provide(sqlLayer, pgLayer));

  return Layer.provideMerge(setup, combined);
}, Layer.unwrap);

export const PostgresPersistenceLayerLive = Layer.unwrap(
  Effect.map(Effect.service(ServerConfig), ({ databaseUrl }) => {
    let url =
      databaseUrl || process.env.DATABASE_URL || "postgres://kruschdb:password@localhost:5432/kruschdb";
    if (process.env.VITEST && !url.includes("5435")) {
      url = "postgres://kruschdb:password@localhost:5435/kruschdb_test";
    }
    return makePgPersistenceLive(url);
  }),
);
