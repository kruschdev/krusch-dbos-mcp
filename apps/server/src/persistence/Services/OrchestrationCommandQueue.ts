import { Context, Effect, Option, Layer, Clock } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { OrchestrationCommand } from "@t3tools/contracts";
import { toPersistenceSqlError, PersistenceSqlError } from "../Errors.ts";

export interface OrchestrationCommandQueueShape {
  readonly offer: (command: OrchestrationCommand) => Effect.Effect<void, PersistenceSqlError>;
  readonly take: () => Effect.Effect<Option.Option<{ command: OrchestrationCommand; startedAtMs: number }>, PersistenceSqlError>;
  readonly complete: (commandId: string) => Effect.Effect<void, PersistenceSqlError>;
}

export class OrchestrationCommandQueue extends Context.Service<
  OrchestrationCommandQueue,
  OrchestrationCommandQueueShape
>()("t3/persistence/Services/OrchestrationCommandQueue") {}

export const OrchestrationCommandQueueLive = Layer.effect(
  OrchestrationCommandQueue,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const offer: OrchestrationCommandQueueShape["offer"] = (command) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* sql`
          INSERT INTO orchestration_command_queue (
            command_id,
            command_type,
            payload,
            status,
            started_at_ms
          ) VALUES (
            ${command.commandId},
            ${command.type},
            ${JSON.stringify(command)}::jsonb,
            'pending',
            ${now}
          )
          ON CONFLICT (command_id) DO NOTHING
        `.pipe(
          Effect.catchTag("SqlError", (e) => Effect.fail(toPersistenceSqlError("OrchestrationCommandQueue.offer")(e))),
          Effect.asVoid,
        );
      });

    const take: OrchestrationCommandQueueShape["take"] = () => Effect.gen(function* () {
      yield* Effect.logDebug("take() called internally!");
      const now = yield* Clock.currentTimeMillis;
      const rows = yield* sql<{ payload: OrchestrationCommand; started_at_ms: string }>`
        UPDATE orchestration_command_queue
        SET status = 'processing', picked_at_ms = ${now}
        WHERE command_id = (
          SELECT command_id FROM orchestration_command_queue
          WHERE status = 'pending'
          ORDER BY started_at_ms ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        RETURNING payload, started_at_ms
      `.pipe(
        Effect.catchTag("SqlError", (e) => {
          return Effect.logError(`take() SqlError: ${e}`).pipe(
            Effect.flatMap(() => Effect.fail(toPersistenceSqlError("OrchestrationCommandQueue.take")(e)))
          );
        })
      );
      yield* Effect.logDebug(`take raw query returned ${rows?.length}`);
      if (!rows || rows.length === 0) return Option.none();
      const row = rows[0];
      if (!row) return Option.none();
      return Option.some({
        command: row.payload,
        startedAtMs: parseInt(row.started_at_ms, 10),
      });
    });

    const complete: OrchestrationCommandQueueShape["complete"] = (commandId) =>
      sql`
        DELETE FROM orchestration_command_queue
        WHERE command_id = ${commandId}
      `.pipe(
        Effect.catchTag("SqlError", (e) => Effect.fail(toPersistenceSqlError("OrchestrationCommandQueue.complete")(e))),
        Effect.asVoid,
      );

    return { offer, take, complete } satisfies OrchestrationCommandQueueShape;
  })
);
