import { Context, Effect, Option, Layer, Clock } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { toPersistenceSqlError, PersistenceSqlError } from "../Errors.ts";
import { ThreadId } from "@kd/contracts";

export interface AgentExecutionJob {
  readonly jobId: string;
  readonly threadId: ThreadId;
  readonly jobType: "AGENT_STEP" | "EXECUTE_TOOLS";
  readonly payload: unknown;
}

export interface AgentExecutionQueueShape {
  readonly offer: (job: AgentExecutionJob) => Effect.Effect<void, PersistenceSqlError>;
  readonly take: () => Effect.Effect<Option.Option<{ job: AgentExecutionJob; startedAtMs: number }>, PersistenceSqlError>;
  readonly complete: (jobId: string) => Effect.Effect<void, PersistenceSqlError>;
  readonly list: () => Effect.Effect<
    { jobId: string; threadId: ThreadId; jobType: string; status: string; startedAtMs: number; pickedAtMs: number | null }[],
    PersistenceSqlError
  >;
}

export class AgentExecutionQueue extends Context.Service<
  AgentExecutionQueue,
  AgentExecutionQueueShape
>()("t3/persistence/Services/AgentExecutionQueue") {}

export const AgentExecutionQueueLive = Layer.effect(
  AgentExecutionQueue,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const offer: AgentExecutionQueueShape["offer"] = (job) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* sql`
          INSERT INTO agent_execution_queue (
            job_id,
            thread_id,
            job_type,
            payload,
            status,
            started_at_ms
          ) VALUES (
            ${job.jobId},
            ${job.threadId},
            ${job.jobType},
            ${JSON.stringify(job.payload)}::jsonb,
            'pending',
            ${now}
          )
          ON CONFLICT (job_id) DO NOTHING
        `.pipe(
          Effect.catchTag("SqlError", (e) => Effect.fail(toPersistenceSqlError("AgentExecutionQueue.offer")(e))),
          Effect.asVoid,
        );
      });

    const take: AgentExecutionQueueShape["take"] = () => Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const rows = yield* sql<{ job_id: string; thread_id: string; job_type: string; payload: unknown; started_at_ms: string }>`
        UPDATE agent_execution_queue
        SET status = 'processing', picked_at_ms = ${now}
        WHERE job_id = (
          SELECT job_id FROM agent_execution_queue
          WHERE status = 'pending'
          ORDER BY started_at_ms ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        RETURNING job_id, thread_id, job_type, payload, started_at_ms
      `.pipe(
        Effect.catchTag("SqlError", (e) => {
          return Effect.fail(toPersistenceSqlError("AgentExecutionQueue.take")(e));
        })
      );
      if (!rows || rows.length === 0) return Option.none();
      const row = rows[0];
      if (!row) return Option.none();
      return Option.some({
        job: {
          jobId: row.job_id,
          threadId: row.thread_id as ThreadId,
          jobType: row.job_type as "AGENT_STEP" | "EXECUTE_TOOLS",
          payload: row.payload,
        },
        startedAtMs: parseInt(row.started_at_ms, 10),
      });
    });

    const complete: AgentExecutionQueueShape["complete"] = (jobId) =>
      sql`
        DELETE FROM agent_execution_queue
        WHERE job_id = ${jobId}
      `.pipe(
        Effect.catchTag("SqlError", (e) => Effect.fail(toPersistenceSqlError("AgentExecutionQueue.complete")(e))),
        Effect.asVoid,
      );

    const list: AgentExecutionQueueShape["list"] = () => Effect.gen(function* () {
      const rows = yield* sql<{ job_id: string; thread_id: string; job_type: string; status: string; started_at_ms: string; picked_at_ms: string | null }>`
        SELECT job_id, thread_id, job_type, status, started_at_ms, picked_at_ms
        FROM agent_execution_queue
        ORDER BY started_at_ms DESC
      `.pipe(
        Effect.catchTag("SqlError", (e) => Effect.fail(toPersistenceSqlError("AgentExecutionQueue.list")(e))),
      );
      return rows.map((row) => ({
        jobId: row.job_id,
        threadId: row.thread_id as ThreadId,
        jobType: row.job_type,
        status: row.status,
        startedAtMs: parseInt(row.started_at_ms, 10),
        pickedAtMs: row.picked_at_ms ? parseInt(row.picked_at_ms, 10) : null,
      }));
    });

    return { offer, take, complete, list } satisfies AgentExecutionQueueShape;
  })
);
