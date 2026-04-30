import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_execution_queue (
      job_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      job_type TEXT NOT NULL,
      payload JSONB NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      started_at_ms BIGINT NOT NULL,
      picked_at_ms BIGINT
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_exec_queue_status_started
    ON agent_execution_queue(status, started_at_ms ASC)
  `;
});
