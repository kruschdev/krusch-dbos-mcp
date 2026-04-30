import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE orchestration_event_embeddings 
    ADD COLUMN IF NOT EXISTS project_id TEXT
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_orch_event_embeddings_project_id
    ON orchestration_event_embeddings(project_id)
  `;
});
