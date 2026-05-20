import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE EXTENSION IF NOT EXISTS vector`;

  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_event_embeddings (
      event_id TEXT PRIMARY KEY,
      content TEXT NOT NULL,
      embedding vector(1024) NOT NULL,
      created_at_ms BIGINT NOT NULL
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_orch_event_embeddings_embedding 
    ON orchestration_event_embeddings USING hnsw (embedding vector_cosine_ops)
  `;
});
