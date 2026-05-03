import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_nuggets (
      id SERIAL PRIMARY KEY,
      content TEXT NOT NULL,
      embedding vector NOT NULL,
      created_at_ms BIGINT NOT NULL
    )
  `;
});
