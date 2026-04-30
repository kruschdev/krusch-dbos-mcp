import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE projection_projects
    ADD COLUMN default_model_selection_json JSONB
  `;

  yield* sql`
    ALTER TABLE projection_threads
    ADD COLUMN model_selection_json JSONB
  `;

  yield* sql`
    ALTER TABLE projection_projects
    DROP COLUMN default_model
  `;

  yield* sql`
    ALTER TABLE projection_threads
    DROP COLUMN model
  `;

  // Data migrations skipped for new Postgres DBOS architecture
});
