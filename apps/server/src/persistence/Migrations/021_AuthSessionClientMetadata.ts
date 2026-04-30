import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE auth_pairing_links
    ADD COLUMN IF NOT EXISTS label TEXT
  `;

  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN IF NOT EXISTS client_label TEXT
  `;

  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN IF NOT EXISTS client_ip_address TEXT
  `;

  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN IF NOT EXISTS client_user_agent TEXT
  `;

  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN IF NOT EXISTS client_device_type TEXT NOT NULL DEFAULT 'unknown'
  `;

  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN IF NOT EXISTS client_os TEXT
  `;

  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN IF NOT EXISTS client_browser TEXT
  `;
});
