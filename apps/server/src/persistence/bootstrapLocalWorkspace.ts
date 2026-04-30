import Database from "better-sqlite3";
import * as path from "path";
import * as fs from "fs";

/**
 * Bootstraps the local SQLite database for a workspace folder.
 * This is the first step in the Dual Embedded Architecture pivot.
 */
export function bootstrapLocalWorkspaceDatabase(workspaceRoot: string) {
  const t3Dir = path.join(workspaceRoot, ".t3");
  if (!fs.existsSync(t3Dir)) {
    fs.mkdirSync(t3Dir, { recursive: true });
  }

  const dbPath = path.join(t3Dir, "local.sqlite");
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  // Migrate local context schema
  db.exec(`
    CREATE TABLE IF NOT EXISTS local_threads (
      thread_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      title TEXT NOT NULL,
      model_selection_json TEXT NOT NULL,
      runtime_mode TEXT NOT NULL,
      interaction_mode TEXT NOT NULL,
      branch TEXT,
      worktree_path TEXT,
      latest_turn_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      archived_at TEXT,
      latest_user_message_at TEXT,
      pending_approval_count INTEGER NOT NULL DEFAULT 0,
      pending_user_input_count INTEGER NOT NULL DEFAULT 0,
      has_actionable_proposed_plan INTEGER NOT NULL DEFAULT 0,
      deleted_at TEXT
    );

    CREATE TABLE IF NOT EXISTS local_messages (
      message_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      turn_id TEXT NOT NULL,
      role TEXT NOT NULL,
      text TEXT NOT NULL,
      attachments_json TEXT,
      is_streaming INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(thread_id) REFERENCES local_threads(thread_id)
    );

    CREATE TABLE IF NOT EXISTS local_file_embeddings (
      id TEXT PRIMARY KEY,
      file_path TEXT NOT NULL,
      content_hash TEXT NOT NULL,
      embedding BLOB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS local_thread_proposed_plans (
      plan_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      turn_id TEXT NOT NULL,
      plan_markdown TEXT NOT NULL,
      implemented_at TEXT,
      implementation_thread_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(thread_id) REFERENCES local_threads(thread_id)
    );

    CREATE TABLE IF NOT EXISTS local_thread_activities (
      activity_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      turn_id TEXT,
      tone TEXT NOT NULL,
      kind TEXT NOT NULL,
      summary TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      sequence INTEGER,
      created_at TEXT NOT NULL,
      FOREIGN KEY(thread_id) REFERENCES local_threads(thread_id)
    );

    CREATE TABLE IF NOT EXISTS local_thread_sessions (
      thread_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      provider_name TEXT NOT NULL,
      provider_session_id TEXT,
      provider_thread_id TEXT,
      runtime_mode TEXT NOT NULL,
      active_turn_id TEXT,
      last_error TEXT,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(thread_id) REFERENCES local_threads(thread_id)
    );

    CREATE TABLE IF NOT EXISTS local_turns (
      turn_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      state TEXT NOT NULL,
      requested_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      assistant_message_id TEXT,
      source_proposed_plan_thread_id TEXT,
      source_proposed_plan_id TEXT,
      checkpoint_turn_count INTEGER,
      checkpoint_ref TEXT,
      checkpoint_status TEXT,
      checkpoint_files_json TEXT,
      FOREIGN KEY(thread_id) REFERENCES local_threads(thread_id)
    );
  `);

  console.log(`[DualDB] Bootstrapped local SQLite database at ${dbPath}`);
  return db;
}
