import pg from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";

const { Client } = pg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables
const envPath = path.join(__dirname, "..", ".env");
if (fs.existsSync(envPath)) {
  const envFile = fs.readFileSync(envPath, "utf8");
  envFile.split("\n").forEach((line) => {
    const match = line.match(/^([^=]+)=(.*)$/);
    if (match) {
      process.env[match[1]] = match[2].replace(/^['"](.*)['"]$/, "$1");
    }
  });
}

const DATABASE_URL = process.env.DATABASE_URL || "postgres://kdcode:password@localhost:5432/kdcode";

function generateId() {
  return crypto.randomUUID();
}

async function dispatch(projectId, provider, model, prompt) {
  const commandId = generateId();
  const threadId = generateId();
  const messageId = generateId();
  const now = new Date().toISOString();

  const payload = {
    type: "thread.turn.start",
    commandId,
    threadId,
    message: {
      messageId,
      role: "user",
      text: prompt,
      attachments: [],
    },
    bootstrap: {
      createThread: {
        projectId,
        title: prompt.substring(0, 50) + "...",
        modelSelection: { provider, model, isCustom: false },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: now,
      },
    },
    createdAt: now,
  };

  const client = new Client({ connectionString: DATABASE_URL });
  try {
    await client.connect();

    await client.query(
      `
      INSERT INTO orchestration_command_queue (command_id, command_type, payload, status, started_at_ms)
      VALUES ($1, $2, $3, 'pending', $4)
    `,
      [commandId, "thread.turn.start", JSON.stringify(payload), Date.now()],
    );

    console.log(`[DBOS Dispatcher] Successfully queued command: ${commandId}`);
    console.log(`[DBOS Dispatcher] Target Project: ${projectId}`);
    console.log(`[DBOS Dispatcher] New Thread ID: ${threadId}`);
    console.log(
      `[DBOS Dispatcher] Run 'node scripts/dbos_dispatch.js status ${commandId}' to monitor.`,
    );
  } catch (err) {
    console.error("[DBOS Dispatcher] Failed to dispatch command:", err.message);
  } finally {
    await client.end();
  }
}

async function status(commandId) {
  const client = new Client({ connectionString: DATABASE_URL });
  try {
    await client.connect();
    const res = await client.query(
      `
      SELECT status, picked_at_ms FROM orchestration_command_queue WHERE command_id = $1
    `,
      [commandId],
    );

    if (res.rows.length === 0) {
      console.log(`[DBOS Dispatcher] Command ${commandId} not found in queue.`);
      return;
    }

    const row = res.rows[0];
    console.log(`[DBOS Dispatcher] Command ${commandId} status: ${row.status}`);
    if (row.picked_at_ms) {
      console.log(
        `[DBOS Dispatcher] Picked up at: ${new Date(parseInt(row.picked_at_ms)).toISOString()}`,
      );
    }
  } catch (err) {
    console.error("[DBOS Dispatcher] Failed to get status:", err.message);
  } finally {
    await client.end();
  }
}

const args = process.argv.slice(2);
const command = args[0];

if (command === "dispatch") {
  const projectId = args[1];
  const provider = args[2] || "chrysalis-swarm";
  const model = args[3] || "executor";
  const prompt = args.slice(4).join(" ");

  if (!projectId || !prompt) {
    console.error(
      "Usage: node dbos_dispatch.js dispatch <projectId> <provider> <model> <prompt...>",
    );
    process.exit(1);
  }
  dispatch(projectId, provider, model, prompt);
} else if (command === "status") {
  const commandId = args[1];
  if (!commandId) {
    console.error("Usage: node dbos_dispatch.js status <commandId>");
    process.exit(1);
  }
  status(commandId);
} else {
  console.log("Usage:");
  console.log("  node dbos_dispatch.js dispatch <projectId> <provider> <model> <prompt...>");
  console.log("  node dbos_dispatch.js status <commandId>");
}
