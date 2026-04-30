import pg from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

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

const DATABASE_URL = process.env.DATABASE_URL || "postgres://t3code:password@localhost:5432/t3code";
const OLLAMA_URL = process.env.OLLAMA_URL || "http://localhost:11434/api/embeddings";

async function generateEmbedding(text) {
  const response = await fetch(OLLAMA_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "nomic-embed-text",
      prompt: text,
    }),
  });
  if (!response.ok) {
    throw new Error(`Ollama fetch failed: ${response.statusText}`);
  }
  const json = await response.json();
  return json.embedding;
}

async function search(query) {
  console.log(`[Memory Bridge] Searching DBOS episodic memory for: "${query}"...`);

  let embedding;
  try {
    embedding = await generateEmbedding(query);
  } catch (err) {
    console.error("[Memory Bridge] Failed to generate embedding:", err.message);
    process.exit(1);
  }

  const client = new Client({ connectionString: DATABASE_URL });

  try {
    await client.connect();

    // Ensure vector extension exists
    await client.query("CREATE EXTENSION IF NOT EXISTS vector");

    // Convert array to pgvector string format
    const vectorStr = `[${embedding.join(",")}]`;

    // Query the database using cosine distance (<=>)
    const res = await client.query(
      `
      SELECT event_id, content, 1 - (embedding <=> $1::vector) AS similarity
      FROM orchestration_event_embeddings
      ORDER BY embedding <=> $1::vector
      LIMIT 5
    `,
      [vectorStr],
    );

    if (res.rows.length === 0) {
      console.log("[Memory Bridge] No semantic matches found.");
      return;
    }

    console.log("\n=== Top Semantic Matches ===\n");
    res.rows.forEach((row, i) => {
      console.log(
        `[Match ${i + 1}] Similarity: ${(row.similarity * 100).toFixed(1)}% | Event ID: ${row.event_id}`,
      );
      console.log(`Content:\n${row.content}\n`);
      console.log("-".repeat(40));
    });
  } catch (err) {
    if (err.message.includes('relation "orchestration_event_embeddings" does not exist')) {
      console.error(
        "[Memory Bridge] DBOS vector tables not initialized yet. Run migrations first.",
      );
    } else {
      console.error("[Memory Bridge] Database Error:", err);
    }
  } finally {
    await client.end();
  }
}

const command = process.argv[2];
const topic = process.argv[3];
const query = process.argv[4] || topic; // Handle `search lessons "foo"` or `search "foo"`

if (command === "search") {
  if (!query) {
    console.error('Usage: node ide_memory_bridge.js search "<query>"');
    process.exit(1);
  }
  search(query);
} else {
  console.log('Usage: node ide_memory_bridge.js search "<query>"');
}
