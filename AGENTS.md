# Krusch DBOS MCP Agent Context

> **Project Origin**: A highly-concurrent, horizontally scalable agentic coding environment retrofitted from the original Krusch DBOS web UI. This repository implements a Database-Oriented Operating System (DBOS) architecture.

## ⚠️ Hazards & Critical Safety Rules

- **NEVER use `docker-compose down -v`**: The PostgreSQL container utilizes the `pgvector` extension and stores critical RAG embeddings and database orchestrations. Wiping the volume destroys all active queues and memory context. Use `docker-compose down` (without `-v`) or `docker restart`.
- **NEVER use `it.live` in tests**: The Effect integration tests are vulnerable to fiber leakage when using standard `@effect/vitest` functions. You **MUST** use the custom `itLive` wrapper from the integration harness to ensure clean teardown and prevent the test runner from hanging indefinitely.
- **SQLITE_BUSY / SQLite Drivers**: This repository has fully migrated to a PostgreSQL-backed architecture via `@effect/sql-pg`. Do **NOT** attempt to scaffold `better-sqlite3` or legacy local files. All orchestrations occur over PostgreSQL `JSONB`.

## 🚀 Quick Start

```bash
# 1. Start the PostgreSQL DBOS Persistence Layer
docker compose up -d

# 2. Install dependencies
bun install

# 3. Run database migrations and bootstrap schema
bun run dev:server --migrate

# 4. Start the Development Server & Thin Client UI
bun run dev
```

## 🏗️ Architecture Overview

The Krusch DBOS MCP architecture decouples heavy Node.js LLM execution environments from lightweight client UIs. By replacing in-memory Node.js loops with a stateless client-server model, multiple agents can safely execute concurrent tasks.

- **DBOS `SKIP LOCKED` Queues**: In-memory orchestration loops have been transitioned to Postgres-native job queues. Workers poll for pending jobs using `SELECT ... FOR UPDATE SKIP LOCKED`, ensuring tasks are consumed exactly once across multi-node deployments without lock contention.
- **DBOS Native Tool Execution**: The `AgentExecutionEngine` background worker natively processes `EXECUTE_TOOLS` queue payloads. This enables agents to modify the file system (`edit_file`, `write_to_file`) and execute system commands (`bash`) natively on the server, streaming outputs back to the DBOS UI via ACID-compliant event dispatching.
- **Universal RAG Embeddings & Dynamic Realignment**: All AI Provider Adapters (Gemini, Claude, Codex) ingest semantic embeddings asynchronously via `VectorEmbeddingWorker`. The system defaults to **`bge-large`** (1024 dimensions) for Ollama to ensure fleet continuity. On worker startup, the system automatically dispatches a test embedding to the active provider to auto-detect its exact dimension output. If the database vector column dimension has a mismatch, the worker dynamically drops the index, truncates the table, alters the column type (`ALTER COLUMN ... TYPE vector(detectedDimension)`), and rebuilds the HNSW vector index at runtime.
- **HALO Optimization Loop**: A local-first `HaloOptimizerService` (inspired by the [context-labs/halo](https://github.com/context-labs/halo) framework) runs entirely decoupled from the main thread, performing nightly sweeps on execution traces. Using local Ollama text-generation and native vector embedding, it synthesizes agent failure states into actionable behavioral nudges natively within the `orchestration_nuggets` table.
- **High-Performance NVMe SSD Storage**: To ensure ultra-low-latency database reads/writes and embedding queries, all Docker containers, event streams, and PostgreSQL databases (specifically the `kruschdb-postgres-data` volume) should reside natively on a high-speed NVMe SSD pool via system-level `"data-root"` configuration.
- **Stateless Execution**: The orchestrator serves strictly as a headless MCP server communicating over standard stdio or HTTP/SSE.

## 🗺️ Key File Map

| Component / Layer | Location                            | Purpose                                                                                                                                                      |
| ----------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Orchestration** | `apps/server/src/orchestration/`    | Core DBOS queue polling, command dispatch, Vector Embedding, and local HALO trace optimization background workers.                                           |
| **Providers**     | `apps/server/src/provider/`         | Universal RAG Context utility and AI Adapters (Claude, Gemini, Codex).                                                                                       |
| **Persistence**   | `apps/server/src/persistence/`      | `@effect/sql-pg` client implementations and JSONB database schema schemas.                                                                                   |
| **Connectivity**  | `apps/server/src/http.ts` & `ws.ts` | The API and WebSocket handlers acting as the bridge for thin-client UIs.                                                                                     |
| **Web UI**        | `apps/web/`                         | React-based frontend thin client.                                                                                                                            |
| **Desktop App**   | `apps/desktop/`                     | Electron thin client with LAN discovery overrides.                                                                                                           |
| **Tests**         | `apps/server/integration/`          | End-to-end integration tests using the custom `itLive` wrapper.                                                                                              |

## 🤖 External Agent SDK Integration

Krusch DBOS MCP supports native integrations with custom agent networks. This allows you to build your agent logic in Python, Go, or TypeScript using official SDKs or raw SSE, while utilizing the DBOS web UI for rendering and state tracking.

### The SSE Streaming Contract

To integrate an external agent, configure your server to listen for `POST` requests at your specified endpoint (configured via `EXTERNAL_AGENT_URL` in `.env`).

The DBOS backend will send the following JSON payload:
```json
{
  "taskRequest": "User's prompt goes here."
}
```

Your external agent API must respond using **Server-Sent Events (SSE)**. The `AgentExecutionEngine` expects events formatted as:
```text
data: {"type": "info", "text": "Initializing agent...\n"}

data: {"type": "content.delta", "text": "Searching vector database...\n"}
```

**Note:** Ensure your SSE chunk payloads end with `\n\n` to properly signal event completion to the DBOS streaming parsers.

### Example: Node.js SSE Integration

```typescript
// Define your standard agent workflow or LLM stream
const logStream = await myAgent.streamEvents({ messages: [taskRequest] });

// Forward events directly to the DBOS UI via SSE
for await (const event of logStream) {
  if (event.type === "content") {
    controller.enqueue(`data: {"type": "content.delta", "text": "${event.data.chunk}\\n"}\n\n`);
  }
}
```

## 🛠️ Common Tasks

- **Updating Configuration**: Adjust environment properties in `.env` (refer to `.env.example`).
- **Running Tests**: `bun run test` (executes unit tests and integration tests utilizing PostgreSQL).
- **Adding an AI Provider**: Scaffold a new adapter in `apps/server/src/provider/Layers/` and inject the unified `fetchRAGContext` pipeline for `pgvector` support.
