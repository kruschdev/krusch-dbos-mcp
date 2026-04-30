# Krusch DBOS (Headless MCP Orchestrator)

<p align="center">
  <img src="assets/dbos-banner.png" alt="DBOS Banner" width="100%">
</p>

> A secure, headless orchestration engine for AI agents. Provides ACID-compliant state persistence and strict, capability-gated routing for Model Context Protocol (MCP) tool execution.

An ultra-concurrent, horizontally scalable agentic coding infrastructure built entirely around the **Database-Oriented Operating System (DBOS)** paradigm.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![Node](https://img.shields.io/badge/Node.js-22+-green.svg)
![Effect](https://img.shields.io/badge/Effect--TS-Strict-blue.svg)
![DB](<https://img.shields.io/badge/Database-PostgreSQL%20(pgvector)-lightgrey.svg>)

## 🧠 Why a Database Operating System (DBOS)?

Traditional AI coding assistants are fundamentally local, single-player applications. If you attempt to connect autonomous agents directly to your system, you risk massive concurrency failures, out-of-sync context, and severe security vulnerabilities (e.g., agents executing destructive terminal commands).

**Krusch DBOS MCP** transforms local AI execution into a **distributed agentic engine**. By migrating orchestration logic out of ephemeral memory and into a highly robust, transactional relational database (PostgreSQL), the database itself operates as the state machine and message bus. Every agent's thought process, tool execution, and architectural plan becomes a persistent, ACID-compliant database transaction.

### What critical bottlenecks does this solve?

1. **The "Shadow MCP" Security Risk**: Allowing agents direct access to raw MCP servers creates massive security blind spots. DBOS acts as a **Zero-Trust Capability Boundary**. Agents only interact with DBOS, which safely routes requests to backend servers (like Infra MCP) that explicitly block destructive commands.
2. **State Loss & Resilience Failures**: If an agent crashes during a 50-step reasoning loop, traditional memory is lost. In Krusch DBOS MCP, every atomic step is persisted. If a compute node loses power, the active transaction is cleanly rolled back, and the `SKIP LOCKED` queue releases the job for resumption.
3. **Context Amnesia ("Goldfish Memory")**: DBOS natively integrates an asynchronous `VectorEmbeddingWorker` powered by `pgvector`. It stores every agent decision and shell command into a temporal-decayed vector database, allowing agents to instantly recall architectural context across infinite sessions.
4. **Self-Healing Agent Loops (HALO)**: DBOS natively runs the **[Hierarchical Agent Loop Optimizer (HALO)](https://github.com/context-labs/halo)**. A background worker continuously sweeps execution traces using a local Ollama LLM to identify systemic agent failures, synthesizing them into behavioral "[Nuggets]" (credit to [NeoVertex1/nuggets](https://github.com/NeoVertex1/nuggets) for the memory storage concept) that gently steer future agent logic.

## 🤝 Swarm Orchestration (Ecosystem Synergy)

Krusch DBOS MCP is the central orchestration router of the KruschDev ecosystem. It receives high-level intents from autonomous clients (like OpenClaw or Claude) and seamlessly routes the specialized tool calls to isolated boundary servers:

- **Krusch DBOS MCP (The Orchestrator)**: The overarching Swarm Director. Enforces security boundaries and maintains the Postgres queues.
- **Krusch Infra MCP (The Mechanic)**: Provides read-only monitoring and safe container bouncing.
- **Krusch Memory MCP (The Archivist)**: Provides persistent episodic memory and project context.
- **PG-Git (The Coder)**: Edits files, writes code, and commits changes safely based on DBOS plans.

> 🗺️ **Want to see the big picture?** Read [The Krusch Stack Ecosystem](./ECOSYSTEM.md) for a complete diagram of how the LLM Proxy, DBOS, and all MCP boundaries fit together.

## 🛡️ Agent Guardrails & Secure Access

If you connect a headless agent to the DBOS MCP, **it does not get unrestricted root access to your computer**. The system is built around strict security protocols:

1. **Targeted Workspace Scoping**: Filesystem tools (`read_file`, `edit_file`) are explicitly jailed. A rogue agent cannot traverse up the file tree to read `/etc/shadow`.
2. **Governable Tool Sets**: DBOS abstracts capabilities into strictly defined MCP tools. All operations are synchronously logged to PostgreSQL before execution, allowing for programmatic halting if a malicious command is detected.
3. **Delegated Destructive Boundaries**: The DBOS orchestrator itself does not hold root privileges. Requests for dangerous actions are routed to boundary servers that natively hard-block destructive commands.

## ⚡ Quick Start

### Prerequisites

- Node.js & Bun (`bun >= 1.0.0`)
- Docker & Docker Compose (for PostgreSQL + `pgvector`)

### 1. Boot the Database Layer

```bash
docker compose up -d
```

### 2. Install & Bootstrap

```bash
bun install
# Run database migrations and bootstrap schema
bun run dev:server --migrate
```

### 3. Connect Your Agents

You can dynamically add downstream MCP servers to DBOS by editing the `~/.t3/mcp.json` file. DBOS will automatically discover them, aggregate their safe tools, and present a unified capability list to any connected MCP client.

---

## 🛠️ Configuration

Copy `.env.example` to `.env` and configure accordingly:

- **`DATABASE_URL`**: Primary PostgreSQL connection string (e.g., `postgres://user:password@db:5432/my_database`).
- **`OLLAMA_URL`**: Local embedding configuration used by the `VectorEmbeddingWorker` (e.g., `http://localhost:11434`).

---

## 🗄️ API & Connectivity Layer

The backend exposes a standard Model Context Protocol interface:

- `GET /mcp/sse`: Connects autonomous agents (like OpenClaw) to the DBOS MCP Server via Server-Sent Events (SSE).
- `POST /mcp/messages`: Receives and executes JSON-RPC tool messages from MCP clients.
- `GET /.well-known/t3/environment`: Emits public server configuration and routing capabilities.

---

## 🏗️ Architecture

For a deep dive into the DBOS queuing model and sequence diagrams, please read [ARCHITECTURE.md](./ARCHITECTURE.md).

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details. Created by [kruschdev](https://github.com/kruschdev).
