# Contributing to Krusch DBOS MCP

First off, thank you for considering contributing to Krusch DBOS MCP! This is the core orchestration layer for our agentic ecosystem, and we rely heavily on community contributions to expand its routing capabilities and provider integrations.

## 1. Where to Contribute

Krusch DBOS MCP is purely a **headless orchestration backend**. If you are looking to contribute to a user interface or thin-client IDE, please check the appropriate external repositories.

Contributions here should focus on:
- Optimizing PostgreSQL `SKIP LOCKED` queries.
- Expanding the Effect-TS routing engine.
- Adding new MCP boundary server integrations or RAG models.

## 2. Development Setup

To get your local environment running for testing the DBOS engine:

1. Ensure you have `bun` and `docker` installed.
2. Spin up the local database layer:
   ```bash
   docker-compose up -d
   ```
3. Install dependencies and run migrations:
   ```bash
   bun install
   bun run dev:server --migrate
   ```

## 3. Pull Request Guidelines

We strictly enforce a small, focused PR philosophy to maintain the stability of the orchestration queue.

- **No UI Changes**: Do not include screenshots or UI components in your PRs. This repo is 100% headless.
- **Unit Tests**: Any changes to the `AgentExecutionQueue` or `HaloOptimizerService` must include corresponding `vitest` unit tests.
- **Explain the Why**: Ensure your PR description explains *why* the architectural change was made, not just *what* it does.

## 4. Code Style

We use `oxlint` for linting and `tsc` for strict type checking.

Run the following before submitting a PR:
```bash
bun run lint
bun run typecheck
```

Thank you for helping us build the ultimate secure AI orchestrator!
