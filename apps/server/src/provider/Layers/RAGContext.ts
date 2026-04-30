import { Effect, Option } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { EmbeddingProvider, type EmbeddingProviderSvc } from "../../orchestration/Layers/EmbeddingProvider.ts";

export const fetchRAGContext = (
  promptText: string,
  threadId: string | null | undefined,
  provider: EmbeddingProviderSvc | null,
  sql: SqlClient.SqlClient | null,
): Effect.Effect<string, never, never> =>
  Effect.gen(function* () {
    if (!promptText || !provider || !sql) return "";

    const ragContext = yield* Effect.gen(function* () {
      let projectId: string | null = null;
      if (threadId) {
        const threadCreatedRow = yield* sql<{ project_id: string }>`
          SELECT payload_json->>'projectId' as project_id
          FROM orchestration_events
          WHERE aggregate_kind = 'thread' AND stream_id = ${threadId} AND event_type = 'thread.created'
          LIMIT 1
        `.pipe(Effect.catchAll(() => Effect.succeed([])));
        if (threadCreatedRow.length > 0) {
          projectId = threadCreatedRow[0].project_id;
        }
      }

      const embedding = yield* provider.getEmbedding(promptText);

      if (!Array.isArray(embedding) || embedding.length === 0) {
        return "";
      }

      const vectorStr = `[${embedding.join(",")}]`;

      const results = yield* sql<{ content: string }>`
        SELECT 
          content, 
          (
            (embedding <=> ${vectorStr}::vector) + 
            (GREATEST(0, (EXTRACT(EPOCH FROM NOW()) - (created_at_ms / 1000.0)) / 86400.0) * 0.005)
          ) AS distance
        FROM orchestration_event_embeddings
        WHERE (project_id = ${projectId} OR project_id IS NULL)
        ORDER BY distance ASC
        LIMIT 5
      `.pipe(
        Effect.catchTag("SqlError", (err) => {
          return Effect.logError("RAG SQL query failed", err).pipe(Effect.as([]));
        }),
      );

      if (results.length === 0) return "";

      const contextText = results.map((r) => r.content).join("\n---\n");
      return `\n\n### SEMANTIC RECALL CONTEXT (Historical Decisions & Actions) ###\n${contextText}\n\n`;
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning(`RAG Context retrieval failed`, cause).pipe(Effect.as("")),
      ),
    );

    return ragContext;
  });
