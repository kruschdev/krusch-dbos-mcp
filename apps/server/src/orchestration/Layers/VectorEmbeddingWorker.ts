import { Effect, Layer, Schedule, Clock } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { EmbeddingProvider } from "./EmbeddingProvider.ts";

export const VectorEmbeddingWorkerLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const provider = yield* EmbeddingProvider;

    const processBatch = Effect.gen(function* () {
      // Find up to 10 events that don't have embeddings yet
      const rows = yield* sql<{ sequence: number; event_id: string; payload_json: string; aggregate_kind: string; stream_id: string }>`
        SELECT e.sequence, e.event_id, e.payload_json::text, e.aggregate_kind, e.stream_id 
        FROM orchestration_events e
        LEFT JOIN orchestration_event_embeddings ee ON e.event_id = ee.event_id
        WHERE ee.event_id IS NULL
        ORDER BY e.sequence ASC
        LIMIT 10
      `;

      if (rows.length === 0) {
        return;
      }

      yield* Effect.forEach(
        rows,
        (row) =>
          Effect.gen(function* () {
            // Parse JSON to extract semantic text
            let content = "";
            try {
              const parsed = JSON.parse(row.payload_json);
              
              // Enhanced extraction logic
              if (parsed.message?.text) {
                content = parsed.message.text;
              } else if (parsed.activity?.summary) {
                content = `${parsed.activity.kind}: ${parsed.activity.summary}`;
                if (parsed.activity.payload?.detail) {
                  content += `\nDetail: ${parsed.activity.payload.detail}`;
                }
              } else if (parsed.delta) {
                content = parsed.delta;
              } else if (parsed.command?.output) {
                content = parsed.command.output;
              } else if (parsed.plan?.markdown) {
                content = parsed.plan.markdown;
              } else if (parsed.text) {
                content = parsed.text;
              } else if (parsed.toolName) {
                content = `Tool Call: ${parsed.toolName}\nInput: ${JSON.stringify(parsed.toolInput)}`;
              } else {
                // Fallback to stringified JSON if no clear text field exists
                content = String(row.payload_json);
              }
            } catch (e) {
              content = String(row.payload_json);
            }

            content = content.substring(0, 5000).trim(); // Increased limit for richer context

            if (!content) {
              return;
            }

            const embedding = yield* provider.getEmbedding(content);

            if (!Array.isArray(embedding)) {
              return;
            }

            // Format array for pgvector: [1,2,3]
            const vectorStr = `[${embedding.join(",")}]`;
            const now = yield* Clock.currentTimeMillis;

            let projectId: string | null = null;
            if (row.aggregate_kind === "project") {
              projectId = row.stream_id;
            } else if (row.aggregate_kind === "thread") {
              const threadCreatedRow = yield* sql<{ project_id: string }>`
                SELECT payload_json->>'projectId' as project_id
                FROM orchestration_events
                WHERE aggregate_kind = 'thread' AND stream_id = ${row.stream_id} AND event_type = 'thread.created'
                LIMIT 1
              `.pipe(Effect.catchAll(() => Effect.succeed([])));
              if (threadCreatedRow.length > 0) {
                projectId = threadCreatedRow[0].project_id;
              }
            }

            yield* sql`
              INSERT INTO orchestration_event_embeddings (event_id, project_id, content, embedding, created_at_ms)
              VALUES (${row.event_id}, ${projectId}, ${content}, ${vectorStr}::vector, ${now})
              ON CONFLICT (event_id) DO NOTHING
            `;

            yield* Effect.logInfo(`Embedded event ${row.event_id}`);
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logError(`Failed to embed event ${row.event_id}`, cause),
            ),
          ),
        { concurrency: 3 },
      );
    });

    // Run the worker loop every 5 seconds
    yield* Effect.logInfo("Starting Vector Embedding Worker...");
    yield* Effect.repeat(processBatch, Schedule.spaced("5 seconds")).pipe(Effect.forkScoped);
  }),
);
