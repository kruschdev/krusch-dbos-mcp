import { Context, Effect, Layer, Schedule, Duration, Stream, Clock } from "effect";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { EmbeddingProvider } from "./EmbeddingProvider.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export interface HaloOptimizerShape {
  readonly runOptimizationLoop: Effect.Effect<void>;
}

export class HaloOptimizerService extends Context.Service<HaloOptimizerService, HaloOptimizerShape>()("t3/orchestration/HaloOptimizerService") {}

const makeHaloOptimizer = Effect.gen(function* () {
  const eventStore = yield* OrchestrationEventStore;
  const embeddingProvider = yield* EmbeddingProvider;
  const sql = yield* SqlClient.SqlClient;

  const runOptimizationLoop = Effect.gen(function* () {
    yield* Effect.logInfo("[HALO Optimizer] Starting nightly trace optimization sweep (Local-First)...");
    
    const allEvents = yield* Stream.runCollect(eventStore.readAll());
    const recentEvents = Array.from(allEvents).slice(-100);
    
    if (recentEvents.length === 0) {
      yield* Effect.logInfo("[HALO Optimizer] No traces found. Skipping.");
      return;
    }

    const compacted = recentEvents.map(e => ({
      type: e.type,
      commandId: e.commandId,
      threadId: "threadId" in e ? e.threadId : undefined,
      projectId: "projectId" in e ? e.projectId : undefined,
      timestamp: e.occurredAt
    }));

    const traceStr = JSON.stringify(compacted, null, 2);

    const prompt = `You are the HALO (Hierarchical Agent Loop Optimizer).
Analyze this DBOS execution trace for systemic agent failures, repetitive tool errors, or security boundary rejections.
TRACE:
${traceStr}

Synthesize the primary failure mode into ONE concise Nugget string. If no issues, reply "NO_ACTION".`;

    const modelName = process.env.HALO_LOCAL_MODEL || "llama3.1";
    
    // Derive base URL from the global OLLAMA_URL (which points to /api/embeddings)
    const rawOllamaBase = process.env.OLLAMA_URL
      ? process.env.OLLAMA_URL.replace(/\/api\/embeddings\/?$/, "")
      : "http://localhost:11434";
    const ollamaBase = rawOllamaBase.endsWith("/") ? rawOllamaBase.slice(0, -1) : rawOllamaBase;
    const ollamaUrl = process.env.OLLAMA_API_URL || `${ollamaBase}/api/generate`;
    
    const fetchResult = yield* Effect.tryPromise(() => fetch(ollamaUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: modelName,
        prompt: prompt,
        stream: false,
        options: { temperature: 0.2 }
      })
    })).pipe(
      Effect.catch((cause) => Effect.succeed(null))
    );

    if (!fetchResult) {
      yield* Effect.logError(`[HALO Optimizer] Local LLM connection failed to ${ollamaUrl}. Ensure Ollama is running.`);
      return;
    }

    if (!fetchResult.ok) {
      const errorText = yield* Effect.tryPromise(() => fetchResult.text()).pipe(Effect.catch(() => Effect.succeed("")));
      yield* Effect.logError(`[HALO Optimizer] Local LLM call failed: ${fetchResult.status} ${fetchResult.statusText} - ${errorText}`);
      return;
    }

    const data = (yield* Effect.tryPromise(() => fetchResult.json())) as any;
    const nugget = data.response?.trim() || "NO_ACTION";

    if (nugget !== "NO_ACTION") {
      yield* Effect.logInfo(`[HALO Optimizer] 🧠 Synthesized Nugget: ${nugget}`);
      yield* saveNuggetLocal(nugget);
    } else {
      yield* Effect.logInfo("[HALO Optimizer] No friction detected. System is operating optimally.");
    }
  });

  const saveNuggetLocal = (nugget: string) => Effect.gen(function* () {
      yield* Effect.logInfo(`[HALO Optimizer] Generating nomic embedding for nugget...`);
      const embedding = yield* embeddingProvider.getEmbedding(nugget);
      
      if (!Array.isArray(embedding) || embedding.length === 0) {
        yield* Effect.logError("[HALO Optimizer] Failed to generate embedding for nugget.");
        return;
      }

      const vectorStr = `[${embedding.join(",")}]`;
      const now = yield* Clock.currentTimeMillis;

      yield* Effect.logInfo(`[HALO Optimizer] Saving nugget to native DBOS orchestration_nuggets table...`);

      // Table is managed by migration 030_OrchestrationNuggets
      yield* sql`
        INSERT INTO orchestration_nuggets (content, embedding, created_at_ms)
        VALUES (${nugget}, ${vectorStr}::vector, ${now})
      `;

      yield* Effect.logInfo(`[HALO Optimizer] 💾 Successfully saved native DBOS nugget.`);
  });

  // Start the background worker
  yield* Effect.forkScoped(
    Effect.repeat(runOptimizationLoop, Schedule.spaced(Duration.hours(24)))
  );

  return {
    runOptimizationLoop
  };
});

export const HaloOptimizerLive = Layer.effect(HaloOptimizerService, makeHaloOptimizer);
