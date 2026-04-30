import { Effect, Context, Layer } from "effect";
import { ServerSettingsService } from "../../serverSettings.ts";

export interface EmbeddingProviderSvc {
  readonly getEmbedding: (text: string) => Effect.Effect<number[], Error>;
}

export class EmbeddingProvider extends Context.Service<
  EmbeddingProvider,
  EmbeddingProviderSvc
>()("t3/orchestration/EmbeddingProvider") {}

export const EmbeddingProviderLive = Layer.effect(
  EmbeddingProvider,
  Effect.gen(function* () {
    const settingsService = yield* ServerSettingsService;

    const service: EmbeddingProviderSvc = {
      getEmbedding: (text: string) =>
        Effect.gen(function* () {
          const { embeddingModelSelection: selection } = yield* settingsService.getSettings;

          if (selection.provider === "none") {
            return yield* Effect.succeed([]);
          }

          if (selection.provider === "ollama") {
            const url = selection.apiEndpoint || "http://localhost:11434/api/embeddings";
            const response = yield* Effect.tryPromise({
              try: () =>
                fetch(url, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    model: selection.model,
                    prompt: text,
                  }),
                }),
              catch: (err) => new Error(`Ollama fetch failed: ${err}`),
            });

            if (!response.ok) {
              throw new Error(`Ollama returned status ${response.status}`);
            }

            const json = (yield* Effect.tryPromise(() => response.json())) as any;
            return json.embedding;
          }

          if (selection.provider === "gemini") {
            const apiKey = selection.apiKey;
            if (!apiKey) {
              throw new Error("Gemini API key is required for embeddings");
            }
            const url = `https://generativelanguage.googleapis.com/v1beta/models/${selection.model}:embedContent?key=${apiKey}`;
            const response = yield* Effect.tryPromise({
              try: () =>
                fetch(url, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    content: { parts: [{ text }] },
                  }),
                }),
              catch: (err) => new Error(`Gemini fetch failed: ${err}`),
            });

            if (!response.ok) {
              throw new Error(`Gemini returned status ${response.status}`);
            }

            const json = (yield* Effect.tryPromise(() => response.json())) as any;
            return json.embedding.values;
          }

          if (selection.provider === "openai") {
            const apiKey = selection.apiKey;
            if (!apiKey) {
              throw new Error("OpenAI API key is required for embeddings");
            }
            const url = "https://api.openai.com/v1/embeddings";
            const response = yield* Effect.tryPromise({
              try: () =>
                fetch(url, {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${apiKey}`,
                  },
                  body: JSON.stringify({
                    input: text,
                    model: selection.model,
                  }),
                }),
              catch: (err) => new Error(`OpenAI fetch failed: ${err}`),
            });

            if (!response.ok) {
              throw new Error(`OpenAI returned status ${response.status}`);
            }

            const json = (yield* Effect.tryPromise(() => response.json())) as any;
            return json.data[0].embedding;
          }

          throw new Error(`Unsupported embedding provider: ${selection.provider}`);
        }),
    };
    return service;
  }),
);
