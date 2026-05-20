import { randomUUID } from "node:crypto";
import {
  EventId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { Exit } from "effect";
import { Cause, Effect, Layer, Queue, Ref, Scope, Stream, Option } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { fetchRAGContext } from "./RAGContext.ts";
import { EmbeddingProvider, type EmbeddingProviderSvc } from "../../orchestration/Layers/EmbeddingProvider.ts";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { GeminiAdapter } from "../Services/GeminiAdapter.ts";

const PROVIDER = "gemini" as const;

interface GeminiTurnSnapshot {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

interface GeminiSessionContext {
  session: ProviderSession;
  readonly turns: Array<GeminiTurnSnapshot>;
  activeTurnId: TurnId | undefined;
  activeTurnController: AbortController | undefined;
  readonly stopped: Ref.Ref<boolean>;
  readonly sessionScope: Scope.Closeable;
}

export interface GeminiAdapterLiveOptions {}

function nowIso(): string {
  return new Date().toISOString();
}

function buildEventBase(input: {
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly createdAt?: string | undefined;
  readonly raw?: unknown;
}): Pick<
  ProviderRuntimeEvent,
  "eventId" | "provider" | "threadId" | "createdAt" | "turnId" | "itemId" | "requestId" | "raw"
> {
  return {
    eventId: EventId.make(randomUUID()),
    provider: PROVIDER,
    threadId: input.threadId,
    createdAt: input.createdAt ?? nowIso(),
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
    ...(input.requestId ? { requestId: RuntimeRequestId.make(input.requestId) } : {}),
    ...(input.raw !== undefined
      ? {
          raw: {
            source: "gemini.api",
            payload: input.raw as any,
          },
        }
      : {}),
  } as any;
}

export function makeGeminiAdapterLive(options?: GeminiAdapterLiveOptions) {
  return Layer.effect(
    GeminiAdapter,
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig;
      const serverSettings = yield* ServerSettingsService;
      const sqlOption = yield* Effect.serviceOption(SqlClient.SqlClient);
      const embeddingProviderOption = yield* Effect.serviceOption(EmbeddingProvider);
      const embeddingProviderSvc: EmbeddingProviderSvc | null = Option.getOrNull(embeddingProviderOption);
      const sqlSvc: SqlClient.SqlClient | null = Option.getOrNull(sqlOption);
      const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
      const sessions = new Map<ThreadId, GeminiSessionContext>();

      const emit = (event: ProviderRuntimeEvent) =>
        Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);

      const stopGeminiContext = Effect.fn("stopGeminiContext")(function* (
        context: GeminiSessionContext,
      ) {
        if (yield* Ref.getAndSet(context.stopped, true)) {
          return;
        }
        yield* Effect.ignore(
          Scope.close(context.sessionScope, { _tag: "Success", value: undefined } as any),
        );
      });

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          const contexts = [...sessions.values()];
          sessions.clear();
          yield* Effect.forEach(
            contexts,
            (context) => Effect.ignoreCause(stopGeminiContext(context)),
            { concurrency: "unbounded", discard: true },
          );
        }),
      );

      return GeminiAdapter.of({
        provider: PROVIDER,
        capabilities: {
          sessionModelSwitch: "in-session",
        },
        streamEvents: Stream.fromQueue(runtimeEvents),

        startSession: (input) =>
          Effect.gen(function* () {
            if (input.provider !== undefined && input.provider !== PROVIDER) {
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "startSession",
                issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
              });
            }

            const sessionScope = yield* Scope.make("sequential");
            const stopped = yield* Ref.make(false);

            const session: ProviderSession = {
              provider: PROVIDER,
              status: "ready",
              runtimeMode: input.runtimeMode,
              cwd: input.cwd,
              model: input.modelSelection?.model || "gemini-3.1-pro",
              threadId: input.threadId,
              createdAt: nowIso(),
              updatedAt: nowIso(),
            };

            const context: GeminiSessionContext = {
              session,
              turns: [],
              activeTurnId: undefined,
              activeTurnController: undefined,
              stopped,
              sessionScope,
            };

            sessions.set(input.threadId, context);

            yield* emit({
              ...buildEventBase({ threadId: input.threadId }),
              type: "session.started",
              payload: { resume: null },
            });

            return session;
          }),

        sendTurn: (input) =>
          Effect.gen(function* () {
            const context = sessions.get(input.threadId);
            if (!context || (yield* Ref.get(context.stopped))) {
              return yield* new ProviderAdapterSessionNotFoundError({
                provider: PROVIDER,
                threadId: input.threadId,
              });
            }

            const turnId = TurnId.make(randomUUID());
            context.activeTurnId = turnId;
            context.activeTurnController = new AbortController();
            context.turns.push({ id: turnId, items: [] });

            const settings = yield* serverSettings.getSettings.pipe(
              Effect.mapError(
                (err) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "sendTurn",
                    detail: `Failed to load settings: ${err.message}`,
                    cause: err,
                  }),
              ),
            );
            const geminiConfig = settings.providers.gemini;
            const apiKey = geminiConfig?.apiKey || serverConfig.geminiApiKey;
            const useCli = !apiKey;

            yield* emit({
              ...buildEventBase({ threadId: input.threadId, turnId }),
              type: "turn.started",
              payload: {},
            });

            yield* Effect.gen(function* () {
              let currentItemId: string | undefined = undefined;
              let fullText = "";
              
              const execution = Effect.gen(function* () {
                let prompt = input.input || "";

                // Automatically utilize vector-search for RAG context
                if (prompt) {
                  const ragContext = yield* fetchRAGContext(prompt, input.threadId, embeddingProviderSvc, sqlSvc);
                  prompt += ragContext;
                  input = { ...input, input: prompt };
                }

                currentItemId = randomUUID();
                const itemId = currentItemId;
                yield* emit({
                  ...buildEventBase({ threadId: input.threadId, turnId, itemId }),
                  type: "item.started",
                  payload: { itemType: "assistant_message", status: "inProgress" },
                });

                if (useCli) {
                  const { spawn } = yield* Effect.sync(() => require("node:child_process"));
                  yield* Effect.tryPromise({
                    try: () =>
                      new Promise<void>((resolve, reject) => {
                        if (context.activeTurnController?.signal.aborted) {
                          return reject(new Error("AbortError"));
                        }

                        const child = spawn("gemini", [
                          "-p",
                          prompt,
                          "--policy",
                          "",
                          "--admin-policy",
                          "",
                        ]);

                        if (context.activeTurnController?.signal) {
                          context.activeTurnController.signal.addEventListener("abort", () => {
                            child.kill();
                            reject(new Error("AbortError"));
                          });
                        }

                        child.stdout.on("data", (data: Buffer) => {
                          const textChunk = data.toString();
                          fullText += textChunk;
                          // Use Queue.offer and Effect.runFork
                          Effect.runFork(
                            Queue.offer(runtimeEvents, {
                              ...buildEventBase({
                                threadId: input.threadId,
                                turnId,
                                itemId,
                                raw: { chunk: textChunk },
                              }),
                              type: "content.delta",
                              payload: { streamKind: "assistant_text", delta: textChunk },
                            } as any),
                          );
                        });

                        child.stderr.on("data", (data: Buffer) => {
                          console.error(`Gemini CLI stderr: ${data}`);
                        });

                        child.on("close", (code: number | null) => {
                          if (code !== 0 && code !== null) {
                            reject(new Error(`Gemini CLI exited with code ${code}`));
                          } else {
                            resolve();
                          }
                        });

                        child.on("error", (err: Error) => {
                          reject(err);
                        });
                      }),
                    catch: (err) => (err instanceof Error ? err : new Error(String(err))),
                  });
                } else {
                  const baseUrl =
                    geminiConfig?.apiEndpoint || "https://generativelanguage.googleapis.com/v1beta";
                  const model =
                    input.modelSelection?.model || context.session.model || "gemini-3.1-pro";
                  const url = `${baseUrl}/models/${model}:streamGenerateContent?alt=sse`;

                  const response = yield* Effect.tryPromise({
                    try: () =>
                      fetch(url, {
                        method: "POST",
                        headers: { 
                          "Content-Type": "application/json",
                          "x-goog-api-key": apiKey
                        },
                        body: JSON.stringify({
                          contents: [{ role: "user", parts: [{ text: prompt }] }],
                        }),
                        signal: context.activeTurnController?.signal || null,
                      }),
                    catch: (err) => new Error(String(err)),
                  });

                  if (!response.ok) {
                    const errText = yield* Effect.tryPromise(() => response.text());
                    return yield* Effect.fail(new Error(`Gemini API Error ${response.status}: ${errText}`));
                  }

                  if (!response.body) return yield* Effect.fail(new Error("No response body"));
                  const body = response.body!;

                  const reader = body.getReader();
                  const decoder = new TextDecoder();
                  let done = false;

                  while (!done) {
                    const { value, done: isDone } = yield* Effect.tryPromise(() => reader.read());
                    done = isDone;
                    if (value) {
                      const chunk = decoder.decode(value, { stream: true });
                      const lines = chunk.split("\n");
                      for (const line of lines) {
                        if (line.startsWith("data: ") && line.length > 6) {
                          const data = line.slice(6);
                          if (data === "[DONE]") continue;
                          try {
                            const parsed = JSON.parse(data);
                            const textChunk = parsed.candidates?.[0]?.content?.parts?.[0]?.text;
                            if (textChunk) {
                              fullText += textChunk;
                              yield* emit({
                                ...buildEventBase({
                                  threadId: input.threadId,
                                  turnId,
                                  itemId,
                                  raw: parsed,
                                }),
                                type: "content.delta",
                                payload: { streamKind: "assistant_text", delta: textChunk },
                              });
                            }
                          } catch (e) {
                            // ignore parse error
                          }
                        }
                      }
                    }
                  }
                }

                yield* emit({
                  ...buildEventBase({ threadId: input.threadId, turnId, itemId }),
                  type: "item.completed",
                  payload: { itemType: "assistant_message", status: "completed", detail: fullText },
                });

                yield* emit({
                  ...buildEventBase({ threadId: input.threadId, turnId }),
                  type: "turn.completed",
                  payload: {},
                } as any);
              });

              const result = yield* Effect.exit(execution);
              if (Exit.isFailure(result)) {
                const error = Cause.squash(result.cause);
                const isAbort =
                  (error instanceof Error && error.name === "AbortError") ||
                  (error instanceof Error && error.cause && (error.cause as Error).name === "AbortError") ||
                  (error instanceof Error && error.message.includes("abort")) ||
                  (error instanceof Error && error.message.includes("AbortError"));

                if (isAbort) {
                  if (currentItemId) {
                    yield* emit({
                      ...buildEventBase({
                        threadId: input.threadId,
                        turnId,
                        itemId: currentItemId,
                      }),
                      type: "item.completed",
                      payload: {
                        itemType: "assistant_message",
                        status: "completed",
                      },
                    });
                  }
                  yield* emit({
                    ...buildEventBase({ threadId: input.threadId, turnId }),
                    type: "turn.aborted",
                    payload: { reason: "User cancelled" },
                  });
                  yield* emit({
                    ...buildEventBase({ threadId: input.threadId, turnId }),
                    type: "turn.completed",
                    payload: { state: "cancelled" },
                  });
                } else {
                  const errorMessage =
                    error instanceof Error ? error.message : String(error) || "Unknown error";
                  yield* emit({
                    ...buildEventBase({ threadId: input.threadId, turnId }),
                    type: "runtime.error",
                    payload: { message: errorMessage, class: "transport_error" },
                  });
                  yield* emit({
                    ...buildEventBase({ threadId: input.threadId, turnId }),
                    type: "turn.completed",
                    payload: { state: "failed", errorMessage: errorMessage },
                  });
                }
              }
            }).pipe(
              Effect.provideService(ServerConfig, serverConfig),
              Effect.forkIn(context.sessionScope),
            );

            return { threadId: input.threadId, turnId } as const;
          }).pipe(
            Effect.provideService(ServerSettingsService, serverSettings),
            Effect.provideService(ServerConfig, serverConfig),
          ),

        interruptTurn: (threadId, turnId) =>
          Effect.gen(function* () {
            const context = sessions.get(threadId);
            if (context && (!turnId || context.activeTurnId === turnId)) {
              context.activeTurnController?.abort();
            }
          }),

        respondToRequest: () => Effect.void,
        respondToUserInput: () => Effect.void,

        stopSession: (threadId) =>
          Effect.gen(function* () {
            const context = sessions.get(threadId);
            if (context) {
              yield* stopGeminiContext(context);
              sessions.delete(threadId);
            }
          }),

        listSessions: () => Effect.succeed(Array.from(sessions.values()).map((c) => c.session)),

        hasSession: (threadId) => Effect.succeed(sessions.has(threadId)),

        readThread: (threadId) =>
          Effect.gen(function* () {
            const context = sessions.get(threadId);
            if (!context) {
              return yield* new ProviderAdapterSessionNotFoundError({
                provider: PROVIDER,
                threadId,
              });
            }
            return { threadId, turns: context.turns };
          }),

        rollbackThread: (threadId, numTurns) =>
          Effect.gen(function* () {
            const context = sessions.get(threadId);
            if (!context) {
              return yield* new ProviderAdapterSessionNotFoundError({
                provider: PROVIDER,
                threadId,
              });
            }
            context.turns.splice(-numTurns, numTurns);
            return { threadId, turns: context.turns };
          }),

        stopAll: () =>
          Effect.gen(function* () {
            const contexts = [...sessions.values()];
            sessions.clear();
            yield* Effect.forEach(
              contexts,
              (context) => Effect.ignoreCause(stopGeminiContext(context)),
              { concurrency: "unbounded", discard: true },
            );
          }),
      });
    }),
  );
}

export const GeminiAdapterLive = makeGeminiAdapterLive();
