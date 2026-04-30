import { Effect, Layer, Duration, Option } from "effect";
import { CommandId, MessageId, ThreadId, TurnId } from "@t3tools/contracts";
import { AgentExecutionQueue } from "../../persistence/Services/AgentExecutionQueue.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { fetchRAGContext } from "../../provider/Layers/RAGContext.ts";
import { EmbeddingProvider } from "../Layers/EmbeddingProvider.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../../config.ts";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import { runProcess } from "../../processRunner.ts";

export const AgentExecutionEngineLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const queue = yield* AgentExecutionQueue;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const sqlOption = yield* Effect.serviceOption(SqlClient.SqlClient);
    const sql = Option.getOrNull(sqlOption);
    const embeddingProvider = yield* EmbeddingProvider;
    const serverConfig = yield* ServerConfig;

    yield* Effect.logInfo("Starting AgentExecutionEngine worker loop...");

    yield* Effect.forkScoped(
      Effect.gen(function* () {
        while (true) {
          const jobOpt = yield* queue.take();
          if (Option.isSome(jobOpt)) {
            const job = jobOpt.value.job;
            yield* Effect.logInfo(`[AgentExecutionEngine] Picked up job ${job.jobId} of type ${job.jobType} for thread ${job.threadId}`);
            
            yield* Effect.gen(function* () {
              if (job.jobType === "AGENT_STEP") {
                const payload = job.payload as any;
                const turnId = TurnId.make(payload.turnId);
                const threadId = job.threadId as ThreadId;
                let prompt = payload.input || "";

                if (prompt) {
                  const ragContext = yield* fetchRAGContext(prompt, threadId, embeddingProvider, sql);
                  prompt += ragContext;
                }

                const messageId = MessageId.make(`assistant:agent:${turnId}`);

                // Connect to External Agent via SSE streaming endpoint
                const agentUrl = process.env.EXTERNAL_AGENT_URL || "http://localhost:11440/api/stream";

                const fetchResult = yield* Effect.promise(async () => {
                  try {
                    const signal = AbortSignal.timeout(300000);
                    const res = await fetch(agentUrl, {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({ taskRequest: prompt }),
                      signal,
                    });
                    return { success: true as const, res };
                  } catch (err) {
                    return { success: false as const, error: err };
                  }
                });

                if (!fetchResult.success) throw fetchResult.error;
                const response = fetchResult.res;

                if (!response.ok) {
                  const errorText = yield* Effect.promise(() => response.text());
                  throw new Error(`Agent API returned ${response.status}: ${errorText}`);
                }

                const reader = response.body?.getReader();
                if (!reader) throw new Error("No readable stream in response");

                const decoder = new TextDecoder("utf-8");
                let buffer = "";

                try {
                  while (true) {
                    const { done, value } = yield* Effect.promise(() => reader.read());
                    if (done) break;

                    buffer += decoder.decode(value, { stream: true });
                    const parts = buffer.split("\n\n");
                    buffer = parts.pop() || "";

                    for (const part of parts) {
                      const lines = part.split("\n");
                      for (const line of lines) {
                        if (line.startsWith("data: ")) {
                          const payloadStr = line.slice(6).trim();
                          if (!payloadStr || payloadStr === "{}") continue;

                          try {
                            const data = JSON.parse(payloadStr);
                            const typeStr = (data.type || "info").toUpperCase();
                            const deltaText = `[${typeStr}] ${data.text || JSON.stringify(data)}\n`;

                            yield* orchestrationEngine.dispatch({
                              type: "thread.message.assistant.delta",
                              commandId: CommandId.make(crypto.randomUUID()),
                              threadId,
                              messageId,
                              delta: deltaText,
                              turnId,
                              createdAt: new Date().toISOString(),
                            });
                          } catch (e) {
                            yield* Effect.logWarning(`[AgentExecutionEngine] SSE Parse Warning: ${e}`);
                          }
                        }
                      }
                    }
                  }
                } catch (streamErr) {
                  yield* Effect.logError(`[AgentExecutionEngine] Stream Read Error`, streamErr);
                  yield* orchestrationEngine.dispatch({
                    type: "thread.message.assistant.delta",
                    commandId: CommandId.make(crypto.randomUUID()),
                    threadId,
                    messageId,
                    delta: `\n\n[System: Stream interrupted unexpectedly]`,
                    turnId,
                    createdAt: new Date().toISOString(),
                  });
                }

                yield* orchestrationEngine.dispatch({
                  type: "thread.message.assistant.complete",
                  commandId: CommandId.make(crypto.randomUUID()),
                  threadId,
                  messageId,
                  turnId,
                  createdAt: new Date().toISOString(),
                });

              } else if (job.jobType === "EXECUTE_TOOLS") {
                const payload = job.payload as any;
                const { turnId, toolName, toolInput, messageId } = payload;
                
                let toolResult = "";
                try {
                  if (toolName === "bash" || toolName === "spawn_autonomous_clone") {
                    const command = (toolInput as any).command || (toolInput as any).script;
                    const res = yield* Effect.tryPromise(() => 
                      runProcess("bash", ["-c", command], { timeoutMs: 30000, allowNonZeroExit: true })
                    );
                    toolResult = res.stdout + (res.stderr ? `\nError Output:\n${res.stderr}` : "");
                  } else if (toolName === "edit_file" || toolName === "write_to_file") {
                    const filePath = (toolInput as any).path || (toolInput as any).file;
                    const content = (toolInput as any).content || (toolInput as any).code;
                    yield* Effect.tryPromise(() => fs.writeFile(filePath, content, "utf-8"));
                    toolResult = `Successfully wrote to ${filePath}`;
                  } else if (toolName === "read_file") {
                    const filePath = (toolInput as any).path || (toolInput as any).file;
                    toolResult = yield* Effect.tryPromise(() => fs.readFile(filePath, "utf-8"));
                  } else {
                    toolResult = `Tool ${toolName} is not supported.`;
                  }
                } catch (e: any) {
                  toolResult = `Tool execution failed: ${e.message}`;
                }

                yield* orchestrationEngine.dispatch({
                  type: "thread.message.assistant.delta",
                  commandId: CommandId.make(crypto.randomUUID()),
                  threadId: job.threadId as ThreadId,
                  messageId: messageId || crypto.randomUUID(),
                  delta: `\n\n\`\`\`\n[Tool Output: ${toolName}]\n${toolResult}\n\`\`\`\n`,
                  turnId,
                  createdAt: new Date().toISOString(),
                });
              }
            }).pipe(
              Effect.catchCause((cause) => 
                Effect.logError(`[AgentExecutionEngine] Job ${job.jobId} failed`, cause)
              ),
              Effect.forkScoped
            );

            yield* queue.complete(job.jobId);
          } else {
            yield* Effect.sleep(Duration.millis(100));
          }
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("AgentExecutionEngine fatal error", cause),
        ),
      ),
    );
  })
);
