import { Effect, Layer, Duration, Option } from "effect";
import { CommandId, MessageId, ThreadId, TurnId } from "@kd/contracts";
import { AgentExecutionQueue } from "../../persistence/Services/AgentExecutionQueue.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { fetchRAGContext } from "../../provider/Layers/RAGContext.ts";
import { EmbeddingProvider } from "../Layers/EmbeddingProvider.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../../config.ts";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { runProcess } from "../../processRunner.ts";

/**
 * Allowed shell commands for EXECUTE_TOOLS jobs.
 * Only commands in this set are permitted for execution.
 * Each entry is the base command name (not the full path).
 */
const ALLOWED_BASH_COMMANDS = new Set([
  "ls", "cat", "head", "tail", "wc", "grep", "find", "echo",
  "pwd", "date", "whoami", "env", "printenv",
  "git", "npm", "npx", "bun", "node", "tsc", "tsx",
  "mkdir", "cp", "mv", "touch", "rm",
  "curl", "wget",
]);

/**
 * Shell metacharacters that indicate command chaining/injection.
 * If any of these are present, the command is rejected.
 */
const SHELL_INJECTION_PATTERN = /[;&|`$(){}]/;

/**
 * Validates that a file path resolves within the project's workspace root.
 * Prevents path traversal attacks (e.g., writing to /etc/crontab).
 */
function validatePathWithinWorkspace(filePath: string, workspaceRoot: string): string | null {
  const resolved = path.resolve(workspaceRoot, filePath);
  const normalizedRoot = path.resolve(workspaceRoot);
  if (!resolved.startsWith(normalizedRoot + path.sep) && resolved !== normalizedRoot) {
    return null;
  }
  return resolved;
}

/**
 * Validates a shell command against the allowlist.
 * Returns null if the command is rejected.
 */
function validateShellCommand(command: string): { valid: true; reason?: undefined } | { valid: false; reason: string } {
  if (!command || typeof command !== "string") {
    return { valid: false, reason: "Empty or non-string command." };
  }

  const trimmed = command.trim();
  if (trimmed.length === 0) {
    return { valid: false, reason: "Empty command." };
  }

  // Extract the base command (first token)
  const baseCommand = trimmed.split(/\s+/)[0];
  if (!baseCommand) {
    return { valid: false, reason: "Could not parse base command." };
  }

  // Strip any path prefix to get the executable name
  const execName = path.basename(baseCommand);

  if (!ALLOWED_BASH_COMMANDS.has(execName)) {
    return { valid: false, reason: `Command '${execName}' is not in the allowlist. Allowed: ${[...ALLOWED_BASH_COMMANDS].join(", ")}` };
  }

  // Check for shell injection metacharacters
  // Allow pipes (|) for simple command chaining but block dangerous operators
  const dangerousPattern = /[;&`$(){}]/;
  if (dangerousPattern.test(trimmed)) {
    return { valid: false, reason: `Command contains disallowed shell metacharacters: ${trimmed.match(dangerousPattern)?.[0]}` };
  }

  return { valid: true };
}

export const AgentExecutionEngineLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const queue = yield* AgentExecutionQueue;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const sqlOption = yield* Effect.serviceOption(SqlClient.SqlClient);
    const sql = Option.getOrNull(sqlOption);
    const embeddingProvider = yield* EmbeddingProvider;
    const serverConfig = yield* ServerConfig;

    /** Resolve the workspace root for a given thread from its project. */
    const resolveWorkspaceRoot = (threadId: ThreadId): Effect.Effect<string | null> =>
      Effect.gen(function* () {
        if (!sql) return serverConfig.cwd;
        const rows = yield* sql<{ workspace_root: string }>`
          SELECT pp.workspace_root
          FROM projection_threads pt
          JOIN projection_projects pp ON pt.project_id = pp.project_id
          WHERE pt.thread_id = ${threadId}
          LIMIT 1
        `.pipe(Effect.catchAll(() => Effect.succeed([])));
        return rows[0]?.workspace_root ?? serverConfig.cwd;
      }).pipe(Effect.catchAll(() => Effect.succeed(serverConfig.cwd)));

    yield* Effect.logInfo("Starting AgentExecutionEngine worker loop...");

    yield* Effect.forkScoped(
      Effect.gen(function* () {
        while (true) {
          const jobOpt = yield* queue.take();
          if (Option.isSome(jobOpt)) {
            const job = jobOpt.value.job;
            const jobId = job.jobId;
            yield* Effect.logInfo(`[AgentExecutionEngine] Picked up job ${jobId} of type ${job.jobType} for thread ${job.threadId}`);
            
            // Process the job, then complete it — all within the same fiber.
            // This ensures the job is only deleted from the queue after processing finishes.
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

                let providerId = "external";
                let modelId = "";
                if (sql) {
                  const threadRows = yield* sql<{ model_selection_json: any }>`SELECT model_selection_json FROM projection_threads WHERE thread_id = ${threadId} LIMIT 1`.pipe(
                    Effect.catchTag("SqlError", () => Effect.succeed([] as { model_selection_json: any }[]))
                  );
                  const firstRow = threadRows[0];
                  if (firstRow && firstRow.model_selection_json) {
                    const ms = typeof firstRow.model_selection_json === 'string' 
                      ? JSON.parse(firstRow.model_selection_json) 
                      : firstRow.model_selection_json;
                    providerId = ms?.provider || ms?.providerId || "external";
                    modelId = ms?.model || "";
                  }
                }

                // Connect to External Agent via SSE streaming endpoint
                let agentUrl = process.env.EXTERNAL_AGENT_URL || "http://localhost:11440/api/stream";
                const combinedId = `${providerId}_${modelId}`.toLowerCase();
                
                if (combinedId.includes("openclaw") || providerId === "openclaw") {
                  agentUrl = process.env.OPENCLAW_AGENT_URL || "http://localhost:5440/api/stream";
                } else if (combinedId.includes("hermes") || providerId === "hermes") {
                  agentUrl = process.env.HERMES_AGENT_URL || "http://localhost:5442/api/stream";
                }

                const fetchResult = yield* Effect.tryPromise({
                  try: async () => {
                    const signal = AbortSignal.timeout(300000);
                    const res = await fetch(agentUrl, {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({ taskRequest: prompt }),
                      signal,
                    });
                    return res;
                  },
                  catch: (err) => new Error(`Agent fetch failed: ${err}`),
                });

                if (!fetchResult.ok) {
                  const errorText = yield* Effect.tryPromise(() => fetchResult.text()).pipe(
                    Effect.catchAll(() => Effect.succeed("(unreadable)"))
                  );
                  return yield* Effect.fail(new Error(`Agent API returned ${fetchResult.status}: ${errorText}`));
                }

                const reader = fetchResult.body?.getReader();
                if (!reader) {
                  return yield* Effect.fail(new Error("No readable stream in response"));
                }

                const decoder = new TextDecoder("utf-8");
                let buffer = "";

                yield* Effect.gen(function* () {
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
                }).pipe(
                  Effect.catchAll((streamErr) =>
                    Effect.gen(function* () {
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
                    })
                  ),
                );

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
                const threadId = job.threadId as ThreadId;
                const workspaceRoot = (yield* resolveWorkspaceRoot(threadId)) || serverConfig.cwd;
                
                let toolResult = "";
                try {
                  if (toolName === "bash") {
                    const command = (toolInput as any)?.command || (toolInput as any)?.script || "";
                    const validation = validateShellCommand(command);
                    if (!validation.valid) {
                      toolResult = `[SECURITY] Command rejected: ${validation.reason}`;
                      yield* Effect.logWarning(`[AgentExecutionEngine] Shell command rejected`, { command, reason: validation.reason, threadId });
                    } else {
                      const res = yield* Effect.tryPromise(() => 
                        runProcess("bash", ["-c", command], { 
                          timeoutMs: 30000, 
                          allowNonZeroExit: true,
                          cwd: workspaceRoot,
                        })
                      );
                      toolResult = res.stdout + (res.stderr ? `\nError Output:\n${res.stderr}` : "");
                    }
                  } else if (toolName === "edit_file" || toolName === "write_to_file") {
                    const rawPath = (toolInput as any)?.path || (toolInput as any)?.file || "";
                    const content = (toolInput as any)?.content || (toolInput as any)?.code || "";
                    const safePath = validatePathWithinWorkspace(rawPath, workspaceRoot);
                    if (!safePath) {
                      toolResult = `[SECURITY] Path rejected: '${rawPath}' resolves outside workspace root '${workspaceRoot}'.`;
                      yield* Effect.logWarning(`[AgentExecutionEngine] File write path rejected`, { rawPath, workspaceRoot, threadId });
                    } else {
                      // Ensure parent directory exists
                      const parentDir = path.dirname(safePath);
                      yield* Effect.tryPromise(() => fs.mkdir(parentDir, { recursive: true })).pipe(
                        Effect.catchAll(() => Effect.succeed(undefined as void))
                      );
                      yield* Effect.tryPromise(() => fs.writeFile(safePath, content, "utf-8"));
                      toolResult = `Successfully wrote to ${safePath}`;
                    }
                  } else if (toolName === "read_file") {
                    const rawPath = (toolInput as any)?.path || (toolInput as any)?.file || "";
                    const safePath = validatePathWithinWorkspace(rawPath, workspaceRoot);
                    if (!safePath) {
                      toolResult = `[SECURITY] Path rejected: '${rawPath}' resolves outside workspace root '${workspaceRoot}'.`;
                      yield* Effect.logWarning(`[AgentExecutionEngine] File read path rejected`, { rawPath, workspaceRoot, threadId });
                    } else {
                      toolResult = yield* Effect.tryPromise(() => fs.readFile(safePath, "utf-8"));
                    }
                  } else {
                    toolResult = `Tool '${toolName}' is not supported. Supported tools: bash, edit_file, write_to_file, read_file.`;
                  }
                } catch (e: any) {
                  toolResult = `Tool execution failed: ${e.message}`;
                }

                yield* orchestrationEngine.dispatch({
                  type: "thread.message.assistant.delta",
                  commandId: CommandId.make(crypto.randomUUID()),
                  threadId,
                  messageId: messageId || crypto.randomUUID(),
                  delta: `\n\n\`\`\`\n[Tool Output: ${toolName}]\n${toolResult}\n\`\`\`\n`,
                  turnId,
                  createdAt: new Date().toISOString(),
                });
              }

              // Job completed successfully — now safe to remove from queue
              yield* queue.complete(jobId);
            }).pipe(
              Effect.catchCause((cause) => 
                Effect.logError(`[AgentExecutionEngine] Job ${jobId} failed`, cause).pipe(
                  // Even on failure, mark the job complete to prevent infinite retry loops.
                  // A dead-letter queue or retry-with-backoff strategy should replace this.
                  Effect.flatMap(() => queue.complete(jobId).pipe(Effect.ignoreCause({ log: true })))
                )
              ),
            );
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
