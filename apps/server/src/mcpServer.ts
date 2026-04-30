import { Effect, Stream, Queue, Runtime } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { GitStatusBroadcaster } from "./git/Services/GitStatusBroadcaster.ts";

// Effect-native SSE Transport implementation
class EffectSseTransport {
  public onclose?: () => void;
  public onerror?: (error: Error) => void;
  public onmessage?: (message: JSONRPCMessage) => void;

  constructor(
    public readonly sessionId: string,
    private readonly queue: Queue.Queue<string>
  ) {}

  async start(): Promise<void> {}

  async send(message: JSONRPCMessage): Promise<void> {
    const data = `event: message\ndata: ${JSON.stringify(message)}\n\n`;
    await Effect.runPromise(Queue.offer(this.queue, data));
  }

  async close(): Promise<void> {
    await Effect.runPromise(Queue.shutdown(this.queue));
    this.onclose?.();
  }
}

// Global active transports map
const activeTransports = new Map<string, EffectSseTransport>();

function createMcpServerForTransport(transport: EffectSseTransport) {
  return Effect.gen(function* () {
    const gitStatusBroadcaster = yield* GitStatusBroadcaster;

    const server = new Server(
      { name: "krusch-dbos", version: "1.0.0" },
      { capabilities: { tools: {}, resources: {} } }
    );

    server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: "get_git_status",
            description: "Get the current git status of the project",
            inputSchema: {
              type: "object",
              properties: { cwd: { type: "string" } },
              required: ["cwd"]
            }
          }
        ]
      };
    });

    server.setRequestHandler(CallToolRequestSchema, async (request) => {
      if (request.params.name === "get_git_status") {
        const cwd = request.params.arguments?.cwd as string;
        try {
          const status = await Effect.runPromise(gitStatusBroadcaster.refreshStatus(cwd));
          return {
            content: [{ type: "text", text: JSON.stringify(status) }]
          };
        } catch (error: any) {
          return {
            content: [{ type: "text", text: `Error: ${error?.message || String(error)}` }],
            isError: true
          };
        }
      }
      throw new Error("Tool not found");
    });

    // server.connect returns a promise, we must run it in Effect
    yield* Effect.promise(() => server.connect(transport).catch(console.error));
    return server;
  });
}

export const mcpSseRouteLayer = HttpRouter.add(
  "GET",
  "/mcp/sse",
  Effect.gen(function* () {
    const sessionId = crypto.randomUUID();
    const queue = yield* Queue.unbounded<string>();
    const transport = new EffectSseTransport(sessionId, queue);
    
    activeTransports.set(sessionId, transport);
    yield* createMcpServerForTransport(transport);

    yield* Queue.offer(
      queue,
      `event: endpoint\ndata: /mcp/messages?sessionId=${sessionId}\n\n`
    );

    const stream = Stream.fromQueue(queue).pipe(
      Stream.map((s) => new TextEncoder().encode(s)),
      Stream.ensuring(Effect.sync(() => {
        activeTransports.delete(sessionId);
      }))
    );

    return HttpServerResponse.stream(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive"
      }
    });
  })
);

export const mcpMessagesRouteLayer = HttpRouter.add(
  "POST",
  "/mcp/messages",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    // URL parsing from request
    const urlStr = request.url;
    // Extract sessionId from search params
    const dummyUrl = new URL(urlStr.startsWith("/") ? `http://localhost${urlStr}` : urlStr);
    const sessionId = dummyUrl.searchParams.get("sessionId");
    
    if (!sessionId) {
      return HttpServerResponse.text("Missing sessionId", { status: 400 });
    }

    const transport = activeTransports.get(sessionId);
    if (!transport) {
      return HttpServerResponse.text("Session not found", { status: 404 });
    }

    const body = yield* request.json;
    if (transport.onmessage) {
      transport.onmessage(body as JSONRPCMessage);
    }

    return HttpServerResponse.text("Accepted", { status: 202 });
  })
);
