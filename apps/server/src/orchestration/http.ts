import {
  ClientOrchestrationCommand,
  OrchestrationDispatchCommandError,
  OrchestrationGetSnapshotError,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import { Effect, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import { ServerAuth } from "../auth/Services/ServerAuth.ts";
import { normalizeDispatchCommand } from "./Normalizer.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../config.ts";

const respondToOrchestrationHttpError = (
  error: OrchestrationDispatchCommandError | OrchestrationGetSnapshotError,
) =>
  Effect.gen(function* () {
    if (error._tag === "OrchestrationGetSnapshotError") {
      yield* Effect.logError("orchestration http route failed", {
        message: error.message,
        cause: error.cause,
      });
      return HttpServerResponse.jsonUnsafe({ error: error.message }, { status: 500 });
    }

    return HttpServerResponse.jsonUnsafe({ error: error.message }, { status: 400 });
  });

const authenticateOwnerSession = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const serverAuth = yield* ServerAuth;
  const session = yield* serverAuth.authenticateHttpRequest(request);
  if (session.role !== "owner") {
    return yield* new OrchestrationDispatchCommandError({
      message: "Only owner sessions can manage projects.",
    });
  }
  return session;
});

export const orchestrationSnapshotRouteLayer = HttpRouter.add(
  "GET",
  "/api/orchestration/snapshot",
  Effect.gen(function* () {
    yield* authenticateOwnerSession;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const snapshot = yield* projectionSnapshotQuery.getSnapshot().pipe(
      Effect.mapError(
        (cause) =>
          new OrchestrationGetSnapshotError({
            message: "Failed to load orchestration snapshot.",
            cause,
          }),
      ),
    );
    return HttpServerResponse.jsonUnsafe(snapshot satisfies OrchestrationReadModel, {
      status: 200,
    });
  }).pipe(
    Effect.catchTag("OrchestrationDispatchCommandError", (err) => {
      console.error("DISPATCH CAUSE:", err.cause);
      return respondToOrchestrationHttpError(err);
    }),
    Effect.catchTag("OrchestrationGetSnapshotError", respondToOrchestrationHttpError),
  ),
);

export const orchestrationDispatchRouteLayer = HttpRouter.add(
  "POST",
  "/api/orchestration/dispatch",
  Effect.gen(function* () {
    yield* authenticateOwnerSession;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const command = yield* HttpServerRequest.schemaBodyJson(ClientOrchestrationCommand).pipe(
      Effect.mapError((cause) => {
        console.error(cause.message);
        return new OrchestrationDispatchCommandError({
          message: "Invalid orchestration command payload.",
          cause,
        });
      }),
    );
    const normalizedCommand = yield* normalizeDispatchCommand(command);
    const result = yield* orchestrationEngine.dispatch(normalizedCommand).pipe(
      Effect.mapError(
        (cause) =>
          new OrchestrationDispatchCommandError({
            message: "Failed to dispatch orchestration command.",
            cause,
          }),
      ),
    );
    return HttpServerResponse.jsonUnsafe(result, { status: 200 });
  }).pipe(
    Effect.catchTag("OrchestrationDispatchCommandError", (err) => {
      console.error("DISPATCH CAUSE:", err.cause);
      return respondToOrchestrationHttpError(err);
    }),
  ),
);

export const orchestrationVectorSearchRouteLayer = HttpRouter.add(
  "POST",
  "/api/orchestration/vector-search",
  Effect.gen(function* () {
    yield* authenticateOwnerSession;

    // Read JSON body: { query: string }
    const body = yield* HttpServerRequest.schemaBodyJson(
      Schema.Struct({ query: Schema.String }),
    ).pipe(
      Effect.mapError(
        (cause) => new OrchestrationDispatchCommandError({ message: "Invalid payload.", cause }),
      ),
    );

    const config = yield* Effect.service(ServerConfig);
    const OLLAMA_URL = config.ollamaUrl;

    const response = yield* Effect.tryPromise({
      try: () =>
        fetch(OLLAMA_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ model: "nomic-embed-text", prompt: body.query }),
        }),
      catch: (cause) =>
        new OrchestrationDispatchCommandError({
          message: "Ollama embedding failed.",
          cause: cause as Error,
        }),
    });

    if (!response.ok) {
      return yield* new OrchestrationDispatchCommandError({
        message: `Ollama error: ${response.status}`,
      });
    }

    const responseJson = yield* Effect.tryPromise({
      try: () => response.json(),
      catch: (cause) =>
        new OrchestrationDispatchCommandError({
          message: "Ollama json parsing failed.",
          cause: cause as Error,
        }),
    });

    const json = responseJson as { embedding: number[] };
    const embedding = json.embedding;
    if (!Array.isArray(embedding)) {
      return yield* new OrchestrationDispatchCommandError({ message: "Ollama response invalid." });
    }

    const vectorStr = `[${embedding.join(",")}]`;
    const sql = yield* Effect.service(SqlClient.SqlClient);

    // Top 5 similar
    const results = yield* sql<{ event_id: string; content: string; distance: number }>`
      SELECT event_id, content, embedding <=> ${vectorStr}::vector AS distance
      FROM orchestration_event_embeddings
      ORDER BY distance ASC
      LIMIT 5
    `.pipe(
      Effect.mapError(
        (cause) =>
          new OrchestrationDispatchCommandError({ message: "Database query failed.", cause }),
      ),
    );

    return HttpServerResponse.jsonUnsafe({ results }, { status: 200 });
  }).pipe(
    Effect.catchTag("OrchestrationDispatchCommandError", (err) => {
      console.error("DISPATCH CAUSE:", err.cause);
      return respondToOrchestrationHttpError(err);
    }),
  ),
);
