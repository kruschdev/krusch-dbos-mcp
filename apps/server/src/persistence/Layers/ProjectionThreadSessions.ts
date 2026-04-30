import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import { Effect, Layer, Option } from "effect";

import { toPersistenceSqlError } from "../Errors.ts";

import {
  ProjectionThreadSession,
  ProjectionThreadSessionRepository,
  type ProjectionThreadSessionRepositoryShape,
  DeleteProjectionThreadSessionInput,
  GetProjectionThreadSessionInput,
} from "../Services/ProjectionThreadSessions.ts";
import { MessageId, ThreadId } from "@t3tools/contracts";
import { ProjectionThreadRepository } from "../Services/ProjectionThreads.ts";
import { ProjectionThreadRepositoryLive } from "./ProjectionThreads.ts";
import { ProjectionProjectRepository } from "../Services/ProjectionProjects.ts";
import { ProjectionProjectRepositoryLive } from "./ProjectionProjects.ts";

const makeProjectionThreadSessionRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threadRepo = yield* ProjectionThreadRepository;
  const projectRepo = yield* ProjectionProjectRepository;

  const upsertProjectionThreadSessionRow = SqlSchema.void({
    Request: ProjectionThreadSession,
    execute: (row) =>
      sql`
        INSERT INTO projection_thread_sessions (
          thread_id,
          status,
          provider_name,
          runtime_mode,
          active_turn_id,
          last_error,
          updated_at
        )
        VALUES (
          ${row.threadId},
          ${row.status},
          ${row.providerName},
          ${row.runtimeMode},
          ${row.activeTurnId},
          ${row.lastError},
          ${row.updatedAt}
        )
        ON CONFLICT (thread_id)
        DO UPDATE SET
          status = excluded.status,
          provider_name = excluded.provider_name,
          runtime_mode = excluded.runtime_mode,
          active_turn_id = excluded.active_turn_id,
          last_error = excluded.last_error,
          updated_at = excluded.updated_at
      `,
  });

  const getProjectionThreadSessionRow = SqlSchema.findOneOption({
    Request: GetProjectionThreadSessionInput,
    Result: ProjectionThreadSession,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          status,
          provider_name AS "providerName",
          runtime_mode AS "runtimeMode",
          active_turn_id AS "activeTurnId",
          last_error AS "lastError",
          updated_at AS "updatedAt"
        FROM projection_thread_sessions
        WHERE thread_id = ${threadId}
      `,
  });

  const deleteProjectionThreadSessionRow = SqlSchema.void({
    Request: DeleteProjectionThreadSessionInput,
    execute: ({ threadId }) =>
      sql`
        DELETE FROM projection_thread_sessions
        WHERE thread_id = ${threadId}
      `,
  });



  const upsert: ProjectionThreadSessionRepositoryShape["upsert"] = (row) =>
    Effect.gen(function* () {
      // 1. Write to PostgreSQL
      yield* upsertProjectionThreadSessionRow(row).pipe(
        Effect.mapError(toPersistenceSqlError("ProjectionThreadSessionRepository.upsert:postgres")),
      );


    });

  const getByThreadId: ProjectionThreadSessionRepositoryShape["getByThreadId"] = (input) =>
    getProjectionThreadSessionRow(input).pipe(
      Effect.mapError(
        toPersistenceSqlError("ProjectionThreadSessionRepository.getByThreadId:postgres"),
      ),
    );

  const deleteByThreadId: ProjectionThreadSessionRepositoryShape["deleteByThreadId"] = (input) =>
    Effect.gen(function* () {
      // 1. Delete from PostgreSQL
      yield* deleteProjectionThreadSessionRow(input).pipe(
        Effect.mapError(
          toPersistenceSqlError("ProjectionThreadSessionRepository.deleteByThreadId:postgres"),
        ),
      );


    });

  return {
    upsert,
    getByThreadId,
    deleteByThreadId,
  } satisfies ProjectionThreadSessionRepositoryShape;
});

export const ProjectionThreadSessionRepositoryLive = Layer.effect(
  ProjectionThreadSessionRepository,
  makeProjectionThreadSessionRepository,
).pipe(
  Layer.provideMerge(ProjectionThreadRepositoryLive),
  Layer.provideMerge(ProjectionProjectRepositoryLive),
);
