import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import { Effect, Layer, Option } from "effect";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  GetProjectionPendingApprovalInput,
  DeleteProjectionPendingApprovalInput,
  ListProjectionPendingApprovalsInput,
  ProjectionPendingApproval,
  ProjectionPendingApprovalRepository,
  type ProjectionPendingApprovalRepositoryShape,
} from "../Services/ProjectionPendingApprovals.ts";

import { ProjectionThreadRepository } from "../Services/ProjectionThreads.ts";
import { ProjectionThreadRepositoryLive } from "./ProjectionThreads.ts";
import { ProjectionProjectRepository } from "../Services/ProjectionProjects.ts";
import { ProjectionProjectRepositoryLive } from "./ProjectionProjects.ts";
import { ThreadId } from "@t3tools/contracts";

const makeProjectionPendingApprovalRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threadRepo = yield* ProjectionThreadRepository;
  const projectRepo = yield* ProjectionProjectRepository;

  const upsertProjectionPendingApprovalRow = SqlSchema.void({
    Request: ProjectionPendingApproval,
    execute: (row) =>
      sql`
        INSERT INTO projection_pending_approvals (
          request_id,
          thread_id,
          turn_id,
          status,
          decision,
          created_at,
          resolved_at
        )
        VALUES (
          ${row.requestId},
          ${row.threadId},
          ${row.turnId},
          ${row.status},
          ${row.decision},
          ${row.createdAt},
          ${row.resolvedAt}
        )
        ON CONFLICT (request_id)
        DO UPDATE SET
          thread_id = excluded.thread_id,
          turn_id = excluded.turn_id,
          status = excluded.status,
          decision = excluded.decision,
          created_at = excluded.created_at,
          resolved_at = excluded.resolved_at
      `,
  });

  const listProjectionPendingApprovalRows = SqlSchema.findAll({
    Request: ListProjectionPendingApprovalsInput,
    Result: ProjectionPendingApproval,
    execute: ({ threadId }) =>
      sql`
        SELECT
          request_id AS "requestId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          status,
          decision,
          created_at AS "createdAt",
          resolved_at AS "resolvedAt"
        FROM projection_pending_approvals
        WHERE thread_id = ${threadId}
        ORDER BY created_at ASC, request_id ASC
      `,
  });

  const getProjectionPendingApprovalRow = SqlSchema.findOneOption({
    Request: GetProjectionPendingApprovalInput,
    Result: ProjectionPendingApproval,
    execute: ({ requestId }) =>
      sql`
        SELECT
          request_id AS "requestId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          status,
          decision,
          created_at AS "createdAt",
          resolved_at AS "resolvedAt"
        FROM projection_pending_approvals
        WHERE request_id = ${requestId}
      `,
  });

  const deleteProjectionPendingApprovalRow = SqlSchema.void({
    Request: DeleteProjectionPendingApprovalInput,
    execute: ({ requestId }) =>
      sql`
        DELETE FROM projection_pending_approvals
        WHERE request_id = ${requestId}
      `,
  });



  const upsert: ProjectionPendingApprovalRepositoryShape["upsert"] = (row) =>
    Effect.gen(function* () {
      // 1. Write to PostgreSQL
      yield* upsertProjectionPendingApprovalRow(row).pipe(
        Effect.mapError(
          toPersistenceSqlError("ProjectionPendingApprovalRepository.upsert:postgres"),
        ),
      );


    });

  const listByThreadId: ProjectionPendingApprovalRepositoryShape["listByThreadId"] = (input) =>
    listProjectionPendingApprovalRows(input).pipe(
      Effect.mapError(
        toPersistenceSqlError("ProjectionPendingApprovalRepository.listByThreadId:postgres"),
      ),
    );

  const getByRequestId: ProjectionPendingApprovalRepositoryShape["getByRequestId"] = (input) =>
    getProjectionPendingApprovalRow(input).pipe(
      Effect.mapError(
        toPersistenceSqlError("ProjectionPendingApprovalRepository.getByRequestId:postgres"),
      ),
    );

  const deleteByRequestId: ProjectionPendingApprovalRepositoryShape["deleteByRequestId"] = (
    input,
  ) =>
    Effect.gen(function* () {
      // 1. Delete from PostgreSQL
      const rowOpt = yield* getByRequestId(input);
      yield* deleteProjectionPendingApprovalRow(input).pipe(
        Effect.mapError(
          toPersistenceSqlError("ProjectionPendingApprovalRepository.deleteByRequestId:postgres"),
        ),
      );


    });

  return {
    upsert,
    listByThreadId,
    getByRequestId,
    deleteByRequestId,
  } satisfies ProjectionPendingApprovalRepositoryShape;
});

export const ProjectionPendingApprovalRepositoryLive = Layer.effect(
  ProjectionPendingApprovalRepository,
  makeProjectionPendingApprovalRepository,
).pipe(
  Layer.provideMerge(ProjectionThreadRepositoryLive),
  Layer.provideMerge(ProjectionProjectRepositoryLive),
);
