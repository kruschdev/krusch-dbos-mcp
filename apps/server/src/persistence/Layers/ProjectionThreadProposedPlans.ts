import { ThreadId } from "@t3tools/contracts";
import { Effect, Layer, Option } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  DeleteProjectionThreadProposedPlansInput,
  ListProjectionThreadProposedPlansInput,
  ProjectionThreadProposedPlan,
  ProjectionThreadProposedPlanRepository,
  type ProjectionThreadProposedPlanRepositoryShape,
} from "../Services/ProjectionThreadProposedPlans.ts";
import { ProjectionThreadRepository } from "../Services/ProjectionThreads.ts";
import { ProjectionThreadRepositoryLive } from "./ProjectionThreads.ts";
import { ProjectionProjectRepository } from "../Services/ProjectionProjects.ts";
import { ProjectionProjectRepositoryLive } from "./ProjectionProjects.ts";

const makeProjectionThreadProposedPlanRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threadRepo = yield* ProjectionThreadRepository;
  const projectRepo = yield* ProjectionProjectRepository;

  const upsertProjectionThreadProposedPlanRow = SqlSchema.void({
    Request: ProjectionThreadProposedPlan,
    execute: (row) => sql`
      INSERT INTO projection_thread_proposed_plans (
        plan_id,
        thread_id,
        turn_id,
        plan_markdown,
        implemented_at,
        implementation_thread_id,
        created_at,
        updated_at
      )
      VALUES (
        ${row.planId},
        ${row.threadId},
        ${row.turnId},
        ${row.planMarkdown},
        ${row.implementedAt},
        ${row.implementationThreadId},
        ${row.createdAt},
        ${row.updatedAt}
      )
      ON CONFLICT (plan_id)
      DO UPDATE SET
        thread_id = excluded.thread_id,
        turn_id = excluded.turn_id,
        plan_markdown = excluded.plan_markdown,
        implemented_at = excluded.implemented_at,
        implementation_thread_id = excluded.implementation_thread_id,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at
    `,
  });

  const listProjectionThreadProposedPlanRows = SqlSchema.findAll({
    Request: ListProjectionThreadProposedPlansInput,
    Result: ProjectionThreadProposedPlan,
    execute: ({ threadId }) => sql`
      SELECT
        plan_id AS "planId",
        thread_id AS "threadId",
        turn_id AS "turnId",
        plan_markdown AS "planMarkdown",
        implemented_at AS "implementedAt",
        implementation_thread_id AS "implementationThreadId",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM projection_thread_proposed_plans
      WHERE thread_id = ${threadId}
      ORDER BY created_at ASC, plan_id ASC
    `,
  });

  const deleteProjectionThreadProposedPlanRows = SqlSchema.void({
    Request: DeleteProjectionThreadProposedPlansInput,
    execute: ({ threadId }) => sql`
      DELETE FROM projection_thread_proposed_plans
      WHERE thread_id = ${threadId}
    `,
  });



  const upsert: ProjectionThreadProposedPlanRepositoryShape["upsert"] = (row) =>
    Effect.gen(function* () {
      // 1. Write to PostgreSQL
      yield* upsertProjectionThreadProposedPlanRow(row).pipe(
        Effect.mapError(
          toPersistenceSqlError("ProjectionThreadProposedPlanRepository.upsert:postgres"),
        ),
      );


    });

  const listByThreadId: ProjectionThreadProposedPlanRepositoryShape["listByThreadId"] = (input) =>
    listProjectionThreadProposedPlanRows(input).pipe(
      Effect.mapError(
        toPersistenceSqlError("ProjectionThreadProposedPlanRepository.listByThreadId:postgres"),
      ),
    );

  const deleteByThreadId: ProjectionThreadProposedPlanRepositoryShape["deleteByThreadId"] = (
    input,
  ) =>
    Effect.gen(function* () {
      // 1. Delete from PostgreSQL
      yield* deleteProjectionThreadProposedPlanRows(input).pipe(
        Effect.mapError(
          toPersistenceSqlError("ProjectionThreadProposedPlanRepository.deleteByThreadId:postgres"),
        ),
      );


    });

  return {
    upsert,
    listByThreadId,
    deleteByThreadId,
  } satisfies ProjectionThreadProposedPlanRepositoryShape;
});

export const ProjectionThreadProposedPlanRepositoryLive = Layer.effect(
  ProjectionThreadProposedPlanRepository,
  makeProjectionThreadProposedPlanRepository,
).pipe(
  Layer.provideMerge(ProjectionThreadRepositoryLive),
  Layer.provideMerge(ProjectionProjectRepositoryLive),
);
