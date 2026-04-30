import { Effect, Layer, Option, Schema, Struct } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import { ChatAttachment, ThreadId } from "@t3tools/contracts";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  GetProjectionThreadMessageInput,
  ProjectionThreadMessageRepository,
  type ProjectionThreadMessageRepositoryShape,
  DeleteProjectionThreadMessagesInput,
  ListProjectionThreadMessagesInput,
  ProjectionThreadMessage,
} from "../Services/ProjectionThreadMessages.ts";

import { ProjectionThreadRepository } from "../Services/ProjectionThreads.ts";
import { ProjectionThreadRepositoryLive } from "./ProjectionThreads.ts";
import { ProjectionProjectRepository } from "../Services/ProjectionProjects.ts";
import { ProjectionProjectRepositoryLive } from "./ProjectionProjects.ts";

const ProjectionThreadMessageDbRowSchema = ProjectionThreadMessage.mapFields(
  Struct.assign({
    isStreaming: Schema.Number,
    attachments: Schema.NullOr(Schema.Array(ChatAttachment)),
  }),
);
type ProjectionThreadMessageDbRow = typeof ProjectionThreadMessageDbRowSchema.Type;

function toProjectionThreadMessage(row: ProjectionThreadMessageDbRow): ProjectionThreadMessage {
  return {
    messageId: row.messageId,
    threadId: row.threadId,
    turnId: row.turnId,
    role: row.role,
    text: row.text,
    isStreaming: row.isStreaming === 1,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    ...(row.attachments !== null ? { attachments: row.attachments } : {}),
  };
}

const makeProjectionThreadMessageRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;  const threadRepo = yield* ProjectionThreadRepository;
  const projectRepo = yield* ProjectionProjectRepository;

  const upsertProjectionThreadMessageRow = SqlSchema.void({
    Request: ProjectionThreadMessage,
    execute: (row) =>
      sql`
        INSERT INTO projection_thread_messages (
          message_id, thread_id, turn_id, role, text, attachments_json, is_streaming, created_at, updated_at
        ) VALUES (
          ${row.messageId}, ${row.threadId}, ${row.turnId}, ${row.role}, ${row.text},
          ${row.attachments !== undefined ? JSON.stringify(row.attachments) : null},
          ${row.isStreaming ? 1 : 0}, ${row.createdAt}, ${row.updatedAt}
        )
        ON CONFLICT (message_id) DO UPDATE SET
          thread_id = excluded.thread_id,
          turn_id = excluded.turn_id,
          role = excluded.role,
          text = excluded.text,
          attachments_json = COALESCE(excluded.attachments_json, projection_thread_messages.attachments_json),
          is_streaming = excluded.is_streaming,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at
      `,
  });

  const getProjectionThreadMessageRow = SqlSchema.findOneOption({
    Request: GetProjectionThreadMessageInput,
    Result: ProjectionThreadMessageDbRowSchema,
    execute: ({ messageId }) =>
      sql`
        SELECT
          message_id AS "messageId", thread_id AS "threadId", turn_id AS "turnId",
          role, text, attachments_json AS "attachments", is_streaming AS "isStreaming",
          created_at AS "createdAt", updated_at AS "updatedAt"
        FROM projection_thread_messages
        WHERE message_id = ${messageId}
      `,
  });

  const listProjectionThreadMessageRows = SqlSchema.findAll({
    Request: ListProjectionThreadMessagesInput,
    Result: ProjectionThreadMessageDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          message_id AS "messageId", thread_id AS "threadId", turn_id AS "turnId",
          role, text, attachments_json AS "attachments", is_streaming AS "isStreaming",
          created_at AS "createdAt", updated_at AS "updatedAt"
        FROM projection_thread_messages
        WHERE thread_id = ${threadId}
        ORDER BY created_at ASC, message_id ASC
      `,
  });

  const deleteProjectionThreadMessagesByThreadRow = SqlSchema.void({
    Request: DeleteProjectionThreadMessagesInput,
    execute: ({ threadId }) =>
      sql`
        DELETE FROM projection_thread_messages
        WHERE thread_id = ${threadId}
      `,
  });



  const upsert: ProjectionThreadMessageRepositoryShape["upsert"] = (row) =>
    Effect.gen(function* () {
      // 1. Write to PostgreSQL
      yield* upsertProjectionThreadMessageRow(row).pipe(
        Effect.mapError(toPersistenceSqlError("ProjectionThreadMessageRepository.upsert:postgres")),
      );
    });

  const getByMessageId: ProjectionThreadMessageRepositoryShape["getByMessageId"] = (input) =>
    getProjectionThreadMessageRow(input).pipe(
      Effect.map(Option.map(toProjectionThreadMessage)),
      Effect.mapError(
        toPersistenceSqlError("ProjectionThreadMessageRepository.getByMessageId:postgres"),
      ),
    );

  const listByThreadId: ProjectionThreadMessageRepositoryShape["listByThreadId"] = (input) =>
    listProjectionThreadMessageRows(input).pipe(
      Effect.map((rows) => rows.map(toProjectionThreadMessage)),
      Effect.mapError(
        toPersistenceSqlError("ProjectionThreadMessageRepository.listByThreadId:postgres"),
      ),
    );

  const deleteByThreadId: ProjectionThreadMessageRepositoryShape["deleteByThreadId"] = (input) =>
    Effect.gen(function* () {
      // 1. Delete from PostgreSQL
      yield* deleteProjectionThreadMessagesByThreadRow(input).pipe(
        Effect.mapError(
          toPersistenceSqlError("ProjectionThreadMessageRepository.deleteByThreadId:postgres"),
        ),
      );


    });

  return {
    upsert,
    getByMessageId,
    listByThreadId,
    deleteByThreadId,
  } satisfies ProjectionThreadMessageRepositoryShape;
});

export const ProjectionThreadMessageRepositoryLive = Layer.effect(
  ProjectionThreadMessageRepository,
  makeProjectionThreadMessageRepository,
).pipe(
  Layer.provideMerge(ProjectionThreadRepositoryLive),
  Layer.provideMerge(ProjectionProjectRepositoryLive),
);
