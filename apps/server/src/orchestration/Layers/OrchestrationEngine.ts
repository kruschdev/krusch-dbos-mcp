import type {
  OrchestrationEvent,
  OrchestrationReadModel,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { OrchestrationCommand } from "@t3tools/contracts";
import * as Semaphore from "effect/Semaphore";
import {
  Cause,
  Config,
  Deferred,
  Duration,
  Effect,
  Exit,
  Layer,
  Metric,
  Option,
  PubSub,
  Queue,
  Schedule,
  Schema,
  Stream,
} from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  metricAttributes,
  orchestrationCommandAckDuration,
  orchestrationCommandsTotal,
  orchestrationCommandDuration,
} from "../../observability/Metrics.ts";
import { toPersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
  type OrchestrationDispatchError,
} from "../Errors.ts";
import { decideOrchestrationCommand } from "../decider.ts";
import { createEmptyReadModel, projectEvent } from "../projector.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
import { OrchestrationCommandQueue } from "../../persistence/Services/OrchestrationCommandQueue.ts";

interface CommandEnvelope {
  command: OrchestrationCommand;
  startedAtMs: number;
}

function commandToAggregateRef(command: OrchestrationCommand): {
  readonly aggregateKind: "project" | "thread";
  readonly aggregateId: ProjectId | ThreadId;
} {
  switch (command.type) {
    case "project.create":
    case "project.meta.update":
    case "project.delete":
      return {
        aggregateKind: "project",
        aggregateId: command.projectId,
      };
    default:
      return {
        aggregateKind: "thread",
        aggregateId: command.threadId,
      };
  }
}

const makeOrchestrationEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* OrchestrationEventStore;
  const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
  const projectionPipeline = yield* OrchestrationProjectionPipeline;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const commandQueueService = yield* OrchestrationCommandQueue;

  let readModel = createEmptyReadModel(new Date().toISOString());
  const engineLock = yield* Semaphore.make(1);

  const eventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();

  const processEnvelope = (envelope: CommandEnvelope): Effect.Effect<void> => {
    const dispatchStartSequence = readModel.snapshotSequence;
    const processingStartedAtMs = Date.now();
    const aggregateRef = commandToAggregateRef(envelope.command);
    const baseMetricAttributes = {
      commandType: envelope.command.type,
      aggregateKind: aggregateRef.aggregateKind,
    } as const;
    const reconcileReadModelAfterDispatchFailure = Effect.gen(function* () {
      const persistedEvents = yield* Stream.runCollect(
        eventStore.readFromSequence(dispatchStartSequence),
      ).pipe(Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)));
      if (persistedEvents.length === 0) {
        return;
      }

      let nextReadModel = readModel;
      for (const persistedEvent of persistedEvents) {
        nextReadModel = yield* projectEvent(nextReadModel, persistedEvent);
      }
      readModel = nextReadModel;

      for (const persistedEvent of persistedEvents) {
        yield* PubSub.publish(eventPubSub, persistedEvent);
      }
    });

    return Effect.gen(function* () {
        yield* Effect.annotateCurrentSpan({
          "orchestration.command_id": envelope.command.commandId,
          "orchestration.command_type": envelope.command.type,
          "orchestration.aggregate_kind": aggregateRef.aggregateKind,
          "orchestration.aggregate_id": aggregateRef.aggregateId,
        });

        const existingReceipt = yield* commandReceiptRepository.getByCommandId({
          commandId: envelope.command.commandId,
        });
        if (Option.isSome(existingReceipt)) {
          if (existingReceipt.value.status === "accepted") {
            return {
              sequence: existingReceipt.value.resultSequence,
            };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: envelope.command.commandId,
            detail: existingReceipt.value.error ?? "Previously rejected.",
          });
        }

        const committedCommand = yield* engineLock.withPermits(1)(
          Effect.gen(function* () {
            const eventBase = yield* decideOrchestrationCommand({
              command: envelope.command,
              readModel,
            });
            const eventBases = Array.isArray(eventBase) ? eventBase : [eventBase];
            const transactionResult = yield* sql
              .withTransaction(
                Effect.gen(function* () {
                  const committedEvents: OrchestrationEvent[] = [];
                  let nextReadModel = readModel;

                  for (const nextEvent of eventBases) {
                    const savedEvent = yield* eventStore.append(nextEvent);
                    nextReadModel = yield* projectEvent(nextReadModel, savedEvent);
                    yield* projectionPipeline.projectEvent(savedEvent);
                    committedEvents.push(savedEvent);
                  }

                  const lastSavedEvent = committedEvents.at(-1) ?? null;
                  if (lastSavedEvent === null) {
                    return yield* new OrchestrationCommandInvariantError({
                      commandType: envelope.command.type,
                      detail: "Command produced no events.",
                    });
                  }

                  yield* commandReceiptRepository.upsert({
                    commandId: envelope.command.commandId,
                    aggregateKind: lastSavedEvent.aggregateKind,
                    aggregateId: lastSavedEvent.aggregateId,
                    acceptedAt: lastSavedEvent.occurredAt,
                    resultSequence: lastSavedEvent.sequence,
                    status: "accepted",
                    error: null,
                  });

                  return {
                    committedEvents,
                    lastSequence: lastSavedEvent.sequence,
                    nextReadModel,
                  } as const;
                }),
              )
              .pipe(
                Effect.catchTag("SqlError", (sqlError) =>
                  Effect.fail(
                    toPersistenceSqlError("OrchestrationEngine.processEnvelope:transaction")(sqlError),
                  ),
                ),
              );

            readModel = transactionResult.nextReadModel;
            return transactionResult;
          })
        );
        for (const [index, event] of committedCommand.committedEvents.entries()) {
          yield* PubSub.publish(eventPubSub, event);
          if (index === 0) {
            yield* Metric.update(
              Metric.withAttributes(
                orchestrationCommandAckDuration,
                metricAttributes({
                  ...baseMetricAttributes,
                  ackEventType: event.type,
                }),
              ),
              Duration.millis(Math.max(0, Date.now() - envelope.startedAtMs)),
            );
          }
        }
        return { sequence: committedCommand.lastSequence };
      }).pipe(
        Effect.withSpan(`orchestration.command.${envelope.command.type}`),
        Effect.retry({
          while: (e) => e._tag === "PersistenceSqlError" || e._tag === "OrchestrationCommandInvariantError",
          schedule: Schedule.spaced(Duration.millis(100)).pipe(Schedule.both(Schedule.recurs(50))),
        }),
        Effect.exit
      )
    .pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const outcome = Exit.isSuccess(exit)
            ? "success"
            : Cause.hasInterruptsOnly(exit.cause)
              ? "interrupt"
              : "failure";
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandDuration,
              metricAttributes(baseMetricAttributes),
            ),
            Duration.millis(Math.max(0, Date.now() - processingStartedAtMs)),
          );
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandsTotal,
              metricAttributes({
                ...baseMetricAttributes,
                outcome,
              }),
            ),
            1,
          );

          if (Exit.isSuccess(exit)) {
            return;
          }

          const error = Cause.squash(exit.cause) as OrchestrationDispatchError;
          console.error("processEnvelope failed:", error);
          if (!Schema.is(OrchestrationCommandPreviouslyRejectedError)(error)) {
            yield* reconcileReadModelAfterDispatchFailure.pipe(
              Effect.catch(() =>
                Effect.logWarning(
                  "failed to reconcile orchestration read model after dispatch failure",
                ).pipe(
                  Effect.annotateLogs({
                    commandId: envelope.command.commandId,
                    snapshotSequence: readModel.snapshotSequence,
                  }),
                ),
              ),
            );

            if (Schema.is(OrchestrationCommandInvariantError)(error)) {
              yield* commandReceiptRepository
                .upsert({
                  commandId: envelope.command.commandId,
                  aggregateKind: aggregateRef.aggregateKind,
                  aggregateId: aggregateRef.aggregateId,
                  acceptedAt: new Date().toISOString(),
                  resultSequence: readModel.snapshotSequence,
                  status: "rejected",
                  error: error.message,
                })
                .pipe(Effect.catch(() => Effect.void));
            }
          }
        }),
      ),
    );
  };

  yield* projectionPipeline.bootstrap;
  readModel = yield* projectionSnapshotQuery.getSnapshot();

  const worker = Effect.forever(
    Effect.gen(function* () {
      let commandOpt;
      try {
        commandOpt = yield* commandQueueService.take().pipe(
          Effect.tapError(err => Effect.sync(() => console.error("commandQueueService.take failed:", err)))
        );
      } catch (e) {
        console.error("Worker synchronous error:", e);
        yield* Effect.sleep(Duration.millis(1000));
        return;
      }
      if (Option.isNone(commandOpt)) {
        yield* Effect.sleep(Duration.millis(100));
        return;
      }
      yield* processEnvelope(commandOpt.value);
      yield* commandQueueService.complete(commandOpt.value.command.commandId);
    })
  );
  const workerCount = yield* Config.number("ORCHESTRATION_WORKER_COUNT").pipe(Config.withDefault(5));
  for (let i = 0; i < workerCount; i++) {
    yield* Effect.forkScoped(worker);
  }
  yield* Effect.logDebug("orchestration engine started").pipe(
    Effect.annotateLogs({ sequence: readModel.snapshotSequence }),
  );

  const getReadModel: OrchestrationEngineShape["getReadModel"] = () =>
    Effect.sync((): OrchestrationReadModel => readModel);

  const readEvents: OrchestrationEngineShape["readEvents"] = (fromSequenceExclusive) =>
    eventStore.readFromSequence(fromSequenceExclusive);

  const dispatch: OrchestrationEngineShape["dispatch"] = (command) =>
    Effect.gen(function* () {
      yield* commandQueueService.offer(command);

      while (true) {
        const receipt = yield* commandReceiptRepository.getByCommandId({ commandId: command.commandId });
        if (Option.isSome(receipt)) {
          if (receipt.value.status === "accepted") {
            return { sequence: receipt.value.resultSequence };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: command.commandId,
            detail: receipt.value.error ?? "Previously rejected.",
          });
        }
        yield* Effect.sleep(Duration.millis(100));
      }
    });

  return {
    getReadModel,
    readEvents,
    dispatch,
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (wsServer, ProviderRuntimeIngestion, CheckpointReactor, etc.)
    // each independently receive all domain events.
    get streamDomainEvents(): OrchestrationEngineShape["streamDomainEvents"] {
      return Stream.fromPubSub(eventPubSub);
    },
  } satisfies OrchestrationEngineShape;
});

export const OrchestrationEngineLive = Layer.effect(
  OrchestrationEngineService,
  makeOrchestrationEngine,
);
