import { Effect, Layer, Schedule, Console, Option, Fiber, Ref } from "effect";
import {
  OrchestrationCommandQueueLive,
  OrchestrationCommandQueue,
} from "../src/persistence/Services/OrchestrationCommandQueue.ts";
import { makePgPersistenceLive } from "../src/persistence/Layers/Postgres.ts";
import { CommandId, ProjectId, OrchestrationCommand } from "@t3tools/contracts";
import * as SqlClient from "effect/unstable/sql/SqlClient";

// Configuration for local postgres testing
const dbUrl = process.env.DATABASE_URL || "postgres://t3code:password@localhost:5432/t3code";

const dbLayer = makePgPersistenceLive(dbUrl);
const mainLayer = Layer.mergeAll(
  dbLayer,
  OrchestrationCommandQueueLive.pipe(Layer.provide(dbLayer)),
);

const program = Effect.gen(function* () {
  const queue = yield* OrchestrationCommandQueue;
  const sql = yield* SqlClient.SqlClient;

  yield* Console.log("Starting orchestration worker integration test...");

  // Setup: clear queue before starting
  yield* sql`TRUNCATE TABLE orchestration_command_queue`.pipe(Effect.ignore);

  const totalJobs = 20;
  const workerCount = 5;

  // Insert test jobs
  yield* Console.log(`Enqueuing ${totalJobs} jobs...`);
  for (let i = 0; i < totalJobs; i++) {
    const cmd: OrchestrationCommand = {
      type: "project.create",
      commandId: CommandId.make(`test-cmd-${i}`),
      projectId: ProjectId.make("test-proj"),
      title: "Test",
      workspaceRoot: "/tmp",
      createdAt: new Date().toISOString(),
    };
    yield* queue.offer(cmd);
  }
  yield* Console.log("Finished enqueuing jobs.");

  const processedCountRef = yield* Ref.make(0);
  const processedSetRef = yield* Ref.make<Set<string>>(new Set());

  // Simulate worker loop
  const makeWorker = (workerId: number) =>
    Effect.gen(function* () {
      yield* Console.log(`Worker ${workerId} started`);
      while (true) {
        const jobOpt = yield* queue.take();
        if (Option.isNone(jobOpt)) {
          // No jobs left, stop worker
          yield* Console.log(`Worker ${workerId} found no jobs. Stopping.`);
          break;
        }

        const job = jobOpt.value;

        // Simulate processing time
        const delay = Math.random() * 100 + 50; // 50-150ms
        yield* Effect.sleep(`${delay} millis`);

        const set = yield* Ref.get(processedSetRef);
        if (set.has(job.command.commandId)) {
          yield* Console.error(
            `ERROR: Worker ${workerId} picked an already processed job: ${job.command.commandId}`,
          );
        }
        yield* Ref.update(processedSetRef, (s) => new Set([...s, job.command.commandId]));
        yield* Ref.update(processedCountRef, (c) => c + 1);

        yield* queue.complete(job.command.commandId);
        yield* Console.log(`Worker ${workerId} completed job ${job.command.commandId}`);
      }
    });

  // Spawn workers concurrently
  yield* Console.log(`Spawning ${workerCount} concurrent workers...`);
  const fibers = [];
  for (let i = 0; i < workerCount; i++) {
    const fiber = yield* Effect.forkScoped(makeWorker(i));
    fibers.push(fiber);
  }

  // Wait for all workers to finish
  yield* Fiber.joinAll(fibers);

  // Validate results
  const totalProcessed = yield* Ref.get(processedCountRef);
  const processedSet = yield* Ref.get(processedSetRef);

  yield* Console.log(`\n--- Test Results ---`);
  yield* Console.log(`Total jobs originally enqueued: ${totalJobs}`);
  yield* Console.log(`Total jobs processed successfully: ${totalProcessed}`);
  yield* Console.log(`Unique jobs processed: ${processedSet.size}`);

  if (totalProcessed === totalJobs && processedSet.size === totalJobs) {
    yield* Console.log(
      "✅ Orchestration Worker Queue Test Passed! `FOR UPDATE SKIP LOCKED` behaves correctly.",
    );
  } else {
    yield* Console.error("❌ Orchestration Worker Queue Test Failed! Mismatch in job processing counts.");
  }
}).pipe(Effect.scoped);

Effect.runPromise(Effect.provide(program, mainLayer)).catch(console.error);
