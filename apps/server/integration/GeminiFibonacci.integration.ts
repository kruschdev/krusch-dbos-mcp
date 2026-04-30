import { Effect, Layer, Scope, Stream, Ref } from "effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { makeGeminiAdapterLive } from "../src/provider/Layers/GeminiAdapter.ts";
import { GeminiAdapter } from "../src/provider/Services/GeminiAdapter.ts";
import { ServerConfig } from "../src/config.ts";
import { ServerSettingsService } from "../src/serverSettings.ts";

const testProgram = Effect.gen(function* () {
  const adapter = yield* GeminiAdapter;
  console.log("Starting session...");
  const session = yield* adapter.startSession({
    threadId: "fibonacci-thread" as any,
    provider: "gemini",
    runtimeMode: "full-access",
  });
  console.log("Session started", session);

  let buffer = "";

  yield* Stream.runForEach(adapter.streamEvents, (event) =>
    Effect.sync(() => {
      if (event.type === "content.delta" && event.payload.streamKind === "assistant_text") {
        buffer += event.payload.delta;
        process.stdout.write(event.payload.delta);
      }
      if (event.type === "turn.completed") {
        console.log("\n\n--- TURN COMPLETED ---\n");
        buffer = "";
      }
    }),
  ).pipe(Effect.forkScoped);

  console.log("\n\n[TURN 1] Sending: 'Write a python fibonacci function.'");
  yield* adapter.sendTurn({
    threadId: "fibonacci-thread" as any,
    input: "Write a python fibonacci function.",
  });

  yield* Effect.sleep("15 seconds");

  console.log("\n\n[TURN 2] Sending: 'Now add memoization to it.'");
  yield* adapter.sendTurn({
    threadId: "fibonacci-thread" as any,
    input: "Now add memoization to it.",
  });

  yield* Effect.sleep("15 seconds");
  yield* adapter.stopAll();
  console.log("Fibonacci Swarm Acceptance Test Done");
});

const configLayer = ServerConfig.layerTest(process.cwd(), { prefix: "test" });
const settingsLayer = ServerSettingsService.layerTest();
const deps = Layer.mergeAll(configLayer, settingsLayer);
const live = makeGeminiAdapterLive().pipe(Layer.provideMerge(deps));
const fullProgram = testProgram.pipe(Effect.provide(live), Effect.provide(NodeServices.layer));

Effect.runPromise(Effect.scoped(fullProgram)).catch(console.error);
