import { Effect, Layer, Scope, Stream } from "effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { makeGeminiAdapterLive } from "../src/provider/Layers/GeminiAdapter.ts";
import { GeminiAdapter } from "../src/provider/Services/GeminiAdapter.ts";
import { ServerConfig } from "../src/config.ts";
import { ServerSettingsService } from "../src/serverSettings.ts";

const testProgram = Effect.gen(function* () {
  const adapter = yield* GeminiAdapter;
  console.log("Starting session...");
  const session = yield* adapter.startSession({
    threadId: "test-thread" as any,
    provider: "gemini",
    runtimeMode: "full-access",
  });
  console.log("Session started", session);

  yield* Stream.runForEach(adapter.streamEvents, (event) =>
    Effect.sync(() => console.log("EVENT:", JSON.stringify(event, null, 2))),
  ).pipe(Effect.forkScoped);

  console.log("Sending turn...");
  yield* adapter.sendTurn({
    threadId: "test-thread" as any,
    input: "Hello Gemini, this is a test. Please reply with 'SUCCESS'.",
  });

  yield* Effect.sleep("5 seconds");
  yield* adapter.stopAll();
  console.log("Done");
});

const configLayer = ServerConfig.layerTest(process.cwd(), { prefix: "test" });
const settingsLayer = ServerSettingsService.layerTest();
const deps = Layer.mergeAll(configLayer, settingsLayer);
const live = makeGeminiAdapterLive().pipe(Layer.provideMerge(deps));
const fullProgram = testProgram.pipe(Effect.provide(live), Effect.provide(NodeServices.layer));

Effect.runPromise(Effect.scoped(fullProgram)).catch(console.error);
