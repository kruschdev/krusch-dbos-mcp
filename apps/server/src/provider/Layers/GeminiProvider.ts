import type { ServerProvider, ServerProviderModel } from "@t3tools/contracts";
import { Effect, Layer, Stream, Equal, Duration } from "effect";

import { ServerSettingsService } from "../../serverSettings.ts";
import { buildServerProvider, providerModelsFromSettings } from "../providerSnapshot.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { GeminiProvider } from "../Services/GeminiProvider.ts";

const PROVIDER = "gemini" as const;

function buildInitialGeminiProviderSnapshot(geminiSettings: {
  enabled: boolean;
  customModels?: ReadonlyArray<string>;
}): ServerProvider {
  const checkedAt = new Date().toISOString();

  const baseModels: Array<ServerProviderModel> = [
    {
      slug: "gemini-3.1-pro",
      name: "Gemini 3.1 Pro",
      capabilities: {
        reasoningEffortLevels: [],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        contextWindowOptions: [],
        promptInjectedEffortLevels: [],
      },
      isCustom: false,
    },
    {
      slug: "gemini-3.1-flash",
      name: "Gemini 3.1 Flash",
      capabilities: {
        reasoningEffortLevels: [],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        contextWindowOptions: [],
        promptInjectedEffortLevels: [],
      },
      isCustom: false,
    },
    {
      slug: "gemini-3.1-flash-lite",
      name: "Gemini 3.1 Flash-Lite",
      capabilities: {
        reasoningEffortLevels: [],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        contextWindowOptions: [],
        promptInjectedEffortLevels: [],
      },
      isCustom: false,
    },
  ];

  const models = providerModelsFromSettings(
    baseModels,
    PROVIDER,
    geminiSettings.customModels ?? [],
    {
      reasoningEffortLevels: [],
      supportsFastMode: false,
      supportsThinkingToggle: false,
      contextWindowOptions: [],
      promptInjectedEffortLevels: [],
    },
  );

  return buildServerProvider({
    provider: PROVIDER,
    enabled: geminiSettings.enabled,
    checkedAt,
    models,
    probe: {
      installed: true,
      version: null,
      status: "ready",
      auth: { status: "unknown" },
    },
  });
}

export const GeminiProviderLive = Layer.effect(
  GeminiProvider,
  Effect.gen(function* () {
    const serverSettings = yield* Effect.service(ServerSettingsService);

    const managedProvider = yield* makeManagedServerProvider({
      getSettings: serverSettings.getSettings.pipe(
        Effect.map((s) => s.providers.gemini),
        Effect.orDie,
      ),
      streamSettings: serverSettings.streamChanges.pipe(Stream.map((s) => s.providers.gemini)),
      haveSettingsChanged: (previous, next) => !Equal.equals(previous, next),
      initialSnapshot: (settings) => buildInitialGeminiProviderSnapshot(settings),
      checkProvider: Effect.gen(function* () {
        const settings = yield* serverSettings.getSettings.pipe(
          Effect.map((s) => s.providers.gemini),
          Effect.orDie,
        );
        return buildInitialGeminiProviderSnapshot(settings);
      }),
      refreshInterval: Duration.minutes(5),
    });

    return managedProvider;
  }),
);
