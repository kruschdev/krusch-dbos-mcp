import { Layer } from "effect";

import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandQueueLive } from "../persistence/Services/OrchestrationCommandQueue.ts";
import { AgentExecutionQueueLive } from "../persistence/Services/AgentExecutionQueue.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { AgentExecutionEngineLive } from "./Layers/AgentExecutionEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { EmbeddingProviderLive } from "./Layers/EmbeddingProvider.ts";

export const OrchestrationEventInfrastructureLayerLive = Layer.mergeAll(
  OrchestrationEventStoreLive,
  OrchestrationCommandReceiptRepositoryLive,
  OrchestrationCommandQueueLive,
  AgentExecutionQueueLive,
);

export const OrchestrationProjectionPipelineLayerLive = OrchestrationProjectionPipelineLive.pipe(
  Layer.provide(OrchestrationEventStoreLive),
);

export const OrchestrationInfrastructureLayerLive = Layer.mergeAll(
  OrchestrationProjectionSnapshotQueryLive,
  OrchestrationEventInfrastructureLayerLive,
  OrchestrationProjectionPipelineLayerLive,
);

import { HaloOptimizerLive } from "./Layers/HaloOptimizerService.ts";

export const OrchestrationLayerLive = Layer.mergeAll(
  OrchestrationInfrastructureLayerLive,
  OrchestrationEngineLive.pipe(Layer.provide(OrchestrationInfrastructureLayerLive)),
  AgentExecutionEngineLive.pipe(
    Layer.provide(OrchestrationEngineLive),
    Layer.provide(OrchestrationInfrastructureLayerLive),
    Layer.provide(EmbeddingProviderLive)
  ),
  HaloOptimizerLive.pipe(
    Layer.provide(OrchestrationInfrastructureLayerLive),
    Layer.provide(EmbeddingProviderLive)
  )
);
