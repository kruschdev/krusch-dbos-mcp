import * as path from "node:path";
import { defineConfig, mergeConfig } from "vitest/config";

import baseConfig from "../../vitest.config.ts";

export default mergeConfig(
  baseConfig,
  defineConfig({
    test: {
      // The server suite exercises sqlite, git, temp worktrees, and orchestration
      // runtimes heavily. Running files in parallel introduces load-sensitive flakes.
      fileParallelism: false,
      maxConcurrency: 1,
      // Server integration tests exercise sqlite, git, and orchestration together.
      // Under package-wide parallel runs they regularly exceed the default 15s budget.
      testTimeout: 60_000,
      hookTimeout: 60_000,
      setupFiles: [path.resolve(import.meta.dirname, "./src/test/setup.ts")],
      env: {
        DATABASE_URL: "postgres://kruschdb:password@localhost:5435/kruschdb_test",
      },
    },
  }),
);
