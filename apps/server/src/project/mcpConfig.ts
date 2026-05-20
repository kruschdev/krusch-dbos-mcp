import { Schema } from "effect";
import { Effect } from "effect";
import type { McpServer } from "effect-acp/schema";
import * as Path from "node:path";
import * as fs from "node:fs/promises";
import * as os from "node:os";

const CursorMcpServerConfig = Schema.Struct({
  command: Schema.String,
  args: Schema.Array(Schema.String),
  env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
});

const McpConfigSchema = Schema.Struct({
  mcpServers: Schema.Record(Schema.String, CursorMcpServerConfig),
});

const loadConfig = (configPath: string) => 
  Effect.gen(function* () {
    const exists = yield* Effect.promise(() =>
      fs
        .access(configPath)
        .then(() => true)
        .catch(() => false),
    );
    if (!exists) return {};

    const content = yield* Effect.promise(() => fs.readFile(configPath, "utf-8").catch(() => ""));
    if (!content) return {};

    const parsedJson = yield* Effect.try({
      try: () => JSON.parse(content),
      catch: () => new Error("Invalid JSON"),
    });

    const decoded = yield* Schema.decodeUnknownEffect(McpConfigSchema)(parsedJson);
    return decoded.mcpServers as Record<string, any>;
  }).pipe(Effect.orElseSucceed(() => ({} as Record<string, any>)));

/**
 * Reads `.kd/mcp.json` from the global home directory and the provided workspace root,
 * merges them (local overrides global), and parses into an array of ACP-compliant McpServer definitions.
 */
export const readMcpServersConfig = (
  workspaceRoot: string,
): Effect.Effect<ReadonlyArray<McpServer>, never, never> =>
  Effect.gen(function* () {
    const globalConfigPath = Path.join(os.homedir(), ".kd", "mcp.json");
    const localConfigPath = Path.join(workspaceRoot, ".kd", "mcp.json");

    const globalServers = yield* loadConfig(globalConfigPath);
    const localServers = yield* loadConfig(localConfigPath);

    const mergedServers = { ...globalServers, ...localServers };

    const servers: McpServer[] = [];
    for (const [name, config] of Object.entries(mergedServers)) {
      const cfg = config as any;
      servers.push({
        name,
        command: cfg.command,
        args: cfg.args,
        env: cfg.env
          ? Object.entries(cfg.env as Record<string, string>).map(([envName, envValue]) => ({
              name: envName,
              value: envValue,
            }))
          : [],
      });
    }

    return servers;
  });
