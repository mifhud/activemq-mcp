/**
 * Typed tool registry: each tool module exports a `ToolDefinition<T>` and
 * `index.ts` registers it against the `McpServer`. Keeping the generic here
 * preserves type inference for parsed args without resorting to `any`.
 */
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import type { ToolResponse } from './result.js';

/** A tool with a Zod-validated input shape and typed handler.
 *
 * `inputSchema` is typed with `Input = unknown` because fields declared with
 * `.optional().default(...)` have an input shape wider than their output (parsed)
 * shape — the `T` parameter is the parsed args the handler receives. */
export interface ToolDefinition<T> {
  name: string;
  title?: string;
  description?: string;
  inputSchema: z.ZodType<T, z.ZodTypeDef, unknown>;
  handler: (args: T, client: ActiveMQClient) => Promise<ToolResponse>;
}

/** Register a single tool definition on the MCP server. */
export function registerTool<T>(server: McpServer, client: ActiveMQClient, def: ToolDefinition<T>): void {
  server.registerTool(def.name, {
    title: def.title,
    description: def.description,
    inputSchema: def.inputSchema,
  }, async (args) => def.handler(args, client));
}
