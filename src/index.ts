/**
 * MCP server entry point for ActiveMQ.
 *
 * Uses the stdio transport to communicate with the Claude host. All logging
 * goes to stderr so stdout carries only protocol frames.
 *
 * Tools are registered from `src/tools/*` (each exports a typed
 * `ToolDefinition`); this file adapts the ActiveMQ client configuration from
 * environment variables and wires the server.
 */
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

import { ActiveMQClient } from './activemq-client.js';
import { registerTool } from './tools/registry.js';
import {
  send_message,
  receive_message,
  browse_queue,
  list_queues,
  list_topics,
  get_queue_stats,
  purge_queue,
  create_queue,
  delete_queue,
} from './tools/index.js';

// ── Configuration from environment ──────────────────────────────────────

function loadConfig() {
  const port = Number(process.env.ACTIVEMQ_PORT ?? 61613);
  const restPort = Number(process.env.ACTIVEMQ_REST_PORT ?? process.env.ACTIVEMQ_JOLOKIA_PORT ?? 8161);
  return {
    host: process.env.ACTIVEMQ_HOST ?? 'localhost',
    port: Number.isNaN(port) ? 61613 : port,
    username: process.env.ACTIVEMQ_USERNAME ?? 'admin',
    password: process.env.ACTIVEMQ_PASSWORD ?? 'admin',
    brokerName: process.env.ACTIVEMQ_BROKER_NAME ?? 'localhost',
    restPort: Number.isNaN(restPort) ? 8161 : restPort,
    restTimeoutMs: Number(process.env.ACTIVEMQ_REST_TIMEOUT_MS ?? 10000),
    stompTimeoutMs: Number(process.env.ACTIVEMQ_STOMP_TIMEOUT_MS ?? 10000),
  };
}

// ── Server setup ────────────────────────────────────────────────────────

const config = loadConfig();
const client = new ActiveMQClient(config);

const server = new McpServer(
  {
    name: 'activemq-mcp',
    version: '1.0.0',
    title: 'ActiveMQ MCP Server',
  },
  {
    capabilities: {
      tools: {},
    },
    // Surface connection defaults to Claude for transparency.
    instructions: [
      'Interact with Apache ActiveMQ via STOMP and the Jolokia REST API.',
      `Configured broker: ${config.host}:${config.port} (STOMP), Jolokia on port ${config.restPort}.`,
      'Queue/topic destinations may be given as /queue/name, /topic/name, or a bare name (defaults to queue).',
    ].join('\n'),
  },
);

// ── Tool registration ───────────────────────────────────────────────────

const tools = [
  send_message,
  receive_message,
  browse_queue,
  list_queues,
  list_topics,
  get_queue_stats,
  purge_queue,
  create_queue,
  delete_queue,
];

for (const tool of tools) {
  registerTool(server, client, tool);
}

// ── Lifecycle ───────────────────────────────────────────────────────────

function log(message: string): void {
  // MCP stdio servers must write diagnostics to stderr only.
  console.error(`[activemq-mcp] ${message}`);
}

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  log('server running on stdio');
  log(`broker=${config.host}:${config.port} rest=${config.host}:${config.restPort}`);
}

main().catch((err) => {
  log(`fatal: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
