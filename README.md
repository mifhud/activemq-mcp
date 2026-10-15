# activemq-mcp

An [MCP](https://modelcontextprotocol.io) (Model Context Protocol) server that
lets Claude (or any MCP client) interact with an **Apache ActiveMQ** broker:

- **STOMP** protocol (`stompit`) for sending/receiving messages
- **Jolokia REST API** (port 8161) for browsing, listing, statistics, and
  administrative operations (purge/create/delete)

## Requirements

- Node.js >= 20
- An Apache ActiveMQ broker with the STOMP transport enabled (default port
  `61613`) and the Jolokia REST API available (default port `8161`)

## Install & build

```bash
npm install
npm run build        # compiles TypeScript → dist/
```

## Configuration

Set these environment variables before running the server:

| Variable | Default | Description |
| --- | --- | --- |
| `ACTIVEMQ_HOST` | `localhost` | Broker hostname |
| `ACTIVEMQ_PORT` | `61613` | STOMP port |
| `ACTIVEMQ_USERNAME` | `admin` | Admin username (STOMP + Jolokia Basic Auth) |
| `ACTIVEMQ_PASSWORD` | `admin` | Admin password |
| `ACTIVEMQ_REST_PORT` (or `ACTIVEMQ_JOLOKIA_PORT`) | `8161` | Jolokia REST port |
| `ACTIVEMQ_BROKER_NAME` | `localhost` | Broker name in Jolokia object names |
| `ACTIVEMQ_STOMP_TIMEOUT_MS` | `10000` | STOMP operation timeout |
| `ACTIVEMQ_REST_TIMEOUT_MS` | `10000` | Jolokia REST call timeout |

## Run

```bash
npm start            # stdio transport
npm run dev          # auto-reloading (tsx)
```

All server diagnostics are written to **stderr** to keep stdout clean for the
MCP stdio transport.

## Claude Desktop integration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "activemq": {
      "command": "node",
      "args": ["/path/to/activemq-mcp/dist/index.js"],
      "env": {
        "ACTIVEMQ_HOST": "localhost",
        "ACTIVEMQ_USERNAME": "admin",
        "ACTIVEMQ_PASSWORD": "admin"
      }
    }
  }
}
```

## Tools

| Tool | Purpose | Key parameters |
| --- | --- | --- |
| `send_message` | Send a message to a queue/topic | destination, message, headers |
| `receive_message` | Consume messages from a queue/topic | destination, maxMessages, timeoutMs, ack |
| `browse_queue` | View queue messages non-destructively | queueName, maxMessages |
| `list_queues` | List all queues | — |
| `list_topics` | List all topics | — |
| `get_queue_stats` | Queue metrics (size, consumers, counts) | queueName |
| `purge_queue` | Delete all messages from a queue | queueName |
| `create_queue` | Create a new queue | queueName |
| `delete_queue` | Remove a queue | queueName, purgeBeforeDelete |

### Destination format

- Queues: `/queue/myqueue` or `myqueue` (bare names default to queues)
- Topics: `/topic/mytopic` or `mytopic`

## Inspect with MCP Inspector

```bash
npx @modelcontextprotocol/inspector node dist/index.js
```

## Architecture

```
src/
├── index.ts              # McpServer + stdio transport + tool registration
├── activemq-client.ts    # STOMP (send/receive) + Jolokia REST (admin)
├── tools/
│   ├── registry.ts       # typed ToolDefinition + registerTool helper
│   ├── result.ts         # shared tool response helpers
│   ├── send-message.ts
│   ├── receive-message.ts
│   ├── browse-queue.ts
│   ├── list-queues.ts
│   ├── list-topics.ts
│   ├── get-queue-stats.ts
│   ├── purge-queue.ts
│   ├── create-queue.ts
│   └── delete-queue.ts
```

## License

MIT
