# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an MCP (Model Context Protocol) server that enables Claude to interact with Apache ActiveMQ message broker. The server exposes tools for sending messages, browsing queues, managing destinations, and monitoring queue statistics through both STOMP protocol and ActiveMQ's REST API (Jolokia).

## Architecture

The MCP server acts as a bridge between Claude and ActiveMQ:
- Uses STOMP protocol (via `stompit`) for message operations
- Uses REST API via Jolokia for administrative operations (browsing, stats, listing)
- Implements MCP SDK's stdio transport for communication with Claude
- All tools use Zod schemas for input validation

## Build & Development Commands

```bash
# Install dependencies
npm install

# Build TypeScript to JavaScript
npm run build

# Run in development mode (with auto-reload)
npm run dev

# Start the MCP server
npm start

# Test with MCP Inspector
npx @modelcontextprotocol/inspector node dist/index.js
```

## ActiveMQ Connection Configuration

The server connects to ActiveMQ using environment variables:
- `ACTIVEMQ_HOST`: Broker hostname (default: localhost)
- `ACTIVEMQ_PORT`: Protocol port (default: 5672 for AMQP, 61613 for STOMP)
- `ACTIVEMQ_USERNAME`: Admin username (default: admin)
- `ACTIVEMQ_PASSWORD`: Admin password (default: admin)
- `ACTIVEMQ_PROTOCOL`: Messaging protocol — `amqp` (default) or `stomp`

REST API calls assume the Jolokia endpoint is on port 8161.

## Core Components

### `src/activemq-client.ts`
Wraps ActiveMQ client operations:
- STOMP operations: `sendMessage()`, `receiveMessage()`
- REST API operations: `browseQueue()`, `listQueues()`, `getQueueStats()`, `purgeQueue()`
- Connection management with proper cleanup

### `src/index.ts`
MCP server implementation:
- Registers all tool handlers using `CallToolRequestSchema`
- Defines tool schemas using Zod for validation
- Maps tool calls to ActiveMQClient methods
- Handles errors and returns formatted responses

### Tool Structure
Each tool in `src/tools/` should:
- Export a Zod schema for input validation
- Export a handler function that takes validated input
- Return MCP-formatted responses with `content` array

## MCP Tools Exposed

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `send_message` | Send to queue/topic | destination, message, headers |
| `browse_queue` | View messages non-destructively | queueName, maxMessages |
| `list_queues` | List all queues | none |
| `list_topics` | List all topics | none |
| `get_queue_stats` | Get queue metrics | queueName |
| `purge_queue` | Delete all messages | queueName |
| `create_queue` | Create new queue | queueName |
| `delete_queue` | Remove queue | queueName |

## ActiveMQ Destination Format

- Queues: `/queue/myqueue` or `myqueue`
- Topics: `/topic/mytopic` or `mytopic`

## Claude Desktop Integration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "activemq": {
      "command": "node",
      "args": ["/path/to/activemq-mcp/dist/index.js"],
      "env": {
        "ACTIVEMQ_HOST": "localhost",
        "ACTIVEMQ_PROTOCOL": "amqp",
        "ACTIVEMQ_PORT": "5672",
        "ACTIVEMQ_USERNAME": "admin",
        "ACTIVEMQ_PASSWORD": "admin"
      }
    }
  }
}
```

## Important Notes

- All MCP server output (console.log) must go to stderr to avoid interfering with stdio transport
- STOMP connections should be properly closed after operations
- Jolokia REST endpoints require Basic Auth using the same credentials
- The server uses ES modules (`"type": "module"` in package.json)
- Queue/topic names in Jolokia URLs need proper encoding for special characters

---

# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

