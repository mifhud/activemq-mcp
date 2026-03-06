#!/usr/bin/env node

/**
 * ActiveMQ MCP Server
 * Provides tools for interacting with Apache ActiveMQ message broker
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { ActiveMQClient } from './activemq-client.js';

// Environment configuration
const protocol = (process.env.ACTIVEMQ_PROTOCOL || 'amqp') as 'amqp' | 'stomp';
const defaultPort = protocol === 'stomp' ? '61613' : '5672';
const config = {
  host: process.env.ACTIVEMQ_HOST || 'localhost',
  port: parseInt(process.env.ACTIVEMQ_PORT || defaultPort, 10),
  username: process.env.ACTIVEMQ_USERNAME || 'admin',
  password: process.env.ACTIVEMQ_PASSWORD || 'admin',
  protocol,
};

// Initialize ActiveMQ client
const client = new ActiveMQClient(config);

// Tool input schemas
const SendMessageSchema = z.object({
  destination: z.string().describe('Queue or topic name (e.g., /queue/myqueue or /topic/mytopic)'),
  message: z.string().describe('Message content to send'),
  headers: z.record(z.string()).optional().describe('Optional message headers'),
});

const BrowseQueueSchema = z.object({
  queueName: z.string().describe('Name of the queue to browse'),
  maxMessages: z.number().optional().default(100).describe('Maximum number of messages to retrieve'),
});

const QueueNameSchema = z.object({
  queueName: z.string().describe('Name of the queue'),
});

const ReceiveMessageSchema = z.object({
  destination: z.string().describe('Queue or topic name to receive from'),
  timeout: z.number().optional().default(5000).describe('Timeout in milliseconds (default: 5000)'),
});

// Create MCP server
const server = new Server(
  {
    name: 'activemq-mcp',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Register tool list handler
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: 'send_message',
        description: 'Send a message to an ActiveMQ queue or topic',
        inputSchema: {
          type: 'object',
          properties: {
            destination: {
              type: 'string',
              description: 'Queue or topic name (e.g., /queue/myqueue or /topic/mytopic)',
            },
            message: {
              type: 'string',
              description: 'Message content to send',
            },
            headers: {
              type: 'object',
              description: 'Optional message headers',
              additionalProperties: { type: 'string' },
            },
          },
          required: ['destination', 'message'],
        },
      },
      {
        name: 'receive_message',
        description: 'Receive a message from an ActiveMQ queue or topic',
        inputSchema: {
          type: 'object',
          properties: {
            destination: {
              type: 'string',
              description: 'Queue or topic name to receive from',
            },
            timeout: {
              type: 'number',
              description: 'Timeout in milliseconds (default: 5000)',
              default: 5000,
            },
          },
          required: ['destination'],
        },
      },
      {
        name: 'browse_queue',
        description: 'Browse messages in a queue without consuming them',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: {
              type: 'string',
              description: 'Name of the queue to browse',
            },
            maxMessages: {
              type: 'number',
              description: 'Maximum number of messages to retrieve (default: 100)',
              default: 100,
            },
          },
          required: ['queueName'],
        },
      },
      {
        name: 'list_queues',
        description: 'List all available queues in the ActiveMQ broker',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'list_topics',
        description: 'List all available topics in the ActiveMQ broker',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'get_queue_stats',
        description: 'Get statistics for a specific queue (size, consumer count, etc.)',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: {
              type: 'string',
              description: 'Name of the queue',
            },
          },
          required: ['queueName'],
        },
      },
      {
        name: 'purge_queue',
        description: 'Remove all messages from a queue',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: {
              type: 'string',
              description: 'Name of the queue to purge',
            },
          },
          required: ['queueName'],
        },
      },
      {
        name: 'create_queue',
        description: 'Create a new queue in the ActiveMQ broker',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: {
              type: 'string',
              description: 'Name of the queue to create',
            },
          },
          required: ['queueName'],
        },
      },
      {
        name: 'delete_queue',
        description: 'Delete a queue from the ActiveMQ broker',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: {
              type: 'string',
              description: 'Name of the queue to delete',
            },
          },
          required: ['queueName'],
        },
      },
    ],
  };
});

// Register tool call handler
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    switch (name) {
      case 'send_message': {
        const { destination, message, headers } = SendMessageSchema.parse(args);
        await client.sendMessage(destination, message, headers);
        return {
          content: [
            {
              type: 'text',
              text: `Successfully sent message to ${destination}`,
            },
          ],
        };
      }

      case 'receive_message': {
        const { destination, timeout } = ReceiveMessageSchema.parse(args);
        const message = await client.receiveMessage(destination, timeout);
        if (message) {
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(message, null, 2),
              },
            ],
          };
        } else {
          return {
            content: [
              {
                type: 'text',
                text: `No message received within ${timeout}ms timeout`,
              },
            ],
          };
        }
      }

      case 'browse_queue': {
        const { queueName, maxMessages } = BrowseQueueSchema.parse(args);
        const messages = await client.browseQueue(queueName, maxMessages);
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(messages, null, 2),
            },
          ],
        };
      }

      case 'list_queues': {
        const queues = await client.listQueues();
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(queues, null, 2),
            },
          ],
        };
      }

      case 'list_topics': {
        const topics = await client.listTopics();
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(topics, null, 2),
            },
          ],
        };
      }

      case 'get_queue_stats': {
        const { queueName } = QueueNameSchema.parse(args);
        const stats = await client.getQueueStats(queueName);
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(stats, null, 2),
            },
          ],
        };
      }

      case 'purge_queue': {
        const { queueName } = QueueNameSchema.parse(args);
        await client.purgeQueue(queueName);
        return {
          content: [
            {
              type: 'text',
              text: `Successfully purged queue: ${queueName}`,
            },
          ],
        };
      }

      case 'create_queue': {
        const { queueName } = QueueNameSchema.parse(args);
        await client.createQueue(queueName);
        return {
          content: [
            {
              type: 'text',
              text: `Successfully created queue: ${queueName}`,
            },
          ],
        };
      }

      case 'delete_queue': {
        const { queueName } = QueueNameSchema.parse(args);
        await client.deleteQueue(queueName);
        return {
          content: [
            {
              type: 'text',
              text: `Successfully deleted queue: ${queueName}`,
            },
          ],
        };
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      content: [
        {
          type: 'text',
          text: `Error: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
});

// Start server
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('ActiveMQ MCP server running on stdio');
}

main().catch((error) => {
  console.error('Server error:', error);
  process.exit(1);
});
