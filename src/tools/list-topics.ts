/** `list_topics` — list all topics on the broker. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { listResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

const inputSchema = z.object({});

export type Args = z.infer<typeof inputSchema>;

export const list_topics = {
  name: 'list_topics',
  title: 'List Topics',
  description: 'List all topics available on the ActiveMQ broker.',
  inputSchema,
  async handler(_args: Args, client: ActiveMQClient) {
    try {
      const topics = await client.listTopics();
      return listResponse('topics', topics);
    } catch (err) {
      return errorResponse(`Failed to list topics: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
