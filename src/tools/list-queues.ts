/** `list_queues` — list all queues on the broker. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { listResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

const inputSchema = z.object({});

export type Args = z.infer<typeof inputSchema>;

export const list_queues = {
  name: 'list_queues',
  title: 'List Queues',
  description: 'List all queues available on the ActiveMQ broker.',
  inputSchema,
  async handler(_args: Args, client: ActiveMQClient) {
    try {
      const queues = await client.listQueues();
      return listResponse('queues', queues);
    } catch (err) {
      return errorResponse(`Failed to list queues: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
