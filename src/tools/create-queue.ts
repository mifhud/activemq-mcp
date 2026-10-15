/** `create_queue` — create a new queue. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { textResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

export const inputSchema = z.object({
  queueName: z.string().describe('The name of the queue to create.'),
});

export type Args = z.infer<typeof inputSchema>;

export const create_queue = {
  name: 'create_queue',
  title: 'Create Queue',
  description: 'Create a new queue on the ActiveMQ broker.',
  inputSchema,
  async handler(args: Args, client: ActiveMQClient) {
    try {
      await client.createQueue(args.queueName);
      return textResponse(`Created queue ${args.queueName}.`);
    } catch (err) {
      return errorResponse(`Failed to create queue ${args.queueName}: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
