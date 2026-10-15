/** `purge_queue` — remove all messages from a queue. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { textResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

export const inputSchema = z.object({
  queueName: z.string().describe('The name of the queue to purge.'),
});

export type Args = z.infer<typeof inputSchema>;

export const purge_queue = {
  name: 'purge_queue',
  title: 'Purge Queue',
  description: 'Delete all messages from a queue without removing the queue itself.',
  inputSchema,
  async handler(args: Args, client: ActiveMQClient) {
    try {
      await client.purgeQueue(args.queueName);
      return textResponse(`Purged all messages from queue ${args.queueName}.`);
    } catch (err) {
      return errorResponse(`Failed to purge queue ${args.queueName}: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
