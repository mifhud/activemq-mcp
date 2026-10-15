/** `get_queue_stats` — retrieve statistics for a queue. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { textResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

export const inputSchema = z.object({
  queueName: z.string().describe('The name of the queue to inspect.'),
});

export type Args = z.infer<typeof inputSchema>;

export const get_queue_stats = {
  name: 'get_queue_stats',
  title: 'Get Queue Stats',
  description: 'Retrieve metrics for a queue (size, consumers, producers, enqueue/dequeue counts).',
  inputSchema,
  async handler(args: Args, client: ActiveMQClient) {
    try {
      const stats = await client.getQueueStats(args.queueName);
      return textResponse(JSON.stringify(stats, null, 2));
    } catch (err) {
      return errorResponse(`Failed to get stats for queue ${args.queueName}: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
