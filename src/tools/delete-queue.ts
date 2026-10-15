/** `delete_queue` — delete a queue. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { textResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

export const inputSchema = z.object({
  queueName: z.string().describe('The name of the queue to delete.'),
  purgeBeforeDelete: z
    .boolean()
    .optional()
    .default(false)
    .describe('Remove all messages before deleting the queue.'),
});

export type Args = z.infer<typeof inputSchema>;

export const delete_queue = {
  name: 'delete_queue',
  title: 'Delete Queue',
  description: 'Delete a queue and its messages from the ActiveMQ broker.',
  inputSchema,
  async handler(args: Args, client: ActiveMQClient) {
    try {
      await client.deleteQueue(args.queueName, args.purgeBeforeDelete);
      return textResponse(`Deleted queue ${args.queueName}.`);
    } catch (err) {
      return errorResponse(`Failed to delete queue ${args.queueName}: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
