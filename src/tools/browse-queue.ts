/** `browse_queue` — non-destructive browse of a queue. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { textResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

export const inputSchema = z.object({
  queueName: z.string().describe('The name of the queue to browse.'),
  maxMessages: z
    .number()
    .int()
    .positive()
    .optional()
    .default(10)
    .describe('Maximum number of messages to return.'),
});

export type Args = z.infer<typeof inputSchema>;

export const browse_queue = {
  name: 'browse_queue',
  title: 'Browse Queue',
  description: 'View messages in a queue without consuming them (non-destructive browse).',
  inputSchema,
  async handler(args: Args, client: ActiveMQClient) {
    try {
      const messages = await client.browseQueue(args.queueName, args.maxMessages);
      if (messages.length === 0) {
        return textResponse(`No messages in queue ${args.queueName}.`);
      }
      return textResponse(JSON.stringify(messages, null, 2));
    } catch (err) {
      return errorResponse(`Failed to browse queue ${args.queueName}: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
