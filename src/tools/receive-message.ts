/** `receive_message` — consume messages from a queue or topic. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { textResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

export const inputSchema = z.object({
  destination: z
    .string()
    .describe('Destination: /queue/name, /topic/name, or a bare name (queues by default).'),
  timeoutMs: z
    .number()
    .int()
    .positive()
    .optional()
    .default(10000)
    .describe('How long (ms) to wait for messages before returning.'),
  maxMessages: z
    .number()
    .int()
    .positive()
    .optional()
    .default(1)
    .describe('Maximum messages to consume in this call.'),
});

export type Args = z.infer<typeof inputSchema>;

export const receive_message = {
  name: 'receive_message',
  title: 'Receive Message',
  description: 'Consume messages from an ActiveMQ queue or topic. Returns when the timeout elapses or maxMessages is reached.',
  inputSchema,
  async handler(args: Args, client: ActiveMQClient) {
    try {
      const messages = await client.receiveMessage(args.destination, {
        timeoutMs: args.timeoutMs,
        maxMessages: args.maxMessages,
      });
      if (messages.length === 0) {
        return textResponse(`No messages received from ${args.destination} within ${args.timeoutMs}ms.`);
      }
      return textResponse(JSON.stringify(messages, null, 2));
    } catch (err) {
      return errorResponse(`Failed to receive from ${args.destination}: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
