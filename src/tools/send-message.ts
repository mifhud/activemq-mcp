/** `send_message` — send a text message to a queue or topic. */
import { z } from 'zod';
import type { ActiveMQClient } from '../activemq-client.js';
import { textResponse, errorResponse } from './result.js';
import type { ToolDefinition } from './registry.js';

export const inputSchema = z.object({
  destination: z
    .string()
    .describe('Destination: /queue/name, /topic/name, or a bare name (queues by default).'),
  message: z.string().describe('The message body to send.'),
  headers: z.record(z.string()).optional().describe('Optional additional STOMP headers.'),
});

export type Args = z.infer<typeof inputSchema>;

export const send_message = {
  name: 'send_message',
  title: 'Send Message',
  description: 'Send a text message to an ActiveMQ queue or topic.',
  inputSchema,
  async handler(args: Args, client: ActiveMQClient) {
    try {
      await client.sendMessage(args.destination, args.message, args.headers);
      return textResponse(`Sent message to ${args.destination}.`);
    } catch (err) {
      return errorResponse(`Failed to send message to ${args.destination}: ${err instanceof Error ? err.message : String(err)}`);
    }
  },
} satisfies ToolDefinition<Args>;
