/**
 * Type definitions for ActiveMQ MCP Server
 */

export interface ActiveMQConfig {
  host: string;
  port: number;
  webPort: number;
  username: string;
  password: string;
  protocol: 'amqp' | 'stomp' | 'openwire';
}

export interface MessageHeaders {
  [key: string]: string;
}

export interface QueueMessage {
  messageId: string;
  timestamp: number;
  headers: MessageHeaders;
  body: string;
}

export interface QueueStats {
  name: string;
  size: number;
  consumerCount: number;
  enqueueCount: number;
  dequeueCount: number;
}

export interface QueueInfo {
  name: string;
  type: 'queue' | 'topic';
}
