/**
 * ActiveMQ Client wrapper for AMQP/STOMP and REST API operations
 */

import axios from 'axios';
import rhea from 'rhea';
import type { ActiveMQConfig, MessageHeaders, QueueMessage, QueueStats, QueueInfo } from './types.js';

/**
 * Convert STOMP-style destinations to AMQP addresses.
 * /queue/foo -> foo
 * /topic/foo -> topic://foo
 * foo -> foo (pass through)
 */
function normalizeAmqpAddress(destination: string): string {
  if (destination.startsWith('/queue/')) return destination.slice(7);
  if (destination.startsWith('/topic/')) return `topic://${destination.slice(7)}`;
  return destination;
}

export class ActiveMQClient {
  private config: ActiveMQConfig;
  private jolokiaUrl: string;

  constructor(config: ActiveMQConfig) {
    this.config = config;
    this.jolokiaUrl = `http://${config.host}:8161/api/jolokia`;
  }

  /**
   * Send a message to a queue or topic
   */
  async sendMessage(
    destination: string,
    message: string,
    headers: MessageHeaders = {}
  ): Promise<void> {
    if (this.config.protocol === 'stomp') {
      return this.sendMessageStomp(destination, message, headers);
    }
    return this.sendMessageAmqp(destination, message, headers);
  }

  private async sendMessageAmqp(
    destination: string,
    message: string,
    headers: MessageHeaders = {}
  ): Promise<void> {
    let destType: string;
    let destName: string;
    if (destination.startsWith('/topic/')) {
      destType = 'Topic';
      destName = destination.slice(7);
    } else if (destination.startsWith('/queue/')) {
      destType = 'Queue';
      destName = destination.slice(7);
    } else {
      destType = 'Queue';
      destName = destination;
    }

    const encodedName = encodeURIComponent(destName);
    const mbean = `org.apache.activemq:type=Broker,brokerName=localhost,destinationType=${destType},destinationName=${encodedName}`;

    const hasHeaders = Object.keys(headers).length > 0;
    try {
      if (hasHeaders) {
        await axios.post(
          this.jolokiaUrl,
          {
            type: 'exec',
            mbean,
            operation: 'sendTextMessage(java.util.Map,java.lang.String,java.lang.String,java.lang.String)',
            arguments: [headers, message, this.config.username, this.config.password],
          },
          { auth: { username: this.config.username, password: this.config.password } }
        );
      } else {
        await axios.post(
          this.jolokiaUrl,
          {
            type: 'exec',
            mbean,
            operation: 'sendTextMessage(java.lang.String)',
            arguments: [message],
          },
          { auth: { username: this.config.username, password: this.config.password } }
        );
      }
    } catch (error: any) {
      throw new Error(`Failed to send message: ${error.message}`);
    }
  }

  private async sendMessageStomp(
    destination: string,
    message: string,
    headers: MessageHeaders = {}
  ): Promise<void> {
    // @ts-ignore - stompit is an optional dependency for STOMP protocol support
    const stompit = await import('stompit');
    return new Promise((resolve, reject) => {
      stompit.connect(
        {
          host: this.config.host,
          port: this.config.port,
          connectHeaders: {
            host: '/',
            login: this.config.username,
            passcode: this.config.password,
          },
        },
        (error: Error | null, client: any) => {
          if (error) {
            reject(new Error(`STOMP connection error: ${error.message}`));
            return;
          }
          const frame = client.send({ destination, ...headers });
          frame.write(message);
          frame.end();
          client.disconnect();
          resolve();
        }
      );
    });
  }

  /**
   * Receive a message from a queue or topic
   */
  async receiveMessage(
    destination: string,
    timeout: number = 5000
  ): Promise<QueueMessage | null> {
    if (this.config.protocol === 'stomp') {
      return this.receiveMessageStomp(destination, timeout);
    }
    return this.receiveMessageAmqp(destination, timeout);
  }

  private async receiveMessageAmqp(
    destination: string,
    timeout: number = 5000
  ): Promise<QueueMessage | null> {
    return new Promise((resolve, reject) => {
      const container = rhea.create_container();
      let resolved = false;
      let timeoutHandle: NodeJS.Timeout | null = null;

      const conn = container.connect({
        host: this.config.host,
        port: this.config.port,
        username: this.config.username,
        password: this.config.password,
      });

      const address = normalizeAmqpAddress(destination);

      conn.once('connection_open', () => {
        const receiver = conn.open_receiver({ source: { address } });

        timeoutHandle = setTimeout(() => {
          if (!resolved) {
            resolved = true;
            receiver.close();
            conn.close();
            resolve(null);
          }
        }, timeout);

        receiver.once('message', (ctx: any) => {
          if (resolved) return;
          resolved = true;
          if (timeoutHandle) clearTimeout(timeoutHandle);

          const msg = ctx.message;
          const props = msg.application_properties || {};
          const msgHeaders: MessageHeaders = {};
          for (const key in props) {
            msgHeaders[key] = String(props[key]);
          }

          const queueMessage: QueueMessage = {
            messageId: msg.message_id ? String(msg.message_id) : '',
            timestamp: msg.creation_time ? Number(msg.creation_time) : 0,
            headers: msgHeaders,
            body: typeof msg.body === 'string' ? msg.body : JSON.stringify(msg.body),
          };

          receiver.close();
          conn.close();
          resolve(queueMessage);
        });
      });

      conn.once('disconnected', (ctx: any) => {
        if (!resolved) {
          resolved = true;
          if (timeoutHandle) clearTimeout(timeoutHandle);
          reject(new Error(`AMQP connection error: ${ctx.error?.message || 'disconnected'}`));
        }
      });

      conn.once('error', (err: any) => {
        if (!resolved) {
          resolved = true;
          if (timeoutHandle) clearTimeout(timeoutHandle);
          reject(new Error(`AMQP error: ${err?.message || String(err)}`));
        }
      });
    });
  }

  private async receiveMessageStomp(
    destination: string,
    timeout: number = 5000
  ): Promise<QueueMessage | null> {
    // @ts-ignore - stompit is an optional dependency for STOMP protocol support
    const stompit = await import('stompit');
    return new Promise((resolve, reject) => {
      stompit.connect(
        {
          host: this.config.host,
          port: this.config.port,
          connectHeaders: {
            host: '/',
            login: this.config.username,
            passcode: this.config.password,
          },
        },
        (error: Error | null, client: any) => {
          if (error) {
            reject(new Error(`STOMP connection error: ${error.message}`));
            return;
          }

          let timeoutHandle: NodeJS.Timeout | null = null;
          let messageReceived = false;

          timeoutHandle = setTimeout(() => {
            if (!messageReceived) {
              client.disconnect();
              resolve(null);
            }
          }, timeout);

          client.subscribe({ destination, ack: 'client-individual' }, (subscribeError: Error | null, message: any) => {
            if (subscribeError) {
              if (timeoutHandle) clearTimeout(timeoutHandle);
              client.disconnect();
              reject(new Error(`STOMP subscribe error: ${subscribeError.message}`));
              return;
            }

            messageReceived = true;
            if (timeoutHandle) clearTimeout(timeoutHandle);

            message.readString('utf-8', (readError: Error | null, body: string | null) => {
              if (readError) {
                client.disconnect();
                reject(new Error(`Failed to read message: ${readError.message}`));
                return;
              }

              const headers: MessageHeaders = {};
              const rawHeaders = message.headers || {};
              for (const key in rawHeaders) {
                if (Object.prototype.hasOwnProperty.call(rawHeaders, key)) {
                  headers[key] = String(rawHeaders[key]);
                }
              }

              const queueMessage: QueueMessage = {
                messageId: headers['message-id'] || '',
                timestamp: parseInt(headers['timestamp'] || '0', 10),
                headers,
                body: body || '',
              };

              client.ack(message);
              client.disconnect();
              resolve(queueMessage);
            });
          });
        }
      );
    });
  }

  /**
   * Browse queue messages using Jolokia REST API
   */
  async browseQueue(queueName: string, maxMessages: number = 100): Promise<QueueMessage[]> {
    try {
      const encodedQueue = encodeURIComponent(queueName);
      const response = await axios.post(
        this.jolokiaUrl,
        {
          type: 'exec',
          mbean: `org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=${encodedQueue}`,
          operation: 'browse()',
        },
        {
          auth: {
            username: this.config.username,
            password: this.config.password,
          },
        }
      );

      const messages = response.data.value || [];
      return messages.slice(0, maxMessages).map((msg: any) => ({
        messageId: msg.JMSMessageID || '',
        timestamp: msg.JMSTimestamp || 0,
        headers: msg.properties || {},
        body: msg.Text || msg.BodyPreview || '',
      }));
    } catch (error: any) {
      throw new Error(`Failed to browse queue: ${error.message}`);
    }
  }

  /**
   * List all queues using Jolokia REST API
   */
  async listQueues(): Promise<QueueInfo[]> {
    try {
      const response = await axios.post(
        this.jolokiaUrl,
        {
          type: 'read',
          mbean: 'org.apache.activemq:type=Broker,brokerName=localhost',
          attribute: 'Queues',
        },
        {
          auth: {
            username: this.config.username,
            password: this.config.password,
          },
        }
      );

      const queues = response.data.value || [];
      return queues.map((q: any) => ({
        name: q.objectName?.destinationName || '',
        type: 'queue' as const,
      }));
    } catch (error: any) {
      throw new Error(`Failed to list queues: ${error.message}`);
    }
  }

  /**
   * List all topics using Jolokia REST API
   */
  async listTopics(): Promise<QueueInfo[]> {
    try {
      const response = await axios.post(
        this.jolokiaUrl,
        {
          type: 'read',
          mbean: 'org.apache.activemq:type=Broker,brokerName=localhost',
          attribute: 'Topics',
        },
        {
          auth: {
            username: this.config.username,
            password: this.config.password,
          },
        }
      );

      const topics = response.data.value || [];
      return topics.map((t: any) => ({
        name: t.objectName?.destinationName || '',
        type: 'topic' as const,
      }));
    } catch (error: any) {
      throw new Error(`Failed to list topics: ${error.message}`);
    }
  }

  /**
   * Get queue statistics using Jolokia REST API
   */
  async getQueueStats(queueName: string): Promise<QueueStats> {
    try {
      const encodedQueue = encodeURIComponent(queueName);
      const response = await axios.post(
        this.jolokiaUrl,
        {
          type: 'read',
          mbean: `org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=${encodedQueue}`,
        },
        {
          auth: {
            username: this.config.username,
            password: this.config.password,
          },
        }
      );

      const stats = response.data.value || {};
      return {
        name: queueName,
        size: stats.QueueSize || 0,
        consumerCount: stats.ConsumerCount || 0,
        enqueueCount: stats.EnqueueCount || 0,
        dequeueCount: stats.DequeueCount || 0,
      };
    } catch (error: any) {
      throw new Error(`Failed to get queue stats: ${error.message}`);
    }
  }

  /**
   * Purge all messages from a queue using Jolokia REST API
   */
  async purgeQueue(queueName: string): Promise<void> {
    try {
      const encodedQueue = encodeURIComponent(queueName);
      await axios.post(
        this.jolokiaUrl,
        {
          type: 'exec',
          mbean: `org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=${encodedQueue}`,
          operation: 'purge()',
        },
        {
          auth: {
            username: this.config.username,
            password: this.config.password,
          },
        }
      );
    } catch (error: any) {
      throw new Error(`Failed to purge queue: ${error.message}`);
    }
  }

  /**
   * Create a new queue using Jolokia REST API
   */
  async createQueue(queueName: string): Promise<void> {
    try {
      await axios.post(
        this.jolokiaUrl,
        {
          type: 'exec',
          mbean: 'org.apache.activemq:type=Broker,brokerName=localhost',
          operation: 'addQueue',
          arguments: [queueName],
        },
        {
          auth: {
            username: this.config.username,
            password: this.config.password,
          },
        }
      );
    } catch (error: any) {
      throw new Error(`Failed to create queue: ${error.message}`);
    }
  }

  /**
   * Delete a queue using Jolokia REST API
   */
  async deleteQueue(queueName: string): Promise<void> {
    try {
      await axios.post(
        this.jolokiaUrl,
        {
          type: 'exec',
          mbean: 'org.apache.activemq:type=Broker,brokerName=localhost',
          operation: 'removeQueue',
          arguments: [queueName],
        },
        {
          auth: {
            username: this.config.username,
            password: this.config.password,
          },
        }
      );
    } catch (error: any) {
      throw new Error(`Failed to delete queue: ${error.message}`);
    }
  }
}
