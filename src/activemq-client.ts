/**
 * ActiveMQ Client wrapper for STOMP and REST API operations
 */

import * as stompit from 'stompit';
import axios from 'axios';
import type { ActiveMQConfig, MessageHeaders, QueueMessage, QueueStats, QueueInfo } from './types.js';

export class ActiveMQClient {
  private config: ActiveMQConfig;
  private jolokiaUrl: string;

  constructor(config: ActiveMQConfig) {
    this.config = config;
    this.jolokiaUrl = `http://${config.host}:8161/api/jolokia`;
  }

  /**
   * Send a message to a queue or topic using STOMP
   */
  async sendMessage(
    destination: string,
    message: string,
    headers: MessageHeaders = {}
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const connectOptions = {
        host: this.config.host,
        port: this.config.port,
        connectHeaders: {
          host: '/',
          login: this.config.username,
          passcode: this.config.password,
        },
      };

      stompit.connect(connectOptions, (error, client) => {
        if (error) {
          reject(new Error(`STOMP connection error: ${error.message}`));
          return;
        }

        const sendHeaders = {
          destination,
          ...headers,
        };

        const frame = client.send(sendHeaders);
        frame.write(message);
        frame.end();

        client.disconnect();
        resolve();
      });
    });
  }

  /**
   * Receive a message from a queue or topic using STOMP
   */
  async receiveMessage(
    destination: string,
    timeout: number = 5000
  ): Promise<QueueMessage | null> {
    return new Promise((resolve, reject) => {
      const connectOptions = {
        host: this.config.host,
        port: this.config.port,
        connectHeaders: {
          host: '/',
          login: this.config.username,
          passcode: this.config.password,
        },
      };

      stompit.connect(connectOptions, (error, client) => {
        if (error) {
          reject(new Error(`STOMP connection error: ${error.message}`));
          return;
        }

        const subscribeHeaders = {
          destination,
          ack: 'client-individual',
        };

        let timeoutHandle: NodeJS.Timeout | null = null;
        let messageReceived = false;

        // Set up timeout
        timeoutHandle = setTimeout(() => {
          if (!messageReceived) {
            client.disconnect();
            resolve(null);
          }
        }, timeout);

        client.subscribe(subscribeHeaders, (subscribeError, message) => {
          if (subscribeError) {
            if (timeoutHandle) clearTimeout(timeoutHandle);
            client.disconnect();
            reject(new Error(`STOMP subscribe error: ${subscribeError.message}`));
            return;
          }

          messageReceived = true;
          if (timeoutHandle) clearTimeout(timeoutHandle);

          message.readString('utf-8', (readError, body) => {
            if (readError) {
              client.disconnect();
              reject(new Error(`Failed to read message: ${readError.message}`));
              return;
            }

            const headers: MessageHeaders = {};
            const rawHeaders = (message as any).headers || {};
            for (const key in rawHeaders) {
              if (rawHeaders.hasOwnProperty(key)) {
                headers[key] = String(rawHeaders[key]);
              }
            }

            const queueMessage: QueueMessage = {
              messageId: headers['message-id'] || '',
              timestamp: parseInt(headers['timestamp'] || '0', 10),
              headers,
              body: body || '',
            };

            // Acknowledge the message
            client.ack(message);
            client.disconnect();
            resolve(queueMessage);
          });
        });
      });
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
