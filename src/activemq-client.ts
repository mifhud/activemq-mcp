/**
 * ActiveMQClient — wraps ActiveMQ interaction over two protocols:
 *  - STOMP (via `stompit`) for message send/receive operations
 *  - Jolokia REST API (port 8161) for administrative operations
 *
 * STOMP connections are opened and closed around each operation so they
 * never leak or block process exit.
 */
import stompit from 'stompit';

export interface ActiveMQConfig {
  host: string;
  port: number;
  username: string;
  password: string;
  /** Jolokia (REST management) port. Defaults to 8161. */
  restPort: number;
  /** ActiveMQ broker name used in Jolokia object names. Defaults to "localhost". */
  brokerName: string;
  /** Request timeout (ms) for Jolokia REST calls. */
  restTimeoutMs: number;
  /** Timeout (ms) for STOMP operations. */
  stompTimeoutMs: number;
}

export interface ReceiveMessageOptions {
  /** How long to wait for messages, in ms. */
  timeoutMs?: number;
  /** Maximum messages to consume before returning. */
  maxMessages?: number;
  /** STOMP ack mode. */
  ack?: 'auto' | 'client' | 'client-individual';
}

export interface ReceivedMessage {
  body: string;
  headers: Record<string, string>;
}

export interface QueueStats {
  queueSize: number;
  consumerCount: number;
  producerCount: number;
  enqueueCount: number;
  dequeueCount: number;
  dispatchCount: number;
  expiredCount: number;
}

export type DestinationType = 'Queue' | 'Topic';

export interface BrowsedMessage {
  /** MessageId (e.g. "ID-host-...-1"). */
  messageId?: string;
  /** STOMP/AMQ headers such as JMSCorrelationID, JMSType. */
  headers: Record<string, string>;
  /** Raw property map from Jolokia browse(). */
  [key: string]: unknown;
}

const DEFAULT_CONFIG: Partial<ActiveMQConfig> = {
  restPort: 8161,
  brokerName: 'localhost',
  restTimeoutMs: 10000,
  stompTimeoutMs: 10000,
};

const ZERO_STATS: QueueStats = {
  queueSize: 0,
  consumerCount: 0,
  producerCount: 0,
  enqueueCount: 0,
  dequeueCount: 0,
  dispatchCount: 0,
  expiredCount: 0,
};

/** A Jolokia JSON response envelope. */
interface JolokiaResponse {
  status: number;
  error?: string;
  error_type?: string;
  value: unknown;
}

/**
 * stompit's bundled @types omit `headers` on `Client.Message`. At runtime every
 * delivered message carries a `headers` map, so we view it through this shape.
 */
interface StompMessageView {
  headers: Record<string, string>;
}

/** A single field pulled from a destination's JMX attributes. */
export type DestinationAttribute =
  | 'QueueSize'
  | 'ConsumerCount'
  | 'ProducerCount'
  | 'EnqueueCount'
  | 'DequeueCount'
  | 'DispatchCount'
  | 'ExpiredCount';

/** Type guard: true when value is a plain JSON object. */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Coerce an unknown value to a number (0 when missing/non-numeric). */
function toNumber(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    return Number.isNaN(parsed) ? 0 : parsed;
  }
  return 0;
}

/** Read the headers map off a stompit message.
 *  Library @types omit `headers` on Client.Message, but every delivered
 *  message carries one at runtime, so view it at the boundary. */
function readMessageHeaders(message: stompit.Client.Message): Record<string, string> {
  const view = message as unknown as StompMessageView;
  return view.headers ?? {};
}

export class ActiveMQClient {
  public readonly config: ActiveMQConfig;

  constructor(config: Partial<ActiveMQConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config } as ActiveMQConfig;
  }

  /** Build STOMP connection options used by `stompit.connect`. */
  private getConnectionOptions(): stompit.connect.ConnectOptions {
    const { host, port, username, password } = this.config;
    return {
      host,
      port,
      connectHeaders: {
        host: '/',
        login: username,
        passcode: password,
        'heart-beat': '5000,5000',
      },
    };
  }

  /** Open a STOMP connection, ensuring it is closed when `fn` completes. */
  private async withStompClient<T>(fn: (client: stompit.Client) => Promise<T>): Promise<T> {
    const options = this.getConnectionOptions();
    const timeoutMs = this.config.stompTimeoutMs;

    const client = await new Promise<stompit.Client>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`STOMP connect to ${this.config.host}:${this.config.port} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      stompit.connect(options, (error, client) => {
        clearTimeout(timer);
        if (error) {
          reject(new Error(`STOMP connection failed: ${error.message}`));
          return;
        }
        resolve(client);
      });
    });

    let result: T;
    try {
      result = await fn(client);
    } finally {
      await this.safeDisconnect(client);
    }
    return result;
  }

  /** Gracefully disconnect, never throwing. */
  private safeDisconnect(client: stompit.Client): Promise<void> {
    const timeoutMs = this.config.stompTimeoutMs;
    return new Promise<void>((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(finish, timeoutMs);
      try {
        client.disconnect(finish);
      } catch {
        finish();
      }
    });
  }

  /** Authorization header shared by Jolokia REST calls. */
  private authHeader(): Record<string, string> {
    const token = Buffer.from(`${this.config.username}:${this.config.password}`).toString('base64');
    return { Authorization: `Basic ${token}` };
  }

  /** Jolokia REST base URL (admin console), used by 3 call sites. */
  private jolokiaBaseUrl(): string {
    return `http://${this.config.host}:${this.config.restPort}/api/jolokia`;
  }

  /** URL-encode a Jolokia object name for use in REST paths. */
  private encodeObjectName(name: string): string {
    return encodeURIComponent(name);
  }

  /** Broker-level object name for administrative operations, used 6+ times. */
  private brokerObjectName(): string {
    return `org.apache.activemq:type=Broker,brokerName=${this.config.brokerName}`;
  }

  /** Build a Jolokia object name for a destination. */
  destinationObjectName(name: string, destinationType: DestinationType = 'Queue'): string {
    const escapedName = ActiveMQClient.escapeDestinationName(name);
    return `org.apache.activemq:type=Broker,brokerName=${this.config.brokerName},destinationType=${destinationType},destinationName=${escapedName}`;
  }

  /** Escape characters that are special within a Jolokia object name. */
  private static escapeDestinationName(name: string): string {
    return name.replace(/,/g, '\\,').replace(/=/g, '\\=').replace(/:/g, '\\:');
  }

  /** Perform a Jolokia REST read of a single attribute. */
  private async jolokiaRead(objectName: string, attribute: string): Promise<unknown> {
    const url = `${this.jolokiaBaseUrl()}/read/${this.encodeObjectName(objectName)}/${encodeURIComponent(attribute)}`;
    return this.jolokiaRequest(url);
  }

  /** Perform a Jolokia REST exec of an operation. */
  private async jolokiaExec(objectName: string, operation: string, args: unknown[] = []): Promise<unknown> {
    const encodedArgs = args.map((a) => encodeURIComponent(String(a))).join(',');
    const path = `${this.encodeObjectName(objectName)}/${operation}(${encodedArgs})`;
    const url = `${this.jolokiaBaseUrl()}/exec/${path}`;
    return this.jolokiaRequest(url);
  }

  /** Dispatch a Jolokia REST request and inspect the envelope. */
  private async jolokiaRequest(url: string): Promise<unknown> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.restTimeoutMs);
    let response: Response;
    try {
      response = await fetch(url, {
        headers: { ...this.authHeader(), Accept: 'application/json' },
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }
    if (!response.ok) {
      const text = await response.text().catch(() => '');
      throw new Error(`Jolokia request failed (${response.status} ${response.statusText}): ${text}`);
    }
    const payload = (await response.json()) as JolokiaResponse;
    if (payload.error && !isPlainObject(payload.value)) {
      throw new Error(`Jolokia error: ${payload.error}`);
    }
    return payload.value;
  }

  // ─── STOMP operations ───────────────────────────────────────────────

  /**
   * Send a text message to a queue or topic.
   * Destination may be `/queue/name`, `/topic/name`, or a bare name (auto-prefixed as queue).
   */
  async sendMessage(
    destination: string,
    message: string,
    headers: Record<string, string> = {},
  ): Promise<void> {
    const dest = ActiveMQClient.normalizeDestination(destination);
    return this.withStompClient<void>(async (client) => {
      const sendHeaders: Record<string, string> = {
        destination: dest,
        'content-type': 'text/plain',
        ...headers,
      };
      await new Promise<void>((resolve, reject) => {
        const frame = client.send(sendHeaders);
        frame.on('error', (e: Error) => reject(e));
        frame.write(message);
        frame.end();
        frame.on('close', resolve);
      });
    });
  }

  /**
   * Receive messages from a queue or topic.
   * Subscribes and collects messages until the timeout elapses or maxMessages
   * is reached, then disconnects.
   */
  async receiveMessage(destination: string, opts: ReceiveMessageOptions = {}): Promise<ReceivedMessage[]> {
    const dest = ActiveMQClient.normalizeDestination(destination);
    const { timeoutMs = this.config.stompTimeoutMs, maxMessages = 1, ack = 'auto' } = opts;

    return this.withStompClient<ReceivedMessage[]>((client) => {
      return new Promise<ReceivedMessage[]>((resolve, reject) => {
        const messages: ReceivedMessage[] = [];
        let settled = false;
        const finish = (result: ReceivedMessage[]) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          resolve(result);
        };
        const timer = setTimeout(() => finish(messages), timeoutMs);

        client.subscribe({ destination: dest, ack }, (err, message) => {
          if (err) {
            if (!settled) {
              settled = true;
              clearTimeout(timer);
              reject(err);
            }
            return;
          }
          const headers = readMessageHeaders(message);
          let body = '';
          message.on('data', (chunk: Buffer) => {
            body += chunk.toString();
          });
          message.on('end', () => {
            messages.push({ body, headers });
            if (messages.length >= maxMessages) {
              finish(messages);
            }
          });
          message.on('error', (e: Error) => {
            if (!settled) {
              settled = true;
              clearTimeout(timer);
              reject(e);
            }
          });
        });
      });
    });
  }

  // ─── Jolokia (REST) operations ──────────────────────────────────────

  /** Non-destructive browse of a queue, returning up to `maxMessages` messages. */
  async browseQueue(queueName: string, maxMessages = 10): Promise<BrowsedMessage[]> {
    return this.browseDestination(queueName, 'Queue', maxMessages);
  }

  /** Non-destructive browse of a topic (subscribed consumers). */
  async browseTopic(topicName: string, maxMessages = 10): Promise<BrowsedMessage[]> {
    return this.browseDestination(topicName, 'Topic', maxMessages);
  }

  private async browseDestination(name: string, destinationType: DestinationType, maxMessages: number): Promise<BrowsedMessage[]> {
    const objectName = this.destinationObjectName(name, destinationType);
    const value = await this.jolokiaExec(objectName, 'browse');
    const list = Array.isArray(value) ? value : [];
    return list.slice(0, maxMessages).map((m) => ActiveMQClient.parseBrowsedMessage(m));
  }

  private static parseBrowsedMessage(m: unknown): BrowsedMessage {
    const record = isPlainObject(m) ? m : {};
    const headers: Record<string, string> = {};
    const messageId = record.messageId ?? record['JMSMessageId'] ?? record['message-id'];
    if (typeof messageId === 'string') {
      headers.messageId = messageId;
    }
    return { headers, messageId: typeof messageId === 'string' ? messageId : undefined, ...record };
  }

  /** List all queues as object-name strings. */
  async listQueues(): Promise<string[]> {
    const value = await this.jolokiaRead(this.brokerObjectName(), 'Queues');
    return ActiveMQClient.extractObjectNames(value);
  }

  /** List all topics as object-name strings. */
  async listTopics(): Promise<string[]> {
    const value = await this.jolokiaRead(this.brokerObjectName(), 'Topics');
    return ActiveMQClient.extractObjectNames(value);
  }

  private static extractObjectNames(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    return value
      .map((item) => {
        if (typeof item === 'string') return item;
        if (isPlainObject(item)) {
          const name = item.objectName;
          return typeof name === 'string' ? name : '';
        }
        return '';
      })
      .filter(Boolean);
  }

  /** Retrieve queue statistics. */
  async getQueueStats(queueName: string): Promise<QueueStats> {
    const objectName = this.destinationObjectName(queueName, 'Queue');
    return this.readDestinationStats(objectName);
  }

  /** Retrieve topic statistics. */
  async getTopicStats(topicName: string): Promise<QueueStats> {
    const objectName = this.destinationObjectName(topicName, 'Topic');
    return this.readDestinationStats(objectName);
  }

  private async readDestinationStats(objectName: string): Promise<QueueStats> {
    const value = await this.jolokiaRead(objectName, '');
    if (!isPlainObject(value)) return { ...ZERO_STATS };
    return {
      queueSize: toNumber(value.QueueSize),
      consumerCount: toNumber(value.ConsumerCount),
      producerCount: toNumber(value.ProducerCount),
      enqueueCount: toNumber(value.EnqueueCount),
      dequeueCount: toNumber(value.DequeueCount),
      dispatchCount: toNumber(value.DispatchCount),
      expiredCount: toNumber(value.ExpiredCount),
    };
  }

  /** Remove all messages from a queue. */
  async purgeQueue(queueName: string): Promise<void> {
    const objectName = this.destinationObjectName(queueName, 'Queue');
    await this.jolokiaExec(objectName, 'purge');
  }

  /** Create a new queue. */
  async createQueue(queueName: string): Promise<void> {
    await this.jolokiaExec(this.brokerObjectName(), 'createQueue', [queueName]);
  }

  /** Delete a queue and its messages. */
  async deleteQueue(queueName: string, purgeBeforeDelete = false): Promise<void> {
    await this.jolokiaExec(this.brokerObjectName(), 'removeQueue', [queueName, purgeBeforeDelete]);
  }

  /** Create a new topic. */
  async createTopic(topicName: string): Promise<void> {
    await this.jolokiaExec(this.brokerObjectName(), 'createTopic', [topicName]);
  }

  /** Delete a topic. */
  async deleteTopic(topicName: string): Promise<void> {
    await this.jolokiaExec(this.brokerObjectName(), 'removeTopic', [topicName]);
  }

  // ─── Helpers ────────────────────────────────────────────────────────

  /** Normalize a destination string into a STOMP `destination` header. */
  private static normalizeDestination(destination: string): string {
    const trimmed = destination.trim();
    if (trimmed.startsWith('/queue/') || trimmed.startsWith('/topic/')) {
      return trimmed;
    }
    return `/queue/${trimmed}`;
  }

  /** Extract the bare queue name from a destination or object name. */
  static extractQueueName(destination: string): string {
    const trimmed = destination.trim();
    if (trimmed.startsWith('/queue/')) return trimmed.slice('/queue/'.length);
    if (trimmed.startsWith('queue:')) return trimmed.slice('queue:'.length);
    return trimmed;
  }

  /** Health-check: attempt a Jolokia read of the broker object. */
  async ping(): Promise<boolean> {
    try {
      const value = await this.jolokiaRead(this.brokerObjectName(), '');
      return !!value && value !== null;
    } catch {
      return false;
    }
  }
}

export default ActiveMQClient;
