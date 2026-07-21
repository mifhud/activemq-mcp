# Plan detail membuat MCP ActiveMQ

Plan ini dibuat dengan bahasa Indonesia. Tetapi ketika implement ke code, wajib menggunakan bahasa Inggris.

````plaintext
Thought process: Merancang arsitektur MCP server untuk integrasi ActiveMQ.

Pengguna meminta rencana detail untuk membuat MCP (Model Context Protocol) untuk ActiveMQ. Ini adalah permintaan teknis untuk membuat integrasi antara Claude/LLM dengan Apache ActiveMQ message broker.

Saya perlu memberikan panduan komprehensif tentang cara membuat MCP server untuk ActiveMQ. Mari saya pikirkan komponen-komponen yang diperlukan:

1. Pemahaman MCP - Model Context Protocol adalah standar untuk menghubungkan AI dengan berbagai tools/data sources
2. ActiveMQ - Message broker dari Apache untuk messaging
3. Implementasi MCP server yang bisa berinteraksi dengan ActiveMQ

Saya akan memberikan plan detail termasuk:
- Arsitektur
- Tools/resources yang perlu diexpose
- Implementasi code
- Konfigurasi

Saya bisa membuat dokumen detail atau langsung menjelaskan. Karena ini adalah permintaan untuk "plan detail", saya akan memberikan penjelasan komprehensif.

Saya tidak perlu mencari di web karena ini adalah pengetahuan teknis yang sudah saya miliki tentang MCP dan ActiveMQ.
````

# Plan Detail Membuat MCP ActiveMQ

## 1. Overview

**MCP (Model Context Protocol)** adalah protokol standar dari Anthropic untuk menghubungkan AI/LLM dengan berbagai data sources dan tools. Dengan membuat MCP ActiveMQ, Claude dapat berinteraksi langsung dengan Apache ActiveMQ message broker.

---

## 2. Arsitektur

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Claude    │────▶│  MCP Server     │────▶│   ActiveMQ      │
│   (Client)  │◀────│  (ActiveMQ)     │◀────│   Broker        │
└─────────────┘     └─────────────────┘     └─────────────────┘
                           │
                    ┌──────┴──────┐
                    │  STOMP /    │
                    │  REST API   │
                    └─────────────┘
```

---

## 3. Fitur/Tools yang Akan Diexpose

| Tool | Deskripsi |
|------|-----------|
| `send_message` | Mengirim pesan ke queue/topic |
| `receive_message` | Menerima pesan dari queue/topic |
| `browse_queue` | Melihat pesan dalam queue tanpa consume |
| `list_queues` | Mendaftar semua queues |
| `list_topics` | Mendaftar semua topics |
| `get_queue_stats` | Mendapat statistik queue (size, consumers, dll) |
| `purge_queue` | Menghapus semua pesan dari queue |
| `create_queue` | Membuat queue baru |
| `delete_queue` | Menghapus queue |

---

## 4. Tech Stack

- **Runtime**: Node.js (TypeScript) atau Python
- **MCP SDK**: `@modelcontextprotocol/sdk` (Node) atau `mcp` (Python)
- **ActiveMQ Client**: 
  - Node: `stompit` atau `rhea` (AMQP)
  - Python: `stomp.py` atau `pika`
- **ActiveMQ API**: REST API via Jolokia

---

## 5. Struktur Project

```
activemq-mcp/
├── src/
│   ├── index.ts              # Entry point MCP server
│   ├── activemq-client.ts    # ActiveMQ connection handler
│   ├── tools/
│   │   ├── send-message.ts
│   │   ├── receive-message.ts
│   │   ├── browse-queue.ts
│   │   ├── list-queues.ts
│   │   └── queue-stats.ts
│   ├── resources/
│   │   └── queue-resource.ts
│   └── types.ts
├── package.json
├── tsconfig.json
└── README.md
```

---

## 6. Implementasi Step-by-Step

### Step 1: Inisialisasi Project

```bash
mkdir activemq-mcp
cd activemq-mcp
npm init -y
npm install @modelcontextprotocol/sdk stompit zod
npm install -D typescript @types/node tsx
```

### Step 2: Konfigurasi TypeScript

```json
// tsconfig.json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true
  },
  "include": ["src/**/*"]
}
```

### Step 3: ActiveMQ Client

```typescript
// src/activemq-client.ts
import stompit from 'stompit';

export interface ActiveMQConfig {
  host: string;
  port: number;
  username: string;
  password: string;
}

export class ActiveMQClient {
  private config: ActiveMQConfig;
  
  constructor(config: ActiveMQConfig) {
    this.config = config;
  }

  private getConnectionOptions(): stompit.ConnectOptions {
    return {
      host: this.config.host,
      port: this.config.port,
      connectHeaders: {
        host: '/',
        login: this.config.username,
        passcode: this.config.password,
        'heart-beat': '5000,5000'
      }
    };
  }

  async sendMessage(destination: string, body: string, headers?: Record<string, string>): Promise<void> {
    return new Promise((resolve, reject) => {
      stompit.connect(this.getConnectionOptions(), (error, client) => {
        if (error) return reject(error);
        
        const sendHeaders = {
          destination,
          'content-type': 'text/plain',
          ...headers
        };
        
        const frame = client.send(sendHeaders);
        frame.write(body);
        frame.end();
        
        client.disconnect();
        resolve();
      });
    });
  }

  async browseQueue(queueName: string, maxMessages: number = 10): Promise<any[]> {
    // Implementasi browse menggunakan REST API Jolokia
    const url = `http://${this.config.host}:8161/api/jolokia/exec/org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=${queueName}/browse()`;
    
    const response = await fetch(url, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${this.config.username}:${this.config.password}`).toString('base64')
      }
    });
    
    const data = await response.json();
    return data.value?.slice(0, maxMessages) || [];
  }

  async listQueues(): Promise<string[]> {
    const url = `http://${this.config.host}:8161/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost/Queues`;
    
    const response = await fetch(url, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${this.config.username}:${this.config.password}`).toString('base64')
      }
    });
    
    const data = await response.json();
    return data.value?.map((q: any) => q.objectName) || [];
  }

  async getQueueStats(queueName: string): Promise<object> {
    const url = `http://${this.config.host}:8161/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=${queueName}`;
    
    const response = await fetch(url, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${this.config.username}:${this.config.password}`).toString('base64')
      }
    });
    
    const data = await response.json();
    return {
      queueSize: data.value?.QueueSize,
      consumerCount: data.value?.ConsumerCount,
      producerCount: data.value?.ProducerCount,
      enqueueCount: data.value?.EnqueueCount,
      dequeueCount: data.value?.DequeueCount
    };
  }
}
```

### Step 4: MCP Server Implementation

```typescript
// src/index.ts
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { ActiveMQClient } from './activemq-client.js';

// Schema definitions
const SendMessageSchema = z.object({
  destination: z.string().describe('Queue atau topic tujuan (e.g., /queue/myqueue)'),
  message: z.string().describe('Isi pesan yang akan dikirim'),
  headers: z.record(z.string()).optional().describe('Header tambahan')
});

const BrowseQueueSchema = z.object({
  queueName: z.string().describe('Nama queue yang akan di-browse'),
  maxMessages: z.number().optional().default(10).describe('Jumlah maksimal pesan')
});

const QueueStatsSchema = z.object({
  queueName: z.string().describe('Nama queue')
});

// Initialize client
const client = new ActiveMQClient({
  host: process.env.ACTIVEMQ_HOST || 'localhost',
  port: parseInt(process.env.ACTIVEMQ_PORT || '61613'),
  username: process.env.ACTIVEMQ_USERNAME || 'admin',
  password: process.env.ACTIVEMQ_PASSWORD || 'admin'
});

// Create server
const server = new Server(
  {
    name: 'activemq-mcp',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
      resources: {},
    },
  }
);

// List available tools
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: 'send_message',
        description: 'Mengirim pesan ke ActiveMQ queue atau topic',
        inputSchema: {
          type: 'object',
          properties: {
            destination: { type: 'string', description: 'Queue/topic tujuan' },
            message: { type: 'string', description: 'Isi pesan' },
            headers: { type: 'object', description: 'Header tambahan' }
          },
          required: ['destination', 'message']
        }
      },
      {
        name: 'browse_queue',
        description: 'Melihat pesan dalam queue tanpa mengkonsumsinya',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: { type: 'string', description: 'Nama queue' },
            maxMessages: { type: 'number', description: 'Jumlah maksimal pesan' }
          },
          required: ['queueName']
        }
      },
      {
        name: 'list_queues',
        description: 'Mendaftar semua queue yang tersedia',
        inputSchema: { type: 'object', properties: {} }
      },
      {
        name: 'get_queue_stats',
        description: 'Mendapatkan statistik queue',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: { type: 'string', description: 'Nama queue' }
          },
          required: ['queueName']
        }
      },
      {
        name: 'purge_queue',
        description: 'Menghapus semua pesan dari queue',
        inputSchema: {
          type: 'object',
          properties: {
            queueName: { type: 'string', description: 'Nama queue' }
          },
          required: ['queueName']
        }
      }
    ]
  };
});

// Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    switch (name) {
      case 'send_message': {
        const { destination, message, headers } = SendMessageSchema.parse(args);
        await client.sendMessage(destination, message, headers);
        return {
          content: [{ type: 'text', text: `Pesan berhasil dikirim ke ${destination}` }]
        };
      }

      case 'browse_queue': {
        const { queueName, maxMessages } = BrowseQueueSchema.parse(args);
        const messages = await client.browseQueue(queueName, maxMessages);
        return {
          content: [{ 
            type: 'text', 
            text: JSON.stringify(messages, null, 2) 
          }]
        };
      }

      case 'list_queues': {
        const queues = await client.listQueues();
        return {
          content: [{ type: 'text', text: JSON.stringify(queues, null, 2) }]
        };
      }

      case 'get_queue_stats': {
        const { queueName } = QueueStatsSchema.parse(args);
        const stats = await client.getQueueStats(queueName);
        return {
          content: [{ type: 'text', text: JSON.stringify(stats, null, 2) }]
        };
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (error) {
    return {
      content: [{ type: 'text', text: `Error: ${error}` }],
      isError: true
    };
  }
});

// Start server
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('MCP ActiveMQ server running on stdio');
}

main().catch(console.error);
```

### Step 5: Package.json Configuration

```json
{
  "name": "activemq-mcp",
  "version": "1.0.0",
  "type": "module",
  "main": "dist/index.js",
  "bin": {
    "activemq-mcp": "dist/index.js"
  },
  "scripts": {
    "build": "tsc",
    "start": "node dist/index.js",
    "dev": "tsx src/index.ts"
  },
  "dependencies": {
    "@modelcontextprotocol/sdk": "^1.0.0",
    "stompit": "^1.0.0",
    "zod": "^3.22.0"
  },
  "devDependencies": {
    "@types/node": "^20.0.0",
    "typescript": "^5.0.0",
    "tsx": "^4.0.0"
  }
}
```

---

## 7. Konfigurasi MCP Client

### Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `ACTIVEMQ_HOST` | `localhost` | Hostname broker ActiveMQ |
| `ACTIVEMQ_PORT` | tergantung protocol | Port koneksi broker |
| `ACTIVEMQ_PROTOCOL` | `amqp` | `amqp`, `stomp`, atau `openwire` |
| `ACTIVEMQ_WEB_PORT` | `8161` | Port web console / Jolokia |
| `ACTIVEMQ_USERNAME` | `admin` | Username broker |
| `ACTIVEMQ_PASSWORD` | `admin` | Password broker |

**Default port per protocol:**

| Protocol | Default Port |
|----------|-------------|
| `amqp` | 5672 |
| `stomp` | 61613 |
| `openwire` | 61616 |

> Openwire tidak punya native library di Node.js, jadi pakai STOMP via `stompit` sebagai transport ke port OpenWire. Pastikan broker mengaktifkan AUTO transport atau STOMP connector di port 61616.

### Claude Desktop

```json
{
  "mcpServers": {
    "activemq": {
      "command": "node",
      "args": ["/path/to/activemq-mcp/dist/index.js"],
      "env": {
        "ACTIVEMQ_HOST": "localhost",
        "ACTIVEMQ_PORT": "61616",
        "ACTIVEMQ_PROTOCOL": "openwire",
        "ACTIVEMQ_WEB_PORT": "8161",
        "ACTIVEMQ_USERNAME": "admin",
        "ACTIVEMQ_PASSWORD": "admin"
      }
    }
  }
}
```

### OpenCode / Codex

```json
{
  "mcpServers": {
    "activemq": {
      "type": "local",
      "command": ["node", "/path/to/activemq-mcp/dist/index.js"],
      "environment": {
        "ACTIVEMQ_HOST": "localhost",
        "ACTIVEMQ_PROTOCOL": "openwire",
        "ACTIVEMQ_PORT": "61616",
        "ACTIVEMQ_WEB_PORT": "8161",
        "ACTIVEMQ_USERNAME": "admin",
        "ACTIVEMQ_PASSWORD": "admin"
      }
    }
  }
}
```

---

## 8. Testing

```bash
# Build
npm run build

# Test manual dengan MCP Inspector
npx @modelcontextprotocol/inspector node dist/index.js
```