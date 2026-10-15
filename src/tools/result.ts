/**
 * Shared response helpers for MCP tools.
 *
 * Every tool returns an MCP `CallToolResult`-shaped object so tool modules
 * own their response formatting and `index.ts` stays a thin registration layer.
 */
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';

export type ToolResponse = CallToolResult;

/** Wrap a plain-text payload as a successful tool result. */
export function textResponse(text: string): ToolResponse {
  return { content: [{ type: 'text', text }] };
}

/** Wrap an error message as a failed (isError) tool result. */
export function errorResponse(message: string): ToolResponse {
  return { content: [{ type: 'text', text: message }], isError: true };
}

/** Summarize a list of strings as a human-readable, numbered result. */
export function listResponse(label: string, items: unknown[]): ToolResponse {
  if (items.length === 0) {
    return textResponse(`No ${label} found.`);
  }
  const lines = items.map((item, i) => {
    if (typeof item === 'string') return `${i + 1}. ${item}`;
    return `${i + 1}. ${JSON.stringify(item)}`;
  });
  return textResponse(`${items.length} ${label}:\n${lines.join('\n')}`);
}
