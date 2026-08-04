import { z } from 'zod/v4';
import {
  MessageId,
  type QueueHandler,
  type QueuePrefix,
  ValidQueueName,
} from './queue.js';

/**
 * JSON replacer/reviver preserving Uint8Array values via a tagged base64
 * envelope ({ __type: 'Uint8Array', data: '<base64>' }) so binary data (e.g.
 * workflow run inputs) survives JSON transports and storage.
 */
export function jsonReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return {
      __type: 'Uint8Array',
      data: Buffer.from(value).toString('base64'),
    };
  }
  return value;
}

export function jsonReviver(_key: string, value: unknown): unknown {
  if (
    value !== null &&
    typeof value === 'object' &&
    (value as any).__type === 'Uint8Array' &&
    typeof (value as any).data === 'string'
  ) {
    return new Uint8Array(Buffer.from((value as any).data, 'base64'));
  }
  return value;
}

/** JSON codec for queue message bodies, built on the replacer/reviver above. */
export function serializeQueueMessage(message: unknown): Buffer {
  return Buffer.from(JSON.stringify(message, jsonReplacer));
}

export function deserializeQueueMessage(data: Uint8Array): unknown {
  // View (not copy) the bytes when possible — job payloads can be large.
  const buffer = Buffer.isBuffer(data)
    ? data
    : Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  return JSON.parse(buffer.toString(), jsonReviver);
}

const QueueMessageHeaders = z.object({
  'x-vqs-queue-name': ValidQueueName,
  'x-vqs-message-id': MessageId,
  'x-vqs-message-attempt': z.coerce.number(),
});

/**
 * Wraps a queue handler in the HTTP delivery contract for the flow route:
 * `x-vqs-*` headers and a serialized message body in; `{ ok: true }`,
 * `{ timeoutSeconds }` (redeliver after that many seconds), or a 500 out.
 *
 * This is the reference `Queue['createQueueHandler']` implementation for
 * worlds that deliver queue messages over HTTP.
 */
export function createFetchQueueHandler(
  prefix: QueuePrefix,
  handler: QueueHandler
): (req: Request) => Promise<Response> {
  return async (req) => {
    const headers = QueueMessageHeaders.safeParse(
      Object.fromEntries(req.headers)
    );
    if (!headers.success || !req.body) {
      return Response.json(
        {
          error: !req.body
            ? 'Missing request body'
            : 'Missing required headers',
        },
        { status: 400 }
      );
    }

    const queueName = headers.data['x-vqs-queue-name'];
    if (!queueName.startsWith(prefix)) {
      return Response.json({ error: 'Unhandled queue' }, { status: 400 });
    }

    const message = deserializeQueueMessage(
      new Uint8Array(await req.arrayBuffer())
    );
    try {
      const result = await handler(message, {
        attempt: headers.data['x-vqs-message-attempt'],
        queueName,
        messageId: headers.data['x-vqs-message-id'],
      });
      return result
        ? Response.json({ timeoutSeconds: result.timeoutSeconds })
        : Response.json({ ok: true });
    } catch (error) {
      return Response.json(String(error), { status: 500 });
    }
  };
}
