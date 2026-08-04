import { z } from 'zod/v4';
import {
  MessageId,
  type QueueHandler,
  type QueuePrefix,
  ValidQueueName,
} from './queue.js';

/**
 * JSON codec for queue message bodies. Preserves Uint8Array values (e.g.
 * workflow run inputs) via a tagged base64 envelope so binary data survives
 * JSON transports.
 */
export function serializeQueueMessage(message: unknown): Buffer {
  return Buffer.from(
    JSON.stringify(message, (_key, value) =>
      value instanceof Uint8Array
        ? { __type: 'Uint8Array', data: Buffer.from(value).toString('base64') }
        : value
    )
  );
}

export function deserializeQueueMessage(data: Uint8Array): unknown {
  return JSON.parse(Buffer.from(data).toString(), (_key, value) =>
    value !== null &&
    typeof value === 'object' &&
    value.__type === 'Uint8Array' &&
    typeof value.data === 'string'
      ? new Uint8Array(Buffer.from(value.data, 'base64'))
      : value
  );
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
