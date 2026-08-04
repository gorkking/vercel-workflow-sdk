import { describe, expect, it, vi } from 'vitest';
import {
  createFetchQueueHandler,
  deserializeQueueMessage,
  serializeQueueMessage,
} from './queue-http.js';

describe('queue message codec', () => {
  it('round-trips Uint8Array values through JSON', () => {
    const message = {
      runId: 'wrun_1',
      runInput: { input: new Uint8Array([1, 2, 3]) },
    };
    const decoded = deserializeQueueMessage(
      serializeQueueMessage(message)
    ) as typeof message;
    expect(decoded.runId).toBe('wrun_1');
    expect(decoded.runInput.input).toBeInstanceOf(Uint8Array);
    expect([...decoded.runInput.input]).toEqual([1, 2, 3]);
  });
});

describe('createFetchQueueHandler', () => {
  const makeRequest = (headers: Record<string, string>) =>
    new Request('http://localhost/.well-known/workflow/v1/flow', {
      method: 'POST',
      headers,
      body: serializeQueueMessage({ runId: 'wrun_1' }),
    });
  const headers = {
    'x-vqs-queue-name': '__wkf_workflow_test',
    'x-vqs-message-id': 'msg_1',
    'x-vqs-message-attempt': '2',
  };

  it('invokes the handler with the parsed message and metadata', async () => {
    const handler = vi.fn(async () => undefined);
    const fetchHandler = createFetchQueueHandler('__wkf_workflow_', handler);

    const response = await fetchHandler(makeRequest(headers));

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({ ok: true });
    expect(handler).toHaveBeenCalledWith(
      { runId: 'wrun_1' },
      {
        attempt: 2,
        queueName: '__wkf_workflow_test',
        messageId: 'msg_1',
      }
    );
  });

  it('returns timeoutSeconds when the handler reschedules', async () => {
    const fetchHandler = createFetchQueueHandler(
      '__wkf_workflow_',
      async () => ({
        timeoutSeconds: 30,
      })
    );

    const response = await fetchHandler(makeRequest(headers));

    await expect(response.json()).resolves.toEqual({ timeoutSeconds: 30 });
  });

  it('rejects requests without queue headers', async () => {
    const fetchHandler = createFetchQueueHandler(
      '__wkf_workflow_',
      async () => undefined
    );

    const response = await fetchHandler(makeRequest({}));

    expect(response.status).toBe(400);
  });

  it('rejects queue names outside the handled prefix', async () => {
    const fetchHandler = createFetchQueueHandler(
      '__other_wkf_workflow_',
      async () => undefined
    );

    const response = await fetchHandler(makeRequest(headers));

    expect(response.status).toBe(400);
  });

  it('maps handler errors to a 500 response', async () => {
    const fetchHandler = createFetchQueueHandler(
      '__wkf_workflow_',
      async () => {
        throw new Error('boom');
      }
    );

    const response = await fetchHandler(makeRequest(headers));

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toContain('boom');
  });
});
