import {
  MessageId,
  parseQueueName,
  type QueueHandler,
  type QueuePayload,
  serializeQueueMessage,
} from '@workflow/world';
import {
  makeWorkerUtils,
  type Runner,
  run,
  type WorkerUtils,
} from 'graphile-worker';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MessageData } from './message.js';
import { createQueue, handlerRegistry } from './queue.js';

const createdQueues: Array<ReturnType<typeof createQueue>> = [];

vi.mock('graphile-worker', () => ({
  Logger: class Logger {
    constructor(_: unknown) {}
  },
  makeWorkerUtils: vi.fn(),
  run: vi.fn(),
}));

describe('postgres queue direct execution', () => {
  const workerUtilsMock = {
    addJob: vi.fn(),
    migrate: vi.fn(),
    release: vi.fn(),
  } as unknown as WorkerUtils;
  const runnerMock = {
    stop: vi.fn(),
    promise: Promise.resolve(),
  };
  const pool = {
    query: vi.fn(async () => ({ rows: [{ exists: false }] })),
  } as any;

  beforeEach(() => {
    vi.clearAllMocks();
    // The handler registry is process-global (shared across module copies),
    // so tests must reset it between runs.
    handlerRegistry.handlers.clear();

    vi.mocked(makeWorkerUtils).mockResolvedValue(workerUtilsMock);
    vi.mocked(run).mockResolvedValue(runnerMock as unknown as Runner);
  });

  afterEach(async () => {
    await Promise.all(createdQueues.splice(0).map((queue) => queue.close()));
    vi.useRealTimers();
  });

  it('does not start the runner until a workflow handler is registered', async () => {
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    await queue.start();

    expect(run).not.toHaveBeenCalled();

    queue.createQueueHandler('__wkf_workflow_', async () => undefined);
    await vi.waitFor(() => {
      expect(run).toHaveBeenCalledTimes(1);
    });
  });

  it('starts the runner immediately when a handler is already registered', async () => {
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', async () => undefined);

    await queue.start();

    expect(run).toHaveBeenCalledTimes(1);
  });

  it('keeps Graphile Worker automatic shutdown by default', async () => {
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', async () => undefined);

    await queue.start();

    expect(run).toHaveBeenCalledWith(
      expect.not.objectContaining({ noHandleSignals: true })
    );
  });

  it('allows the application to manage shutdown', async () => {
    const queue = buildQueue(
      {
        connectionString: 'postgres://test',
        applicationManagedShutdown: true,
      },
      pool
    );
    queue.createQueueHandler('__wkf_workflow_', async () => undefined);

    await queue.start();

    expect(run).toHaveBeenCalledWith(
      expect.objectContaining({ noHandleSignals: true })
    );
  });

  it('executes jobs by invoking the registered handler directly', async () => {
    const handler = vi.fn<QueueHandler>(async () => undefined);
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', handler);
    await queue.start();

    const message = {
      runId: 'run_01ABC',
      stepId: 'step_01ABC',
      stepName: 'test-step',
    } satisfies QueuePayload;
    const payload = buildMessageData('__wkf_workflow_test-step', message, {
      idempotencyKey: 'step_01ABC',
    });

    await expect(
      getTaskHandler()(payload, { job: { attempts: 1 } })
    ).resolves.toBeUndefined();

    expect(handler).toHaveBeenCalledWith(message, {
      attempt: 1,
      queueName: '__wkf_workflow_test-step',
      messageId: 'msg_01ABC',
    });
  });

  it('routes namespaced jobs to the namespaced handler', async () => {
    const handler = vi.fn<QueueHandler>(async () => undefined);
    const queue = buildQueue(
      { connectionString: 'postgres://test', namespace: 'custom' },
      pool
    );
    queue.createQueueHandler('__custom_wkf_workflow_', handler);
    await queue.start();

    const message = { runId: 'run_01ABC' } satisfies QueuePayload;
    await getTaskHandler()(
      buildMessageData('__custom_wkf_workflow_test-workflow', message),
      { job: { attempts: 1 } }
    );

    expect(handler).toHaveBeenCalledWith(
      message,
      expect.objectContaining({
        queueName: '__custom_wkf_workflow_test-workflow',
      })
    );
  });

  it('executes health-check payloads that carry no runId', async () => {
    const handler = vi.fn<QueueHandler>(async () => undefined);
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', handler);
    await queue.start();

    const message = { __healthCheck: true, correlationId: 'hc_01ABC' };
    await getTaskHandler()(
      buildMessageData('__wkf_workflow_health_check', message as QueuePayload),
      { job: { attempts: 1 } }
    );

    expect(handler).toHaveBeenCalledWith(message, expect.anything());
  });

  it('schedules a replacement job when the handler reschedules', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', async () => ({
      timeoutSeconds: 30,
    }));
    await queue.start();

    await getTaskHandler()(
      buildMessageData(
        '__wkf_workflow_test-step',
        { runId: 'run_01ABC', stepId: 'step_01ABC', stepName: 'test-step' },
        { idempotencyKey: 'step_01ABC' }
      ),
      { job: { attempts: 1 } }
    );

    expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
      'workflow_flows',
      expect.objectContaining({
        attempt: 2,
        id: 'test-step',
        idempotencyKey: 'step_01ABC',
        messageId: 'msg_01ABC',
      }),
      expect.objectContaining({
        jobKey: 'step_01ABC',
        maxAttempts: 3,
        runAt: new Date('2024-01-01T00:00:30.000Z'),
      })
    );
  });

  it('sums the stored attempt with graphile retry attempts', async () => {
    const handler = vi.fn<QueueHandler>(async () => undefined);
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', handler);
    await queue.start();

    await getTaskHandler()(
      buildMessageData(
        '__wkf_workflow_test-workflow',
        { runId: 'run_01ABC' },
        { attempt: 3 }
      ),
      { job: { attempts: 2 } }
    );

    expect(handler).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ attempt: 4 })
    );
  });

  it('propagates handler errors so graphile retries the job', async () => {
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', async () => {
      throw new Error('replay failed');
    });
    await queue.start();

    await expect(
      getTaskHandler()(
        buildMessageData('__wkf_workflow_test-workflow', {
          runId: 'run_01ABC',
        }),
        { job: { attempts: 1 } }
      )
    ).rejects.toThrow('replay failed');

    expect(workerUtilsMock.addJob).not.toHaveBeenCalled();
  });

  it('deduplicates completed idempotency keys', async () => {
    const handler = vi.fn<QueueHandler>(async () => undefined);
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', handler);
    await queue.start();

    const payload = buildMessageData(
      '__wkf_workflow_test-step',
      { runId: 'run_01ABC', stepId: 'step_01ABC', stepName: 'test-step' },
      { idempotencyKey: 'step_01ABC' }
    );
    const task = getTaskHandler();
    await task(payload, { job: { attempts: 1 } });
    await task(payload, { job: { attempts: 2 } });

    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('serializes workflow replays for the same runId', async () => {
    let resolveFirstExecution!: () => void;
    const firstExecutionStarted = Promise.withResolvers<void>();
    let activeExecutions = 0;
    let maxActiveExecutions = 0;
    const handler = vi.fn<QueueHandler>(async () => {
      activeExecutions += 1;
      maxActiveExecutions = Math.max(maxActiveExecutions, activeExecutions);
      if (handler.mock.calls.length === 1) {
        firstExecutionStarted.resolve();
        await new Promise<void>((resolve) => {
          resolveFirstExecution = resolve;
        });
      }
      activeExecutions -= 1;
      return undefined;
    });
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    queue.createQueueHandler('__wkf_workflow_', handler);
    await queue.start();

    const task = getTaskHandler();
    const message = { runId: 'wrun_01ABC' };
    const first = task(
      buildMessageData('__wkf_workflow_test-workflow', message, {
        messageId: MessageId.parse('msg_01ABC'),
      }),
      { job: { attempts: 1 } }
    );
    const second = task(
      buildMessageData('__wkf_workflow_test-workflow', message, {
        messageId: MessageId.parse('msg_01ABD'),
      }),
      { job: { attempts: 1 } }
    );

    await firstExecutionStarted.promise;
    await Promise.resolve();
    expect(handler).toHaveBeenCalledTimes(1);

    resolveFirstExecution();
    await Promise.all([first, second]);

    expect(handler).toHaveBeenCalledTimes(2);
    expect(maxActiveExecutions).toBe(1);
  });

  it('queues producer delays and headers in graphile job metadata', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);

    await queue.queue(
      '__wkf_workflow_test-step',
      {
        runId: 'run_01ABC',
        stepId: 'step_01ABC',
        stepName: 'test-step',
      },
      {
        delaySeconds: 5,
        headers: { traceparent: 'trace-parent' },
        idempotencyKey: 'step_01ABC',
      }
    );

    expect(run).not.toHaveBeenCalled();
    expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
      'workflow_flows',
      expect.objectContaining({
        attempt: 1,
        headers: { traceparent: 'trace-parent' },
        id: 'test-step',
        idempotencyKey: 'step_01ABC',
      }),
      expect.objectContaining({
        jobKey: 'step_01ABC',
        maxAttempts: 3,
        runAt: new Date('2024-01-01T00:00:05.000Z'),
      })
    );
  });

  it('rejects queueing after close', async () => {
    const queue = buildQueue({ connectionString: 'postgres://test' }, pool);
    await queue.close();

    await expect(
      queue.queue('__wkf_workflow_test-workflow', { runId: 'run_01ABC' })
    ).rejects.toThrow('closed');
  });
});

function buildQueue(
  config: Parameters<typeof createQueue>[0],
  pgPool: Parameters<typeof createQueue>[1]
) {
  const queue = createQueue(config, pgPool);
  createdQueues.push(queue);
  return queue;
}

function buildMessageData(
  queueName: string,
  payload: QueuePayload,
  opts?: {
    attempt?: number;
    headers?: Record<string, string>;
    idempotencyKey?: string;
    messageId?: MessageId;
  }
) {
  const { id } = parseQueueName(queueName);

  return MessageData.encode({
    id,
    data: serializeQueueMessage(payload),
    attempt: opts?.attempt ?? 1,
    headers: opts?.headers,
    idempotencyKey: opts?.idempotencyKey,
    messageId: opts?.messageId ?? MessageId.parse('msg_01ABC'),
  });
}

function getTaskHandler() {
  const taskList = vi.mocked(run).mock.calls[0]?.[0]?.taskList;
  const task = taskList?.workflow_flows;
  expect(task).toBeTypeOf('function');
  return task as (payload: unknown, helpers: unknown) => Promise<void>;
}
