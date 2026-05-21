import { RUN_ERROR_CODES } from '@workflow/errors';
import {
  type Event,
  SPEC_VERSION_CURRENT,
  type WorkflowRun,
} from '@workflow/world';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { setWorld } from './runtime/world.js';
import { workflowEntrypoint } from './runtime.js';
import {
  dehydrateStepReturnValue,
  dehydrateWorkflowArguments,
  hydrateRunError,
  hydrateWorkflowReturnValue,
} from './serialization.js';

vi.mock('@vercel/functions', () => ({
  waitUntil: vi.fn(),
}));

async function runWorkflowHandlerWithEvents(
  workflowCode: string,
  workflowRun: WorkflowRun,
  events: Event[]
) {
  const createdEvents: unknown[] = [];
  const eventsCreate = vi.fn(async (_runId: string, data: any) => {
    createdEvents.push(data);

    if (data.eventType === 'run_started') {
      return {
        run: workflowRun,
        events,
      };
    }

    return {
      event: {
        eventId: `event-${createdEvents.length}`,
        runId: workflowRun.runId,
        createdAt: new Date(),
        ...data,
      },
    };
  });

  setWorld({
    specVersion: SPEC_VERSION_CURRENT,
    createQueueHandler: vi.fn(
      (
        _prefix: string,
        handler: (message: unknown, metadata: unknown) => Promise<unknown>
      ) => {
        return async () => {
          await handler(
            {
              runId: workflowRun.runId,
              requestedAt: new Date('2024-01-01T00:00:00.000Z'),
            },
            {
              requestId: 'req_test',
              attempt: 1,
              queueName: `__wkf_workflow_${workflowRun.workflowName}`,
              messageId: 'msg_test',
            }
          );
          return new Response(null, { status: 204 });
        };
      }
    ),
    events: {
      create: eventsCreate,
      list: vi.fn(async () => ({
        data: events,
        hasMore: false,
        cursor: 'cursor_test',
      })),
    },
    runs: {
      get: vi.fn(async () => workflowRun),
    },
    queue: vi.fn(),
    getEncryptionKeyForRun: vi.fn(async () => undefined),
  } as any);

  const handler = workflowEntrypoint(workflowCode);
  await handler(new Request('https://example.test'));

  return createdEvents;
}

describe('workflowEntrypoint replay guards', () => {
  afterEach(() => {
    setWorld(undefined);
    vi.clearAllMocks();
  });

  const getWorkflowTransformCode = (workflowName: string) =>
    `;globalThis.__private_workflows = new Map();
    globalThis.__private_workflows.set(${JSON.stringify(workflowName)}, ${workflowName});`;

  it('uses dynamic workflow code from the run executionContext when present', async () => {
    const ops: Promise<any>[] = [];
    const workflowRun: WorkflowRun = {
      runId: 'wrun_runtime_dynamic',
      workflowName: 'workflow//dynamic/test-run//workflow',
      status: 'running',
      input: await dehydrateWorkflowArguments(
        ['Ada'],
        'wrun_runtime_dynamic',
        undefined,
        ops
      ),
      executionContext: {
        dynamicWorkflow: {
          version: 1,
          sourceHash: 'hash',
          exportName: 'workflow',
          workflowCode: `
            async function workflow(name) {
              return "hello " + name;
            }
            ;globalThis.__private_workflows = new Map();
            globalThis.__private_workflows.set("workflow//dynamic/test-run//workflow", workflow);
          `,
        },
      },
      createdAt: new Date('2024-01-01T00:00:00.000Z'),
      updatedAt: new Date('2024-01-01T00:00:00.000Z'),
      startedAt: new Date('2024-01-01T00:00:00.000Z'),
      deploymentId: 'test-deployment',
    };

    const events: Event[] = [
      {
        eventId: 'event-0',
        runId: workflowRun.runId,
        eventType: 'run_created',
        eventData: {
          input: workflowRun.input,
          deploymentId: workflowRun.deploymentId,
          workflowName: workflowRun.workflowName,
          executionContext: workflowRun.executionContext,
        },
        createdAt: new Date('2024-01-01T00:00:00.000Z'),
        specVersion: SPEC_VERSION_CURRENT,
      },
      {
        eventId: 'event-1',
        runId: workflowRun.runId,
        eventType: 'run_started',
        createdAt: new Date('2024-01-01T00:00:00.000Z'),
        specVersion: SPEC_VERSION_CURRENT,
      },
    ];

    const createdEvents = await runWorkflowHandlerWithEvents(
      `function workflow() { throw new Error("static bundle should not run"); }${getWorkflowTransformCode('workflow')}`,
      workflowRun,
      events
    );

    const runCompleted = createdEvents.find(
      (event: any) => event.eventType === 'run_completed'
    ) as any;
    if (!runCompleted) {
      const runFailed = createdEvents.find(
        (event: any) => event.eventType === 'run_failed'
      ) as any;
      if (runFailed) {
        const error = await hydrateRunError(
          runFailed.eventData.error,
          workflowRun.runId,
          undefined,
          ops
        );
        throw error;
      }
    }
    expect(runCompleted).toBeDefined();
    expect(
      await hydrateWorkflowReturnValue(
        runCompleted.eventData.output,
        workflowRun.runId,
        undefined,
        ops
      )
    ).toBe('hello Ada');
  });

  it('records run_failed when a committed wait_completed targets the wrong wait', async () => {
    const ops: Promise<any>[] = [];
    const workflowRun: WorkflowRun = {
      runId: 'wrun_runtime_wait_guard',
      workflowName: 'workflow',
      status: 'running',
      input: await dehydrateWorkflowArguments(
        [],
        'wrun_runtime_wait_guard',
        undefined,
        ops
      ),
      createdAt: new Date('2024-01-01T00:00:00.000Z'),
      updatedAt: new Date('2024-01-01T00:00:00.000Z'),
      startedAt: new Date('2024-01-01T00:00:00.000Z'),
      deploymentId: 'test-deployment',
    };

    const events: Event[] = [
      {
        eventId: 'event-0',
        runId: workflowRun.runId,
        eventType: 'wait_created',
        correlationId: 'wait_01HK153X00GYR8SV1JHHTGN5HE',
        eventData: {
          resumeAt: new Date('2024-01-01T00:00:05.000Z'),
        },
        createdAt: new Date('2024-01-01T00:00:00.000Z'),
      },
      {
        eventId: 'event-1',
        runId: workflowRun.runId,
        eventType: 'wait_completed',
        correlationId: 'wait_01HK153X00GYR8SV1JHHTGN5HE',
        eventData: {
          resumeAt: new Date('2024-01-01T00:00:06.000Z'),
        },
        createdAt: new Date('2024-01-01T00:00:05.000Z'),
      },
    ];

    const createdEvents = await runWorkflowHandlerWithEvents(
      `const sleep = globalThis[Symbol.for("WORKFLOW_SLEEP")];
      async function workflow() {
        await sleep('5s');
        return 'done';
      }${getWorkflowTransformCode('workflow')}`,
      workflowRun,
      events
    );

    expect(createdEvents).toContainEqual(
      expect.objectContaining({
        eventType: 'run_failed',
        eventData: expect.objectContaining({
          errorCode: RUN_ERROR_CODES.RUNTIME_ERROR,
        }),
      })
    );
  });

  it('records run_failed when a committed hook_received targets the wrong hook', async () => {
    const ops: Promise<any>[] = [];
    const workflowRun: WorkflowRun = {
      runId: 'wrun_runtime_hook_guard',
      workflowName: 'workflow',
      status: 'running',
      input: await dehydrateWorkflowArguments(
        [],
        'wrun_runtime_hook_guard',
        undefined,
        ops
      ),
      createdAt: new Date('2024-01-01T00:00:00.000Z'),
      updatedAt: new Date('2024-01-01T00:00:00.000Z'),
      startedAt: new Date('2024-01-01T00:00:00.000Z'),
      deploymentId: 'test-deployment',
    };

    const events: Event[] = [
      {
        eventId: 'event-0',
        runId: workflowRun.runId,
        eventType: 'hook_received',
        correlationId: 'hook_01HK153X00GYR8SV1JHHTGN5HE',
        eventData: {
          token: 'wrong-token',
          payload: await dehydrateStepReturnValue(
            { message: 'hello' },
            'wrun_runtime_hook_guard',
            undefined,
            ops
          ),
        },
        createdAt: new Date('2024-01-01T00:00:00.000Z'),
      },
    ];

    const createdEvents = await runWorkflowHandlerWithEvents(
      `const createHook = globalThis[Symbol.for("WORKFLOW_CREATE_HOOK")];
      async function workflow() {
        const hook = createHook({ token: 'expected-token' });
        const payload = await hook;
        return payload.message;
      }${getWorkflowTransformCode('workflow')}`,
      workflowRun,
      events
    );

    expect(createdEvents).toContainEqual(
      expect.objectContaining({
        eventType: 'run_failed',
        eventData: expect.objectContaining({
          errorCode: RUN_ERROR_CODES.RUNTIME_ERROR,
        }),
      })
    );
  });
});
