import type { World } from '@workflow/world';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const {
  mockDehydrateStepError,
  mockEventsCreate,
  mockLoadStepFunction,
  mockRuntimeLogger,
  mockStepLogger,
} = vi.hoisted(() => ({
  mockDehydrateStepError: vi.fn().mockResolvedValue(new Uint8Array([4, 5, 6])),
  mockEventsCreate: vi.fn(),
  mockLoadStepFunction: vi.fn(),
  mockRuntimeLogger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
  mockStepLogger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock('@vercel/functions', () => ({
  waitUntil: vi.fn(),
}));

vi.mock('../private.js', () => ({
  loadStepFunction: mockLoadStepFunction,
}));

vi.mock('../logger.js', () => ({
  runtimeLogger: mockRuntimeLogger,
  stepLogger: mockStepLogger,
}));

vi.mock('../serialization.js', async () => {
  const actual = await vi.importActual<typeof import('../serialization.js')>(
    '../serialization.js'
  );
  return {
    ...actual,
    dehydrateStepError: (...args: unknown[]) => mockDehydrateStepError(...args),
  };
});

vi.mock('../telemetry.js', () => ({
  trace: vi.fn((_name: string, _opts: unknown, fn?: unknown) => {
    const callback = typeof _opts === 'function' ? _opts : fn;
    return (callback as (span?: undefined) => unknown)(undefined);
  }),
}));

import { executeStep } from './step-executor.js';

describe('executeStep', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDehydrateStepError.mockResolvedValue(new Uint8Array([4, 5, 6]));
    mockEventsCreate.mockImplementation(
      (_runId: string, event: { eventType: string }) => {
        if (event.eventType === 'step_started') {
          return Promise.resolve({
            step: {
              stepId: 'step_abc',
              status: 'running',
              attempt: 1,
              startedAt: new Date(),
              input: [],
            },
            event: {},
          });
        }
        return Promise.resolve({ event: {} });
      }
    );
  });

  it('records step_failed when a lazy step module loader throws', async () => {
    mockLoadStepFunction.mockRejectedValue(
      new Error('Could not load the "sharp" module using the linux-x64 runtime')
    );

    const world = {
      events: { create: mockEventsCreate },
      getEncryptionKeyForRun: vi.fn().mockResolvedValue(undefined),
    } as unknown as World;

    const result = await executeStep({
      world,
      workflowRunId: 'wrun_test123',
      workflowName: 'test-workflow',
      workflowStartedAt: Date.now(),
      stepId: 'step_abc',
      stepName: 'step//./workflows/image//resize',
    });

    expect(result).toEqual({ type: 'failed' });
    expect(
      mockEventsCreate.mock.calls.map(([, event]) => event.eventType)
    ).toEqual(['step_started', 'step_failed']);
    expect(mockEventsCreate).toHaveBeenLastCalledWith(
      'wrun_test123',
      expect.objectContaining({
        eventType: 'step_failed',
        correlationId: 'step_abc',
        eventData: expect.objectContaining({
          stepName: 'step//./workflows/image//resize',
          error: expect.any(Uint8Array),
        }),
      })
    );
  });
});
