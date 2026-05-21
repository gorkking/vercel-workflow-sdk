import { WorkflowRuntimeError, WorkflowWorldError } from '@workflow/errors';
import {
  SPEC_VERSION_CURRENT,
  SPEC_VERSION_LEGACY,
  SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT,
  SPEC_VERSION_SUPPORTS_EVENT_SOURCING,
} from '@workflow/world';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  it,
  vi,
} from 'vitest';
import type { Run } from './run.js';
import type { WorkflowFunction } from './start.js';
import { start } from './start.js';
import { setWorld } from './world.js';

// Mock @vercel/functions
vi.mock('@vercel/functions', () => ({
  waitUntil: vi.fn(),
}));

// Mock telemetry
vi.mock('../telemetry.js', () => ({
  serializeTraceCarrier: vi.fn().mockResolvedValue({}),
  trace: vi.fn((_name, fn) => fn(undefined)),
}));

describe('start', () => {
  describe('error handling', () => {
    it('should throw WorkflowRuntimeError when workflow is undefined', async () => {
      await expect(
        // @ts-expect-error - intentionally passing undefined
        start(undefined, [])
      ).rejects.toThrow(WorkflowRuntimeError);

      await expect(
        // @ts-expect-error - intentionally passing undefined
        start(undefined, [])
      ).rejects.toThrow(
        `'start' received an invalid workflow function. Ensure the Workflow SDK is configured correctly and the function includes a 'use workflow' directive.`
      );
    });

    it('should throw WorkflowRuntimeError when workflow is null', async () => {
      await expect(
        // @ts-expect-error - intentionally passing null
        start(null, [])
      ).rejects.toThrow(WorkflowRuntimeError);

      await expect(
        // @ts-expect-error - intentionally passing null
        start(null, [])
      ).rejects.toThrow(
        `'start' received an invalid workflow function. Ensure the Workflow SDK is configured correctly and the function includes a 'use workflow' directive.`
      );
    });

    it('should throw WorkflowRuntimeError when workflow has no workflowId', async () => {
      const invalidWorkflow = () => Promise.resolve('result');

      await expect(start(invalidWorkflow, [])).rejects.toThrow(
        WorkflowRuntimeError
      );

      await expect(start(invalidWorkflow, [])).rejects.toThrow(
        `'start' received an invalid workflow function. Ensure the Workflow SDK is configured correctly and the function includes a 'use workflow' directive.`
      );
    });

    it('should throw WorkflowRuntimeError when workflow has empty string workflowId', async () => {
      const invalidWorkflow = Object.assign(() => Promise.resolve('result'), {
        workflowId: '',
      });

      await expect(start(invalidWorkflow, [])).rejects.toThrow(
        WorkflowRuntimeError
      );
    });
  });

  describe('specVersion', () => {
    let mockEventsCreate: ReturnType<typeof vi.fn>;
    let mockQueue: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      mockEventsCreate = vi.fn().mockImplementation((runId) => {
        return Promise.resolve({
          run: { runId: runId ?? 'wrun_test123', status: 'pending' },
        });
      });
      mockQueue = vi.fn().mockResolvedValue(undefined);

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
      } as any);
    });

    afterEach(() => {
      setWorld(undefined);
      vi.clearAllMocks();
    });

    it('should use world.specVersion when available, falling back to SPEC_VERSION_SUPPORTS_EVENT_SOURCING', async () => {
      const validWorkflow = Object.assign(() => Promise.resolve('result'), {
        workflowId: 'test-workflow',
      });

      // Mock world without specVersion → falls back to safe baseline (v2)
      await start(validWorkflow, []);

      expect(mockEventsCreate).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          eventType: 'run_created',
          specVersion: SPEC_VERSION_SUPPORTS_EVENT_SOURCING,
        }),
        expect.objectContaining({
          v1Compat: false,
        })
      );

      vi.clearAllMocks();

      // Mock world with specVersion 3 → uses it
      setWorld({
        specVersion: SPEC_VERSION_CURRENT,
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
      } as any);

      await start(validWorkflow, []);

      expect(mockEventsCreate).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          eventType: 'run_created',
          specVersion: SPEC_VERSION_CURRENT,
        }),
        expect.objectContaining({
          v1Compat: false,
        })
      );
    });

    it('should use provided specVersion when passed in options', async () => {
      const validWorkflow = Object.assign(() => Promise.resolve('result'), {
        workflowId: 'test-workflow',
      });

      await start(validWorkflow, [], { specVersion: SPEC_VERSION_LEGACY });

      expect(mockEventsCreate).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          eventType: 'run_created',
          specVersion: SPEC_VERSION_LEGACY,
        }),
        expect.objectContaining({
          v1Compat: true,
        })
      );
    });

    it('should use provided specVersion with v1Compat true for legacy versions', async () => {
      const validWorkflow = Object.assign(() => Promise.resolve('result'), {
        workflowId: 'test-workflow',
      });

      await start(validWorkflow, [], { specVersion: 1 });

      expect(mockEventsCreate).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          eventType: 'run_created',
          specVersion: 1,
        }),
        expect.objectContaining({
          v1Compat: true,
        })
      );
    });
  });

  describe('encryption', () => {
    let mockEventsCreate: ReturnType<typeof vi.fn>;
    let mockQueue: ReturnType<typeof vi.fn>;
    let mockGetEncryptionKeyForRun: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      mockEventsCreate = vi.fn().mockImplementation((runId) => {
        return Promise.resolve({
          run: { runId: runId ?? 'wrun_test123', status: 'pending' },
        });
      });
      mockQueue = vi.fn().mockResolvedValue(undefined);
      mockGetEncryptionKeyForRun = vi.fn().mockResolvedValue(undefined);

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_resolved'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
        getEncryptionKeyForRun: mockGetEncryptionKeyForRun,
      } as any);
    });

    afterEach(() => {
      setWorld(undefined);
      vi.clearAllMocks();
    });

    it('should pass resolved deploymentId to getEncryptionKeyForRun even when not in opts', async () => {
      const validWorkflow = Object.assign(() => Promise.resolve('result'), {
        workflowId: 'test-workflow',
      });

      // Call start() without explicit deploymentId in options — it should
      // be resolved from world.getDeploymentId() and forwarded to
      // getEncryptionKeyForRun so the key can be fetched.
      await start(validWorkflow, []);

      expect(mockGetEncryptionKeyForRun).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          deploymentId: 'deploy_resolved',
        })
      );
    });

    it('should pass explicit deploymentId from opts to getEncryptionKeyForRun', async () => {
      const validWorkflow = Object.assign(() => Promise.resolve('result'), {
        workflowId: 'test-workflow',
      });

      await start(validWorkflow, [], { deploymentId: 'deploy_explicit' });

      expect(mockGetEncryptionKeyForRun).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          deploymentId: 'deploy_explicit',
        })
      );
    });
  });

  describe('deploymentId: latest', () => {
    let mockEventsCreate: ReturnType<typeof vi.fn>;
    let mockQueue: ReturnType<typeof vi.fn>;

    const validWorkflow = Object.assign(() => Promise.resolve('result'), {
      workflowId: 'test-workflow',
    });

    beforeEach(() => {
      mockEventsCreate = vi.fn().mockImplementation((runId) => {
        return Promise.resolve({
          run: { runId: runId ?? 'wrun_test123', status: 'pending' },
        });
      });
      mockQueue = vi.fn().mockResolvedValue(undefined);
    });

    afterEach(() => {
      setWorld(undefined);
      vi.clearAllMocks();
    });

    it('should resolve "latest" to the actual deployment ID via resolveLatestDeploymentId', async () => {
      const mockResolveLatest = vi
        .fn()
        .mockResolvedValue('dpl_resolved_abc123');

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
        resolveLatestDeploymentId: mockResolveLatest,
      } as any);

      await start(validWorkflow, [], { deploymentId: 'latest' });

      expect(mockResolveLatest).toHaveBeenCalledTimes(1);

      // The resolved deployment ID should be used in the run_created event
      expect(mockEventsCreate).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          eventType: 'run_created',
          eventData: expect.objectContaining({
            deploymentId: 'dpl_resolved_abc123',
          }),
        }),
        expect.anything()
      );

      // The resolved deployment ID should be used in the queue call
      expect(mockQueue).toHaveBeenCalledWith(
        expect.any(String),
        expect.any(Object),
        expect.objectContaining({ deploymentId: 'dpl_resolved_abc123' })
      );
    });

    it('should pass the resolved deployment ID to getEncryptionKeyForRun when using "latest"', async () => {
      const mockResolveLatest = vi
        .fn()
        .mockResolvedValue('dpl_resolved_abc123');
      const mockGetEncryptionKeyForRun = vi.fn();

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
        resolveLatestDeploymentId: mockResolveLatest,
        getEncryptionKeyForRun: mockGetEncryptionKeyForRun,
      } as any);

      await start(validWorkflow, [], { deploymentId: 'latest' });

      expect(mockResolveLatest).toHaveBeenCalledTimes(1);
      expect(mockGetEncryptionKeyForRun).toHaveBeenCalled();

      const [, contextArg] =
        mockGetEncryptionKeyForRun.mock.calls[
          mockGetEncryptionKeyForRun.mock.calls.length - 1
        ] || [];

      expect(contextArg).toEqual(
        expect.objectContaining({
          deploymentId: 'dpl_resolved_abc123',
        })
      );
    });

    it('should throw WorkflowRuntimeError when "latest" is used with a World that does not implement resolveLatestDeploymentId', async () => {
      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
        // No resolveLatestDeploymentId
      } as any);

      await expect(
        start(validWorkflow, [], { deploymentId: 'latest' })
      ).rejects.toThrow(WorkflowRuntimeError);

      await expect(
        start(validWorkflow, [], { deploymentId: 'latest' })
      ).rejects.toThrow(
        "deploymentId 'latest' requires a World that implements resolveLatestDeploymentId()"
      );
    });

    it('should not call resolveLatestDeploymentId when a normal deploymentId is provided', async () => {
      const mockResolveLatest = vi
        .fn()
        .mockResolvedValue('dpl_resolved_abc123');

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
        resolveLatestDeploymentId: mockResolveLatest,
      } as any);

      await start(validWorkflow, [], { deploymentId: 'dpl_specific_456' });

      expect(mockResolveLatest).not.toHaveBeenCalled();

      // The provided deployment ID should be used directly
      expect(mockEventsCreate).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          eventData: expect.objectContaining({
            deploymentId: 'dpl_specific_456',
          }),
        }),
        expect.anything()
      );
    });

    it('should not call resolveLatestDeploymentId when no deploymentId is provided', async () => {
      const mockResolveLatest = vi
        .fn()
        .mockResolvedValue('dpl_resolved_abc123');

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('dpl_default_789'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
        resolveLatestDeploymentId: mockResolveLatest,
      } as any);

      await start(validWorkflow, []);

      expect(mockResolveLatest).not.toHaveBeenCalled();

      // Should use the default from getDeploymentId()
      expect(mockEventsCreate).toHaveBeenCalledWith(
        expect.stringMatching(/^wrun_/),
        expect.objectContaining({
          eventData: expect.objectContaining({
            deploymentId: 'dpl_default_789',
          }),
        }),
        expect.anything()
      );
    });
  });

  describe('resilient start (run_created failure)', () => {
    const validWorkflow = Object.assign(() => Promise.resolve('result'), {
      workflowId: 'test-workflow',
    });

    afterEach(() => {
      setWorld(undefined);
      vi.clearAllMocks();
    });

    it('should succeed when events.create throws a 500 error (queue still dispatched)', async () => {
      const mockQueue = vi.fn().mockResolvedValue({ messageId: null });
      const serverError = new WorkflowWorldError('Internal Server Error', {
        status: 500,
      });
      const mockEventsCreate = vi.fn().mockRejectedValue(serverError);

      setWorld({
        // World declares specVersion 3 to enable CBOR queue transport + runInput
        specVersion: SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT,
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
      } as any);

      // start() should NOT throw — the queue was still dispatched
      const run = await start(validWorkflow, [42]);
      expect(run.runId).toMatch(/^wrun_/);

      // Queue should have been called with runInput
      expect(mockQueue).toHaveBeenCalledTimes(1);
      const [, queuePayload] = mockQueue.mock.calls[0];
      expect(queuePayload.runInput).toBeDefined();
      expect(queuePayload.runInput.deploymentId).toBe('deploy_123');
      expect(queuePayload.runInput.workflowName).toBe('test-workflow');
      expect(queuePayload.runInput.specVersion).toBe(
        SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT
      );
    });

    it('should throw when queue fails even if events.create succeeds', async () => {
      const mockEventsCreate = vi.fn().mockResolvedValue({
        run: { runId: 'wrun_test', status: 'pending' },
      });
      const mockQueue = vi
        .fn()
        .mockRejectedValue(new Error('Queue unavailable'));

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
      } as any);

      await expect(start(validWorkflow, [])).rejects.toThrow(
        'Queue unavailable'
      );
    });

    it('should throw when events.create fails with a non-retryable error (e.g. 400)', async () => {
      const badRequest = new WorkflowWorldError('Bad Request', {
        status: 400,
      });
      const mockEventsCreate = vi.fn().mockRejectedValue(badRequest);
      const mockQueue = vi.fn().mockResolvedValue({ messageId: null });

      setWorld({
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
      } as any);

      await expect(start(validWorkflow, [])).rejects.toThrow('Bad Request');
    });
  });

  describe('dynamic workflow source', () => {
    let mockEventsCreate: ReturnType<typeof vi.fn>;
    let mockQueue: ReturnType<typeof vi.fn>;

    const validSource = `
async function workflow(input) {
  "use workflow";
  const user = await steps.fetchUser(input.userId);
  await steps.sendEmail(user.email);
  return { ok: true };
}`;

    const fetchUser = Object.assign(async () => undefined, {
      stepId: 'step//./steps//fetchUser',
    });
    const sendEmail = Object.assign(async () => undefined, {
      stepId: 'step//./steps//sendEmail',
    });

    beforeEach(() => {
      mockEventsCreate = vi.fn().mockImplementation((runId) => {
        return Promise.resolve({
          run: { runId: runId ?? 'wrun_test123', status: 'pending' },
        });
      });
      mockQueue = vi.fn().mockResolvedValue({ messageId: null });

      setWorld({
        specVersion: SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT,
        getDeploymentId: vi.fn().mockResolvedValue('deploy_123'),
        events: { create: mockEventsCreate },
        queue: mockQueue,
      } as any);
    });

    afterEach(() => {
      setWorld(undefined);
      vi.clearAllMocks();
    });

    it('stores dynamic workflow code in executionContext and queues the generated workflow name', async () => {
      await start(validSource, [{ userId: 'user_123' }], {
        dynamic: {
          steps: { fetchUser, sendEmail },
        },
      });

      const [, runCreated] = mockEventsCreate.mock.calls[0];
      const dynamicWorkflow =
        runCreated.eventData.executionContext.dynamicWorkflow;
      const expectedWorkflowName = `workflow//dynamic/${dynamicWorkflow.sourceHash.slice(0, 32)}//workflow`;
      expect(runCreated.eventData.workflowName).toBe(expectedWorkflowName);
      expect(runCreated.eventData.executionContext.dynamicWorkflow).toEqual(
        expect.objectContaining({
          version: 1,
          exportName: 'workflow',
          sourceHash: expect.any(String),
          workflowCode: expect.stringContaining('steps = Object.freeze'),
        })
      );
      expect(
        runCreated.eventData.executionContext.dynamicWorkflow.workflowCode
      ).toContain('__dynamicUseStep("step//./steps//fetchUser")');

      const [queueName, queuePayload] = mockQueue.mock.calls[0];
      expect(queueName).toBe(`__wkf_workflow_${expectedWorkflowName}`);
      expect(queuePayload.runInput.workflowName).toBe(expectedWorkflowName);
      expect(queuePayload.runInput.executionContext.dynamicWorkflow).toEqual(
        runCreated.eventData.executionContext.dynamicWorkflow
      );
    });

    it('accepts explicit stepId references', async () => {
      await start(validSource, [{ userId: 'user_123' }], {
        dynamic: {
          steps: {
            fetchUser: { stepId: 'step//./steps//fetchUser' },
            sendEmail: { stepId: 'step//./steps//sendEmail' },
          },
        },
      });

      const [, runCreated] = mockEventsCreate.mock.calls[0];
      expect(
        runCreated.eventData.executionContext.dynamicWorkflow.workflowCode
      ).toContain('__dynamicUseStep("step//./steps//sendEmail")');
    });

    it('rejects source strings without dynamic options', async () => {
      await expect(
        // @ts-expect-error - intentionally missing dynamic options
        start(validSource, [])
      ).rejects.toThrow('Dynamic workflow source requires options.dynamic');
    });

    it('rejects missing steps', async () => {
      await expect(
        start(validSource, [], { dynamic: { steps: {} } })
      ).rejects.toThrow('dynamic.steps');
    });

    it('rejects step aliases without stepId metadata', async () => {
      await expect(
        start(validSource, [], {
          dynamic: {
            steps: {
              fetchUser: (async () => undefined) as any,
              sendEmail,
            },
          },
        })
      ).rejects.toThrow('must be an imported step function');
    });

    it('rejects unsupported module syntax', async () => {
      await expect(
        start(`import { x } from './x';\n${validSource}`, [], {
          dynamic: { steps: { fetchUser, sendEmail } },
        })
      ).rejects.toThrow('cannot contain import or export syntax');
    });

    it('rejects source without a use workflow directive', async () => {
      await expect(
        start('async function workflow() { return 1; }', [], {
          dynamic: { steps: { fetchUser, sendEmail } },
        })
      ).rejects.toThrow('must start with a "use workflow" directive');
    });

    it('rejects custom dynamic workflow ids', async () => {
      await expect(
        start(validSource, [], {
          dynamic: {
            // @ts-expect-error - dynamic IDs are generated from source + steps
            id: 'custom-id',
            steps: { fetchUser, sendEmail },
          },
        })
      ).rejects.toThrow('dynamic.id is not supported');
    });
  });

  describe('overload type inference', () => {
    // Type-only assertions that don't execute start() at runtime.
    // We use expectTypeOf on the function signature's return type directly.

    type TypedWf = WorkflowFunction<[string, number], boolean>;
    type ZeroArgWf = WorkflowFunction<[], string>;
    type Meta = { workflowId: string };

    it('should preserve types without deploymentId', () => {
      // With args
      expectTypeOf<
        (wf: TypedWf, args: [string, number]) => Promise<Run<boolean>>
      >().toMatchTypeOf<typeof start>();

      // Zero-arg workflow without args
      expectTypeOf(start<string>)
        .parameter(0)
        .toMatchTypeOf<ZeroArgWf | Meta>();
    });

    it('should return Run<unknown> when deploymentId is provided', () => {
      // Typed workflow with deploymentId - return type becomes Run<unknown>
      type StartWithDeploymentId = (
        wf: TypedWf | Meta,
        args: unknown[],
        opts: { deploymentId: string }
      ) => Promise<Run<unknown>>;
      expectTypeOf<StartWithDeploymentId>().toMatchTypeOf<typeof start>();
    });

    it('should accept typed workflows with deploymentId (no contravariance issue)', () => {
      // This is the key test: a typed workflow should be assignable to the
      // deploymentId overload. We verify by checking the first parameter
      // accepts TypedWf.
      type DeploymentIdOverload = <TArgs extends unknown[], TResult>(
        wf: WorkflowFunction<TArgs, TResult> | Meta,
        args: unknown[],
        opts: { deploymentId: string }
      ) => Promise<Run<unknown>>;
      expectTypeOf<DeploymentIdOverload>().toMatchTypeOf<typeof start>();
    });

    it('should return Run<unknown> for dynamic workflow source', () => {
      expectTypeOf<
        (
          source: string,
          args: unknown[],
          opts: {
            dynamic: { steps: Record<string, { stepId: string }> };
          }
        ) => Promise<Run<unknown>>
      >().toMatchTypeOf<typeof start>();
    });
  });
});
