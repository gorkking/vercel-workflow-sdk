import { createHash } from 'node:crypto';
import { waitUntil } from '@vercel/functions';
import {
  EntityConflictError,
  ThrottleError,
  WorkflowRuntimeError,
  WorkflowWorldError,
} from '@workflow/errors';
import type { WorkflowInvokePayload, World } from '@workflow/world';
import {
  isLegacySpecVersion,
  SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT,
  SPEC_VERSION_SUPPORTS_EVENT_SOURCING,
} from '@workflow/world';
import { monotonicFactory } from 'ulid';
import { importKey } from '../encryption.js';
import { runtimeLogger } from '../logger.js';
import type { Serializable } from '../schemas.js';
import { dehydrateWorkflowArguments } from '../serialization.js';
import * as Attribute from '../telemetry/semantic-conventions.js';
import { serializeTraceCarrier, trace } from '../telemetry.js';
import { waitedUntil } from '../util.js';
import { version as workflowCoreVersion } from '../version.js';
import { getWorldLazy } from './get-world-lazy.js';
import { getWorkflowQueueName } from './helpers.js';
import { Run } from './run.js';

/** ULID generator for client-side runId generation */
const ulid = monotonicFactory();

export interface StartOptionsBase {
  /**
   * The world to use for the workflow run creation,
   * by default the world is inferred from the environment variables.
   */
  world?: World;

  /**
   * The spec version to use for the workflow run. Defaults to the latest version.
   */
  specVersion?: number;
}

export interface StartOptionsWithDeploymentId extends StartOptionsBase {
  /**
   * The deployment ID to use for the workflow run.
   *
   * By default, this is automatically inferred from environment variables
   * when deploying to Vercel.
   *
   * Set to `'latest'` to automatically resolve the most recent deployment
   * for the current environment (same production target or git branch).
   * This is currently a Vercel-specific feature.
   *
   * **Note:** When `deploymentId` is provided, the argument and return types become `unknown`
   * since there is no guarantee the types will be consistent across deployments.
   */
  deploymentId: 'latest' | (string & {});
}

export interface StartOptionsWithoutDeploymentId extends StartOptionsBase {
  deploymentId?: undefined;
}

/**
 * Options for starting a workflow run.
 */
export type StartOptions =
  | StartOptionsWithDeploymentId
  | StartOptionsWithoutDeploymentId;

export type DynamicWorkflowStepReference = { readonly stepId: string };

export interface DynamicWorkflowOptions {
  /**
   * Already-registered step functions exposed to the dynamic workflow source.
   *
   * Each value may be an imported step function transformed by Workflow SDK
   * (with a `.stepId` property) or an explicit `{ stepId }` reference.
   */
  steps: Record<string, DynamicWorkflowStepReference>;

  /**
   * Name of the async workflow function in the source. Defaults to "workflow".
   */
  exportName?: string;
}

export type DynamicStartOptions = StartOptions & {
  dynamic: DynamicWorkflowOptions;
};

/**
 * Represents an imported workflow function.
 */
export type WorkflowFunction<TArgs extends unknown[], TResult> = (
  ...args: TArgs
) => Promise<TResult>;

/**
 * Represents the generated metadata of a workflow function.
 */
export type WorkflowMetadata = { workflowId: string };

export interface DynamicWorkflowExecutionContext {
  version: 1;
  workflowCode: string;
  sourceHash: string;
  exportName: string;
}

const DYNAMIC_WORKFLOW_SOURCE_MAX_BYTES = 32 * 1024;
const SAFE_DYNAMIC_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const UNSUPPORTED_DYNAMIC_MODULE_SYNTAX =
  /(^|[\s;])(?:import\s*(?:[\w*{]|\(|['"])|export\s+(?:async\s+)?(?:function|const|let|var|class|default|\{|\*))/m;

function stableJsonStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableJsonStringify).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    return `{${Object.entries(value as Record<string, unknown>)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, val]) => `${JSON.stringify(key)}:${stableJsonStringify(val)}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function sha256Hex(input: string): string {
  return createHash('sha256').update(input).digest('hex');
}

function assertDynamicWorkflowIdentifier(kind: string, value: string) {
  if (!SAFE_DYNAMIC_IDENTIFIER.test(value)) {
    throw new WorkflowRuntimeError(
      `Invalid dynamic workflow ${kind} "${value}". Use only letters, numbers, and underscores, and start with a letter or underscore.`
    );
  }
}

function validateDynamicWorkflowSource(source: string, exportName: string) {
  if (Buffer.byteLength(source, 'utf8') > DYNAMIC_WORKFLOW_SOURCE_MAX_BYTES) {
    throw new WorkflowRuntimeError(
      `Dynamic workflow source is too large. The MVP limit is ${DYNAMIC_WORKFLOW_SOURCE_MAX_BYTES} bytes.`
    );
  }

  if (UNSUPPORTED_DYNAMIC_MODULE_SYNTAX.test(source)) {
    throw new WorkflowRuntimeError(
      'Dynamic workflow source cannot contain import or export syntax in the MVP.'
    );
  }

  const functionMatch = new RegExp(
    `\\basync\\s+function\\s+${exportName}\\s*\\([^)]*\\)\\s*\\{`
  ).exec(source);
  if (!functionMatch) {
    throw new WorkflowRuntimeError(
      `Dynamic workflow source must define async function ${exportName}(...).`
    );
  }

  const bodyStart = functionMatch.index + functionMatch[0].length;
  const bodyPrefix = source.slice(bodyStart, bodyStart + 200);
  if (!/^\s*(?:"use workflow"|'use workflow')\s*;/.test(bodyPrefix)) {
    throw new WorkflowRuntimeError(
      `Dynamic workflow function "${exportName}" must start with a "use workflow" directive.`
    );
  }
}

function getDynamicStepId(alias: string, value: unknown): string {
  const stepId =
    (value && typeof value === 'object') || typeof value === 'function'
      ? (value as { stepId?: unknown }).stepId
      : undefined;

  if (typeof stepId !== 'string' || stepId.length === 0) {
    throw new WorkflowRuntimeError(
      `Dynamic workflow step "${alias}" must be an imported step function or an object with a non-empty stepId.`
    );
  }

  return stepId;
}

function compileDynamicWorkflowSource(
  source: string,
  options: DynamicWorkflowOptions
): {
  workflowName: string;
  dynamicWorkflow: DynamicWorkflowExecutionContext;
} {
  const exportName = options.exportName ?? 'workflow';
  if ('id' in options) {
    throw new WorkflowRuntimeError(
      'dynamic.id is not supported. Dynamic workflow IDs are generated from the source and step references.'
    );
  }
  assertDynamicWorkflowIdentifier('exportName', exportName);
  validateDynamicWorkflowSource(source, exportName);

  if (!options.steps || Object.keys(options.steps).length === 0) {
    throw new WorkflowRuntimeError(
      'Dynamic workflow options must include at least one registered step in dynamic.steps.'
    );
  }

  const stepEntries = Object.entries(options.steps)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([alias, value]) => {
      assertDynamicWorkflowIdentifier(`step alias`, alias);
      return [alias, getDynamicStepId(alias, value)] as const;
    });

  const sourceHash = sha256Hex(
    `${source}\n${stableJsonStringify(Object.fromEntries(stepEntries))}`
  );
  const workflowIdSegment = sourceHash.slice(0, 32);

  const workflowName = `workflow//dynamic/${workflowIdSegment}//${exportName}`;
  const stepProxyEntries = stepEntries
    .map(
      ([alias, stepId]) =>
        `${JSON.stringify(alias)}: __dynamicUseStep(${JSON.stringify(stepId)})`
    )
    .join(',\n  ');

  const workflowCode = `
globalThis.__private_workflows = new Map();
const __dynamicUseStep = globalThis[Symbol.for("WORKFLOW_USE_STEP")];
if (typeof __dynamicUseStep !== "function") {
  throw new Error("WORKFLOW_USE_STEP is not available in the workflow VM.");
}
const steps = Object.freeze({
  ${stepProxyEntries}
});
const sleep = globalThis[Symbol.for("WORKFLOW_SLEEP")];
const createHook = globalThis[Symbol.for("WORKFLOW_CREATE_HOOK")];
${source}
Object.defineProperty(${exportName}, "workflowId", {
  value: ${JSON.stringify(workflowName)},
  writable: false,
  enumerable: false,
  configurable: false
});
globalThis.__private_workflows.set(${JSON.stringify(workflowName)}, ${exportName});
`;

  return {
    workflowName,
    dynamicWorkflow: {
      version: 1,
      workflowCode,
      sourceHash,
      exportName,
    },
  };
}

/**
 * Starts a workflow run.
 *
 * @param workflow - The imported workflow function to start.
 * @param args - The arguments to pass to the workflow (optional).
 * @param options - The options for the workflow run (optional).
 * @returns The unique run ID for the newly started workflow invocation.
 */
// Overloads with deploymentId - args and return type become unknown
// Uses generics so typed workflows are assignable (avoids contravariance issues),
// but the return type and args are still unknown since the deployed version may differ.
export function start<TArgs extends unknown[], TResult>(
  workflow: WorkflowFunction<TArgs, TResult> | WorkflowMetadata,
  args: unknown[],
  options: StartOptionsWithDeploymentId
): Promise<Run<unknown>>;

export function start<TResult>(
  workflow: WorkflowFunction<[], TResult> | WorkflowMetadata,
  options: StartOptionsWithDeploymentId
): Promise<Run<unknown>>;

// Overloads without deploymentId - preserve type inference
export function start<TArgs extends unknown[], TResult>(
  workflow: WorkflowFunction<TArgs, TResult> | WorkflowMetadata,
  args: TArgs,
  options?: StartOptionsWithoutDeploymentId
): Promise<Run<TResult>>;

export function start<TResult>(
  workflow: WorkflowFunction<[], TResult> | WorkflowMetadata,
  options?: StartOptionsWithoutDeploymentId
): Promise<Run<TResult>>;

export function start(
  source: string,
  args: unknown[],
  options: DynamicStartOptions
): Promise<Run<unknown>>;

export function start(
  source: string,
  options: DynamicStartOptions
): Promise<Run<unknown>>;

export async function start<TArgs extends unknown[], TResult>(
  workflow: WorkflowFunction<TArgs, TResult> | WorkflowMetadata | string,
  argsOrOptions?: TArgs | StartOptions | DynamicStartOptions,
  options?: StartOptions | DynamicStartOptions
) {
  'use step';
  return await waitedUntil(() => {
    let args: Serializable[] = [];
    let opts: StartOptions | DynamicStartOptions = options ?? {};
    if (Array.isArray(argsOrOptions)) {
      args = argsOrOptions as Serializable[];
    } else if (typeof argsOrOptions === 'object') {
      opts = argsOrOptions;
    }

    let dynamicWorkflow: DynamicWorkflowExecutionContext | undefined;
    let workflowName: string | undefined;
    if (typeof workflow === 'string') {
      const dynamicOptions = (opts as Partial<DynamicStartOptions>).dynamic;
      if (!dynamicOptions) {
        throw new WorkflowRuntimeError(
          'Dynamic workflow source requires options.dynamic.'
        );
      }
      const compiled = compileDynamicWorkflowSource(workflow, dynamicOptions);
      workflowName = compiled.workflowName;
      dynamicWorkflow = compiled.dynamicWorkflow;
    } else {
      // @ts-expect-error this field is added by our client transform
      workflowName = workflow?.workflowId;
    }

    if (!workflowName) {
      throw new WorkflowRuntimeError(
        `'start' received an invalid workflow function. Ensure the Workflow SDK is configured correctly and the function includes a 'use workflow' directive.`,
        { slug: 'start-invalid-workflow-function' }
      );
    }

    return trace(`workflow.start ${workflowName}`, async (span) => {
      span?.setAttributes({
        ...Attribute.WorkflowName(workflowName),
        ...Attribute.WorkflowOperation('start'),
      });

      span?.setAttributes({
        ...Attribute.WorkflowArgumentsCount(args.length),
      });

      const world = opts?.world ?? (await getWorldLazy());
      let deploymentId = opts.deploymentId ?? (await world.getDeploymentId());

      // When 'latest' is requested, resolve the actual latest deployment ID
      // for the current deployment's environment (same production target or
      // same git branch for preview deployments).
      if (deploymentId === 'latest') {
        if (!world.resolveLatestDeploymentId) {
          throw new WorkflowRuntimeError(
            "deploymentId 'latest' requires a World that implements resolveLatestDeploymentId()"
          );
        }
        deploymentId = await world.resolveLatestDeploymentId();
      }

      const ops: Promise<void>[] = [];

      // Generate runId client-side so we have it before serialization
      // (required for future E2E encryption where runId is part of the encryption context)
      const runId = `wrun_${ulid()}`;

      // Serialize current trace context to propagate across queue boundary
      const traceCarrier = await serializeTraceCarrier();

      // Use world-declared specVersion when available (our worlds set this),
      // otherwise fall back to the safe baseline that community worlds handle.
      // Community worlds built against older @workflow/world reject runs with
      // specVersion > their SPEC_VERSION_CURRENT via requiresNewerWorld().
      const specVersion =
        opts.specVersion ??
        world.specVersion ??
        SPEC_VERSION_SUPPORTS_EVENT_SOURCING;
      const v1Compat = isLegacySpecVersion(specVersion);

      // Resolve encryption key for the new run. The runId has already been
      // generated above (client-generated ULID) and will be used for both
      // key derivation and the run_created event. The World implementation
      // uses the runId for per-run HKDF key derivation. We pass the resolved
      // deploymentId (not just the raw opts) so the World can use it for
      // key resolution even when deploymentId was inferred from the environment
      // rather than explicitly provided in opts (e.g., in e2e test runners).
      const rawKey = await world.getEncryptionKeyForRun?.(runId, {
        ...opts,
        deploymentId,
      });
      const encryptionKey = rawKey ? await importKey(rawKey) : undefined;

      // Create run via run_created event (event-sourced architecture)
      // Pass client-generated runId - server will accept and use it
      const workflowArguments = await dehydrateWorkflowArguments(
        args,
        runId,
        encryptionKey,
        ops,
        globalThis,
        v1Compat
      );

      const executionContext = {
        traceCarrier,
        workflowCoreVersion,
        features: { encryption: !!encryptionKey },
        ...(dynamicWorkflow ? { dynamicWorkflow } : {}),
      };

      // Call events.create (run_created) and queue in parallel.
      // If events.create fails with 429/5xx, the run was still accepted
      // via the queue and creation will be re-tried async by the runtime.
      const [runCreatedResult, queueResult] = await Promise.allSettled([
        world.events.create(
          runId,
          {
            eventType: 'run_created',
            specVersion,
            eventData: {
              deploymentId: deploymentId,
              workflowName: workflowName,
              input: workflowArguments,
              executionContext,
            },
          },
          { v1Compat }
        ),
        world.queue(
          getWorkflowQueueName(workflowName),
          {
            runId,
            traceCarrier,
            ...(specVersion >= SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT
              ? {
                  runInput: {
                    input: workflowArguments,
                    deploymentId,
                    workflowName,
                    specVersion,
                    executionContext,
                  },
                }
              : {}),
          } satisfies WorkflowInvokePayload,
          {
            deploymentId,
            specVersion,
          }
        ),
      ]);

      // Queue failure is always fatal — the run was not enqueued
      if (queueResult.status === 'rejected') {
        throw queueResult.reason;
      }

      // Handle events.create result
      let resilientStart = false;
      if (runCreatedResult.status === 'rejected') {
        const err = runCreatedResult.reason;
        if (EntityConflictError.is(err)) {
          // 409: The run already exists. This can happen in extreme cases where
          // the run creation call gets a cold start or other slowdown, and the queue
          // + run_started call completes faster. We expect this to be <=1% of cases.
          // In this case, we can safely return.
        } else if (isRetryableStartError(err)) {
          // 429 (ThrottleError) and 5xx (WorkflowWorldError with status >= 500)
          // are retryable — the run was accepted via the queue and creation
          // will be re-tried by the runtime when it calls run_started.
          resilientStart = true;
          runtimeLogger.warn(
            'Run creation event failed, but the run was accepted via the queue. ' +
              'The run_created event will be re-tried async by the runtime.',
            { workflowRunId: runId, error: err.message }
          );
        } else {
          throw err;
        }
      } else {
        const result = runCreatedResult.value;
        // Assert that the run was created
        if (!result.run) {
          throw new WorkflowRuntimeError(
            "Missing 'run' in server response for 'run_created' event"
          );
        }

        // Verify server accepted our runId
        if (!v1Compat && result.run.runId !== runId) {
          throw new WorkflowRuntimeError(
            `Server returned different runId than requested: expected ${runId}, got ${result.run.runId}`
          );
        }
      }

      waitUntil(
        Promise.all(ops).catch((err) => {
          // Ignore expected client disconnect errors (e.g., browser refresh during streaming)
          const isAbortError =
            err?.name === 'AbortError' || err?.name === 'ResponseAborted';
          if (!isAbortError) throw err;
        })
      );

      span?.setAttributes({
        ...Attribute.WorkflowRunId(runId),
        ...Attribute.DeploymentId(deploymentId),
        ...(runCreatedResult.status === 'fulfilled' &&
        runCreatedResult.value.run
          ? Attribute.WorkflowRunStatus(runCreatedResult.value.run.status)
          : {}),
      });

      return new Run<TResult>(runId, { resilientStart });
    });
  });
}

/**
 * Checks if an error from events.create (run_created) is retryable,
 * meaning the queue can re-try creation later via the run_started path.
 * - ThrottleError (429): rate limited, will succeed later
 * - WorkflowWorldError with status >= 500: server error, will succeed later
 */
function isRetryableStartError(err: unknown): boolean {
  if (ThrottleError.is(err)) return true;
  if (WorkflowWorldError.is(err) && err.status && err.status >= 500)
    return true;
  return false;
}
