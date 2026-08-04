import {
  createFetchQueueHandler,
  deserializeQueueMessage,
  getQueueTopicPrefix,
  MessageId,
  parseQueueName,
  type Queue,
  type QueueHandler,
  QueuePayloadSchema,
  type QueuePrefix,
  resolveQueueNamespace,
  serializeQueueMessage,
  type ValidQueueName,
  WorkflowInvokePayloadSchema,
} from '@workflow/world';
import {
  Logger,
  makeWorkerUtils,
  type Runner,
  run,
  type WorkerUtils,
} from 'graphile-worker';
import type { Pool } from 'pg';
import { monotonicFactory } from 'ulid';
import { z } from 'zod/v4';
import type { PostgresWorldConfig } from './config.js';
import { MessageData } from './message.js';

function createGraphileLogger() {
  const isJsonMode = () => process.env.WORKFLOW_JSON_MODE === '1';
  const isVerbose = () => Boolean(process.env.DEBUG);

  return new Logger(() => (level: string, message: string, meta?: unknown) => {
    if (isJsonMode()) return;
    if ((level === 'debug' || level === 'info') && !isVerbose()) return;
    const pipe = level === 'error' ? process.stderr : process.stdout;
    if (meta) {
      pipe.write(
        `[Graphile Worker] ${message} ${JSON.stringify(meta, null, 2)}\n`
      );
    } else {
      pipe.write(`[Graphile Worker] ${message}\n`);
    }
  });
}

const graphileLogger = createGraphileLogger();
const COMPLETED_IDEMPOTENCY_CACHE_LIMIT = 10_000;
const HANDLER_WAIT_WARNING_MS = 10_000;
const GraphileHelpers = z.object({
  job: z.object({
    attempts: z.number().int().positive(),
  }),
});

/**
 * Queue handlers registered by the workflow runtime, keyed by queue prefix.
 * Held on globalThis so every copy of this module in the process (e.g. one
 * bundled into a route module and one loaded by instrumentation) shares a
 * single registry. The latest registration for a prefix wins (a reloaded
 * route module in dev replaces its predecessor), and handlers are never
 * unregistered: they delegate to the runtime's global world resolution, so a
 * world created later in the same process reuses them.
 *
 * Exported for tests.
 */
export type HandlerRegistry = {
  handlers: Map<QueuePrefix, QueueHandler>;
  onRegister: Set<() => void>;
};
const RegistryKey = Symbol.for('@workflow/world-postgres//queueHandlers');
const globalScope = globalThis as { [RegistryKey]?: HandlerRegistry };
export const handlerRegistry: HandlerRegistry = globalScope[RegistryKey] ?? {
  handlers: new Map(),
  onRegister: new Set(),
};
globalScope[RegistryKey] = handlerRegistry;

/**
 * Registers the runtime's queue handler for direct in-process execution by
 * the embedded graphile-worker runner, and returns the standard HTTP wrapper
 * for the generated flow route.
 */
const createQueueHandler: Queue['createQueueHandler'] = (prefix, handler) => {
  handlerRegistry.handlers.set(prefix, handler);
  for (const notify of [...handlerRegistry.onRegister]) notify();
  return createFetchQueueHandler(prefix, handler);
};

/**
 * The Postgres queue stores messages under one graphile-worker flow task and
 * executes them by calling the registered in-process queue handler directly.
 */
export type PostgresQueue = Queue & {
  start(): Promise<void>;
  close(): Promise<void>;
};

export function createQueue(
  config: PostgresWorldConfig,
  pool: Pool
): PostgresQueue {
  const generateMessageId = monotonicFactory();
  const jobQueueName = `${config.jobPrefix || 'workflow_'}flows`;
  const workflowPrefix = getQueueTopicPrefix(
    'workflow',
    resolveQueueNamespace(config.namespace)
  );

  const completedMessages = new Set<string>();
  const inflightMessages = new Map<string, Promise<void>>();
  const inflightWorkflowRuns = new Map<
    string,
    Promise<'completed' | 'rescheduled'>
  >();
  let workerUtilsPromise: Promise<WorkerUtils> | null = null;
  let runnerPromise: Promise<Runner | null> | null = null;
  const closeController = new AbortController();
  const closeSignal = closeController.signal;

  function ensureWorkerUtils(): Promise<WorkerUtils> {
    workerUtilsPromise ??= (async () => {
      const utils = await makeWorkerUtils({
        pgPool: pool,
        logger: graphileLogger,
      });
      await utils.migrate();
      await migratePgBossJobs(utils);
      return utils;
    })().catch((err) => {
      workerUtilsPromise = null;
      throw err;
    });
    return workerUtilsPromise;
  }

  function markMessageCompleted(idempotencyKey: string) {
    completedMessages.delete(idempotencyKey);
    completedMessages.add(idempotencyKey);
    if (completedMessages.size > COMPLETED_IDEMPOTENCY_CACHE_LIMIT) {
      const oldestKey = completedMessages.values().next().value;
      if (oldestKey) {
        completedMessages.delete(oldestKey);
      }
    }
  }

  async function addGraphileJob({
    queueId,
    body,
    messageId,
    attempt,
    idempotencyKey,
    headers,
    delaySeconds,
  }: {
    queueId: string;
    body: Buffer | Uint8Array;
    messageId: MessageId;
    attempt: number;
    idempotencyKey?: string;
    headers?: Record<string, string>;
    delaySeconds?: number;
  }) {
    const utils = await ensureWorkerUtils();
    const runAt =
      typeof delaySeconds === 'number' && delaySeconds > 0
        ? new Date(Date.now() + delaySeconds * 1000)
        : undefined;

    await utils.addJob(
      jobQueueName,
      MessageData.encode({
        id: queueId,
        data: Buffer.from(body),
        attempt,
        messageId,
        idempotencyKey,
        headers,
      }),
      {
        // One durable row per logical message: retries and timeoutSeconds
        // reschedules replace the job instead of adding a second row.
        jobKey: idempotencyKey ?? messageId,
        ...(runAt ? { runAt } : {}),
        maxAttempts: 3,
      }
    );
  }

  async function migratePgBossJobs(utils: WorkerUtils): Promise<void> {
    // Scenario A: Drizzle migration already ran — staging table exists
    const hasStaging = await pool.query(
      `SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'workflow'
        AND table_name = '_pgboss_pending_jobs'
      ) AS exists`
    );
    if (hasStaging.rows[0]?.exists) {
      const jobs = await pool.query(
        `SELECT name, data, singleton_key, retry_limit
        FROM "workflow"."_pgboss_pending_jobs"`
      );
      for (const job of jobs.rows) {
        await utils.addJob(job.name, job.data as Record<string, unknown>, {
          jobKey: job.singleton_key ?? undefined,
          maxAttempts: job.retry_limit ?? 3,
        });
      }
      await pool.query(`DROP TABLE "workflow"."_pgboss_pending_jobs"`);
      return;
    }

    // Scenario B: Drizzle migration didn't run — pgboss schema still exists
    const hasPgBoss = await pool.query(
      `SELECT EXISTS (
        SELECT 1 FROM information_schema.schemata
        WHERE schema_name = 'pgboss'
      ) AS exists`
    );
    if (hasPgBoss.rows[0]?.exists) {
      const jobs = await pool.query(
        `SELECT name, data, singleton_key, retry_limit
        FROM pgboss.job
        WHERE state IN ('created', 'retry')`
      );
      for (const job of jobs.rows) {
        await utils.addJob(job.name, job.data as Record<string, unknown>, {
          jobKey: job.singleton_key ?? undefined,
          maxAttempts: job.retry_limit ?? 3,
        });
      }
      await pool.query(`DROP SCHEMA pgboss CASCADE`);
    }
  }

  /** Resolves once a handler for this queue's prefix is registered, or on close. */
  function waitForHandler(): Promise<void> {
    if (handlerRegistry.handlers.has(workflowPrefix) || closeSignal.aborted) {
      return Promise.resolve();
    }

    const warnTimer = setTimeout(() => {
      console.warn(
        `[world-postgres] Waiting for a workflow handler for "${workflowPrefix}" before consuming queue jobs. ` +
          'Import the generated workflow route module in this process so it can register its handler.'
      );
    }, HANDLER_WAIT_WARNING_MS);
    warnTimer.unref();

    return new Promise((resolve) => {
      const check = () => {
        if (
          !handlerRegistry.handlers.has(workflowPrefix) &&
          !closeSignal.aborted
        ) {
          return;
        }
        handlerRegistry.onRegister.delete(check);
        closeSignal.removeEventListener('abort', check);
        clearTimeout(warnTimer);
        resolve();
      };
      handlerRegistry.onRegister.add(check);
      closeSignal.addEventListener('abort', check);
    });
  }

  async function start(): Promise<void> {
    if (closeSignal.aborted) return;
    await ensureWorkerUtils();

    // The runner starts only once a workflow handler is registered: jobs stay
    // in Postgres until this process can execute them, and are never consumed
    // by an enqueue-only process. Armed in the background so start() does not
    // block boot when the route module loads after instrumentation.
    if (!runnerPromise) {
      const armed = (async (): Promise<Runner | null> => {
        await waitForHandler();
        if (closeSignal.aborted) return null;
        return run({
          pgPool: pool,
          // Default of 50 is high enough to avoid worker-pool exhaustion in
          // workflows that use parent→child polling patterns (e.g. awaiting a
          // child workflow via `childRun.returnValue` inside the parent).
          // Every such poll holds a worker slot for the duration of the child
          // run. See packages/core/src/runtime/run.ts and
          // docs/content/docs/changelog/eager-processing.mdx for context.
          concurrency: config.queueConcurrency || 50,
          logger: graphileLogger,
          ...(config.applicationManagedShutdown === true && {
            noHandleSignals: true,
          }),
          pollInterval: 500, // graphile-worker uses LISTEN/NOTIFY when available
          taskList: { [jobQueueName]: handleFlowJob },
        });
      })();
      armed.catch((err) => {
        if (runnerPromise === armed) runnerPromise = null;
        console.error('[world-postgres] Queue runner failed to start:', err);
      });
      runnerPromise = armed;
    }
    if (handlerRegistry.handlers.has(workflowPrefix)) {
      await runnerPromise;
    }
  }

  const queue: Queue['queue'] = async (queueName, message, opts) => {
    if (closeSignal.aborted) {
      throw new Error('[world-postgres] The queue is closed');
    }
    const { id: queueId } = parseQueueName(queueName);
    const messageId = MessageId.parse(`msg_${generateMessageId()}`);
    await addGraphileJob({
      queueId,
      body: serializeQueueMessage(message),
      messageId,
      attempt: 1,
      idempotencyKey: opts?.idempotencyKey,
      headers: opts?.headers,
      delaySeconds: opts?.delaySeconds,
    });
    return { messageId };
  };

  async function handleFlowJob(
    payload: unknown,
    helpers: unknown
  ): Promise<void> {
    const messageData = MessageData.parse(payload);
    const { job } = GraphileHelpers.parse(helpers);
    // messageData.attempt is the first logical delivery this graphile job
    // represents; graphile's own `attempts` counts retries of this job. Their
    // sum survives timeoutSeconds reschedules, which create replacement jobs.
    const attempt = messageData.attempt + job.attempts - 1;
    const queueName = `${workflowPrefix}${messageData.id}` as ValidQueueName;
    const message = deserializeQueueMessage(messageData.data);
    QueuePayloadSchema.parse(message);
    const workflowInvoke = WorkflowInvokePayloadSchema.safeParse(message);
    const workflowRunSerializationKey =
      workflowInvoke.success && !workflowInvoke.data.stepId
        ? `workflow:${workflowInvoke.data.runId}`
        : undefined;

    const executeTask = async (): Promise<'completed' | 'rescheduled'> => {
      const handler = handlerRegistry.handlers.get(workflowPrefix);
      if (!handler) {
        // The runner only starts after registration; throwing hands the job
        // back to graphile for redelivery.
        throw new Error(
          `[world-postgres] No workflow handler registered for "${workflowPrefix}"`
        );
      }

      const result = await handler(message, {
        attempt,
        queueName,
        messageId: messageData.messageId,
      });
      if (result) {
        // Schedule the follow-up job before we return so a crash cannot
        // lose the wake-up request.
        await addGraphileJob({
          queueId: messageData.id,
          body: messageData.data,
          messageId: messageData.messageId,
          attempt: attempt + 1,
          idempotencyKey: messageData.idempotencyKey,
          headers: messageData.headers,
          delaySeconds: result.timeoutSeconds,
        });
        return 'rescheduled';
      }
      return 'completed';
    };

    const idempotencyKey = messageData.idempotencyKey;
    if (!idempotencyKey) {
      if (workflowRunSerializationKey) {
        // Preserve step fan-out while preventing two workflow replays from
        // mutating the same run's event log at the same time.
        const previous = inflightWorkflowRuns.get(workflowRunSerializationKey);
        const execution = (previous ?? Promise.resolve())
          .catch(() => {})
          .then(() => executeTask())
          .finally(() => {
            if (
              inflightWorkflowRuns.get(workflowRunSerializationKey) ===
              execution
            ) {
              inflightWorkflowRuns.delete(workflowRunSerializationKey);
            }
          });
        inflightWorkflowRuns.set(workflowRunSerializationKey, execution);
        await execution;
        return;
      }

      await executeTask();
      return;
    }

    if (completedMessages.has(idempotencyKey)) {
      return;
    }

    const existing = inflightMessages.get(idempotencyKey);
    if (existing) {
      await existing;
      return;
    }

    const execution = executeTask()
      .then((result) => {
        if (result === 'completed') {
          markMessageCompleted(idempotencyKey);
        }
      })
      .finally(() => {
        inflightMessages.delete(idempotencyKey);
      });
    inflightMessages.set(idempotencyKey, execution);
    await execution;
  }

  return {
    createQueueHandler,
    getDeploymentId: async () => 'postgres',
    queue,
    start,
    async close() {
      closeController.abort();
      const runner = await runnerPromise?.catch(() => null);
      if (runner) {
        try {
          await runner.stop();
        } catch (error) {
          if (
            !(error instanceof Error) ||
            error.message !== 'Runner is already stopped'
          ) {
            throw error;
          }
        }
        await runner.promise.catch(() => {});
        runnerPromise = null;
      }
      if (workerUtilsPromise) {
        const utils = await workerUtilsPromise.catch(() => null);
        if (utils) await utils.release();
        workerUtilsPromise = null;
      }
    },
  };
}
