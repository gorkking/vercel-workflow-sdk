import { registerOTel } from '@vercel/otel';

export async function register() {
  registerOTel({
    serviceName: 'nextjs-webpack',
    instrumentationConfig: {
      fetch: {
        // By default @vercel/otel only propagates W3C trace context to Vercel
        // deployment URLs, so outgoing requests to the workflow-server
        // (vercel-workflow.com) and the Vercel Queue Service
        // (*.vercel-queue.com) get a client span with no `traceparent` header
        // — which breaks the trace link to those services' spans in APM.
        // Explicitly propagate context to both domains so traces stay
        // correlated end to end.
        // https://vercel.com/docs/tracing/instrumentation#configuring-context-propagation
        propagateContextUrls: [/vercel-workflow\.com/, /vercel-queue\.com/],
      },
    },
  });

  if (
    process.env.NEXT_RUNTIME === 'nodejs' &&
    process.env.WORKFLOW_TARGET_WORLD === '@workflow/world-postgres'
  ) {
    // Load the generated flow route module so it registers its queue handler
    // (the Postgres world executes queue jobs in-process), then start the
    // world's embedded queue worker.
    await import('./app/.well-known/workflow/v1/flow/route');
    const { getWorld } = await import('workflow/runtime');
    const world = await getWorld();
    await world.start?.();
  }
}
