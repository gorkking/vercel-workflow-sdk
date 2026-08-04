#!/usr/bin/env node

// Start the Postgres World before starting the Astro server
// Needed since we test this in CI
// Astro doesn't have a hook for starting the Postgres World in production

async function main() {
  const usePostgres =
    process.env.WORKFLOW_TARGET_WORLD === '@workflow/world-postgres';

  if (usePostgres) {
    console.log('Starting Postgres World...');
    const { getWorld } = await import('workflow/runtime');
    const world = await getWorld();
    if (world.start) {
      console.log('Starting World workers...');
      await world.start();
    }
  }

  // Now start the Astro server
  await import('../dist/server/entry.mjs');

  if (usePostgres) {
    // Astro loads route modules lazily, so hit the flow route's health
    // endpoint once to load it — its module registers the queue handler the
    // Postgres world needs for in-process execution.
    const url = `http://localhost:${process.env.PORT || 4321}/.well-known/workflow/v1/flow?__health`;
    for (let attempt = 0; attempt < 50; attempt++) {
      try {
        if ((await fetch(url)).ok) return;
      } catch {}
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    console.warn(`Could not reach ${url} to load the workflow flow route.`);
  }
}

main().catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});
