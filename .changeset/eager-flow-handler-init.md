---
'@workflow/core': patch
'@workflow/astro': patch
'@workflow/sveltekit': patch
---

Build the flow route's queue handler eagerly at module load (retrying transient failures on the next request) so worlds see `createQueueHandler` before the first request. Astro and SvelteKit generated routes now create the entrypoint once at module scope instead of per request.
