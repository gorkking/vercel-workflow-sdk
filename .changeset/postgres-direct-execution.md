---
'@workflow/world-postgres': minor
'@workflow/world-testing': patch
---

`@workflow/world-postgres` now executes queue jobs by calling the registered in-process workflow handler directly instead of POSTing to the app's own HTTP routes. The HTTP loopback path is removed entirely — including `WORKFLOW_LOCAL_BASE_URL`/`PORT` handling, port detection, and TCP readiness probing. The graphile-worker runner starts only once a workflow handler is registered, so jobs are never consumed by a process that cannot execute them (enqueue-only processes stay inert). Load the generated workflow route module in the worker process before/alongside `world.start()`. Graphile task names and job payloads are unchanged, so in-flight and suspended runs survive the upgrade.
