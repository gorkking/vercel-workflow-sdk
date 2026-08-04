---
'@workflow/nitro': patch
'@workflow/nest': patch
---

Load the generated workflow flow module at server boot so its queue handler registers without an HTTP request — required for worlds like `@workflow/world-postgres` that execute queue jobs in-process. Nitro dev now boot-loads the flow bundle (production already loads it eagerly), and the Nest `WorkflowModule` imports the generated bundles in `onModuleInit`.
