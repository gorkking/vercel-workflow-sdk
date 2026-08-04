---
'@workflow/world': patch
'@workflow/world-local': patch
'@workflow/world-postgres': patch
---

Move the HTTP queue-handler wrapper and the Uint8Array-safe queue message codec into `@workflow/world` (`createFetchQueueHandler`, `serializeQueueMessage`, `deserializeQueueMessage`). `@workflow/world-local` uses the shared implementation, and `@workflow/world-postgres` no longer depends on `@workflow/world-local`.
