import { SPEC_VERSION_CURRENT, type World } from '@workflow/world';
import { afterEach, expect, it, vi } from 'vitest';

const targetWorld = vi.hoisted(() => Promise.withResolvers<World>());

vi.mock('@workflow/world-local', () => ({
  createWorld: () => targetWorld.promise,
}));

import { getWorldHandlers, setWorld } from './world.js';

afterEach(() => {
  setWorld(undefined);
  vi.unstubAllEnvs();
});

it('does not replace a world installed while the target world is loading', async () => {
  vi.stubEnv('WORKFLOW_TARGET_WORLD', 'local');
  setWorld(undefined);
  const handlers = getWorldHandlers();
  const createQueueHandler = vi.fn();

  setWorld({
    specVersion: SPEC_VERSION_CURRENT,
    createQueueHandler,
  } as unknown as World);
  targetWorld.resolve({
    specVersion: SPEC_VERSION_CURRENT,
    createQueueHandler: vi.fn(),
  } as unknown as World);

  await expect(handlers).resolves.toMatchObject({ createQueueHandler });
});
