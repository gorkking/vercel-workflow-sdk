import { describe, expect, it } from 'vitest';
import { createWorld } from './index.js';

describe('postgres world construction', () => {
  it('opens no connections until a stream is read and closes cleanly', async () => {
    // Port 1 is unreachable: constructing (and closing) a world must succeed
    // without ever touching the database. The runtime constructs worlds
    // eagerly at module load, including during framework builds, so world
    // construction doing I/O would surface as connection errors at build
    // time and leaked LISTEN connections at runtime.
    const world = createWorld({
      connectionString: 'postgres://user:pw@127.0.0.1:1/db',
    });

    await expect(world.close?.()).resolves.toBeUndefined();
  });
});
