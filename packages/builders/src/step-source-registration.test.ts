import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { BaseBuilder, type DiscoveredEntries } from './base-builder.js';
import type { StandaloneConfig } from './types.js';

class TestBuilder extends BaseBuilder {
  async build(): Promise<void> {
    // no-op
  }

  public createSourceStepRegistrations(
    inputFiles: string[],
    outfile: string,
    discoveredEntries: DiscoveredEntries
  ) {
    return this.createStepsBundle({
      inputFiles,
      outfile,
      externalizeNonSteps: true,
      bundleTransitiveLocalStepDependencies: false,
      sourceStepRegistrationImports: true,
      discoveredEntries,
    });
  }

  public createCombinedRoute(
    inputFiles: string[],
    stepsOutfile: string,
    flowOutfile: string
  ) {
    return this.createCombinedBundle({
      inputFiles,
      stepsOutfile,
      flowOutfile,
      bundleFinalOutput: false,
      externalizeNonSteps: true,
      bundleTransitiveLocalStepDependencies: false,
      sourceStepRegistrationImports: true,
    });
  }
}

const realTmpdir = realpathSync(tmpdir());

function writeFile(path: string, contents: string): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, contents, 'utf-8');
}

function createBuilder(workingDir: string): TestBuilder {
  const config: StandaloneConfig = {
    buildTarget: 'standalone',
    workingDir,
    dirs: ['.'],
    stepsBundlePath: join(workingDir, '.workflow', 'steps.js'),
    workflowsBundlePath: join(workingDir, '.workflow', 'workflows.js'),
    webhookBundlePath: join(workingDir, '.workflow', 'webhook.js'),
  };
  return new TestBuilder(config);
}

describe('step source registration', () => {
  let testRoot: string;

  beforeEach(() => {
    testRoot = mkdtempSync(join(realTmpdir, 'workflow-step-registration-'));
    writeFile(
      join(testRoot, 'node_modules', 'workflow', 'package.json'),
      JSON.stringify({
        name: 'workflow',
        version: '1.0.0',
        type: 'module',
        exports: {
          './runtime': './runtime.js',
          './internal/builtins': './internal/builtins.js',
        },
      })
    );
    writeFile(
      join(testRoot, 'node_modules', 'workflow', 'runtime.js'),
      `globalThis.__capturedStepLoaders = [];
export function registerStepFunctionLoader(stepId, loader) {
  globalThis.__capturedStepLoaders.push({ stepId, loader });
}
export function workflowEntrypoint() {
  return function POST() {
    return new Response('ok');
  };
}
`
    );
    writeFile(
      join(testRoot, 'node_modules', 'workflow', 'internal', 'builtins.js'),
      'export const __builtins = true;\n'
    );
  });

  afterEach(() => {
    rmSync(testRoot, { recursive: true, force: true });
  });

  it('imports serde-only files for step context class registration', async () => {
    const entryFile = join(testRoot, 'src', 'entry.ts');
    const stepFile = join(testRoot, 'src', 'step.ts');
    const serdeFile = join(testRoot, 'src', 'serde.ts');
    const outfile = join(testRoot, '.workflow', 'steps.js');

    mkdirSync(dirname(outfile), { recursive: true });
    writeFile(entryFile, `export { runStep } from './step';\n`);
    writeFile(
      stepFile,
      `export async function runStep() {
  'use step';
  return 1;
}
`
    );
    writeFile(
      serdeFile,
      `export class Value {
  static classId = 'Value';
  static [Symbol.for('workflow-serialize')](value: Value) {
    return value;
  }
  static [Symbol.for('workflow-deserialize')](value: Value) {
    return value;
  }
}
`
    );

    const discoveredEntries: DiscoveredEntries = {
      discoveredSteps: new Set([stepFile]),
      discoveredWorkflows: new Set(),
      discoveredSerdeFiles: new Set([serdeFile]),
    };

    const { manifest } = await createBuilder(
      testRoot
    ).createSourceStepRegistrations([entryFile], outfile, discoveredEntries);
    const generated = readFileSync(outfile, 'utf-8');

    expect(generated).toContain('import "workflow/internal/builtins";');
    expect(generated).toContain('import "../src/step.ts";');
    expect(generated).toContain('import "../src/serde.ts";');
    expect(Object.keys(manifest.classes ?? {})).toContain('src/serde.ts');
  });

  it('registers lazy step loaders in combined routes', async () => {
    const workflowFile = join(testRoot, 'workflows', 'image.ts');
    const stepsOutfile = join(testRoot, '.workflow', '__step_registrations.js');
    const flowOutfile = join(testRoot, '.workflow', 'route.js');

    mkdirSync(dirname(flowOutfile), { recursive: true });
    writeFile(
      workflowFile,
      `export async function imageWorkflow() {
  'use workflow';
  return resize();
}

export async function resize() {
  'use step';
  return 1;
}
`
    );

    const { stepsManifest } = await createBuilder(testRoot).createCombinedRoute(
      [workflowFile],
      stepsOutfile,
      flowOutfile
    );
    const routeCode = readFileSync(flowOutfile, 'utf-8');
    const stepIds = Object.values(stepsManifest.steps ?? {}).flatMap(
      (entries) => Object.values(entries).map(({ stepId }) => stepId)
    );

    expect(stepIds).toHaveLength(1);
    expect(routeCode).toContain(
      "import { registerStepFunctionLoader, workflowEntrypoint } from 'workflow/runtime';"
    );
    expect(routeCode).toContain(
      `registerStepFunctionLoader(${JSON.stringify(stepIds[0])}, () => import("./__step_registrations.js"));`
    );
    expect(routeCode).not.toContain('import { __steps_registered }');
    expect(routeCode).not.toContain('void __steps_registered');
  });

  it('defers step registration module load failures until a step loader runs', async () => {
    const workflowFile = join(testRoot, 'workflows', 'image.ts');
    const stepsOutfile = join(testRoot, '.workflow', '__step_registrations.js');
    const legacyStepsOutfile = join(
      testRoot,
      '.workflow',
      '__legacy_step_registrations.js'
    );
    const legacyRouteOutfile = join(testRoot, '.workflow', 'legacy-route.js');
    const flowOutfile = join(testRoot, '.workflow', 'route.js');
    const sharpLoadError =
      'Could not load the "sharp" module using the linux-x64 runtime';

    mkdirSync(dirname(flowOutfile), { recursive: true });
    writeFile(
      workflowFile,
      `export async function imageWorkflow() {
  'use workflow';
  return resize();
}

export async function resize() {
  'use step';
  return 1;
}
`
    );
    writeFile(
      legacyStepsOutfile,
      `throw new Error(${JSON.stringify(sharpLoadError)});
export const __steps_registered = true;
`
    );
    writeFile(
      legacyRouteOutfile,
      `import { __steps_registered } from './__legacy_step_registrations.js';
void __steps_registered;
export function POST() {}
`
    );

    await expect(
      import(`${pathToFileURL(legacyRouteOutfile).href}?legacy=${Date.now()}`)
    ).rejects.toThrow(sharpLoadError);

    const { stepsManifest } = await createBuilder(testRoot).createCombinedRoute(
      [workflowFile],
      stepsOutfile,
      flowOutfile
    );
    const stepIds = Object.values(stepsManifest.steps ?? {}).flatMap(
      (entries) => Object.values(entries).map(({ stepId }) => stepId)
    );
    writeFile(
      stepsOutfile,
      `throw new Error(${JSON.stringify(sharpLoadError)});
export const __steps_registered = true;
`
    );

    await expect(
      import(`${pathToFileURL(flowOutfile).href}?fixed=${Date.now()}`)
    ).resolves.toHaveProperty('POST');

    const loaders = (
      globalThis as typeof globalThis & {
        __capturedStepLoaders?: Array<{
          stepId: string;
          loader: () => Promise<unknown>;
        }>;
      }
    ).__capturedStepLoaders;
    expect(loaders).toEqual([expect.objectContaining({ stepId: stepIds[0] })]);
    await expect(loaders?.[0]?.loader()).rejects.toThrow(sharpLoadError);
  });
});
