const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { setTimeout: delay } = require('node:timers/promises');
const { test } = require('node:test');
const { readProjectMirrorsInConsumer } = require('./project-mirror-consumer');

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mirror-consumer-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const receipt = path.join(root, 'child.pid');
  const preload = path.join(root, 'hang.cjs');
  fs.writeFileSync(
    preload,
    `require('node:fs').writeFileSync(${JSON.stringify(receipt)}, String(process.pid)); process.on('SIGTERM', () => {}); setInterval(() => {}, 1000);`,
  );
  const previous = process.env.NODE_OPTIONS;
  process.env.NODE_OPTIONS = `--require=${JSON.stringify(preload)}`;
  t.after(() => {
    if (previous === undefined) delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = previous;
  });
  return receipt;
}

function assertExited(receipt) {
  assert.equal(fs.existsSync(receipt), true, 'the owned real child must have started');
  const pid = Number(fs.readFileSync(receipt, 'utf8'));
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' }, 'the child must be gone before rejection');
}

test('consumer deadline waits for its owned Node to close before rejecting', async (t) => {
  const receipt = fixture(t);
  const diagnostics = [];
  await assert.rejects(
    readProjectMirrorsInConsumer({
      nodeExe: process.execPath,
      entries: [],
      timeoutMs: 1000,
      onDiagnostic: (event) => diagnostics.push(event),
    }),
    /timed out/,
  );
  assertExited(receipt);
  assert.equal(diagnostics.length, 2);
  assert.equal(diagnostics[1].status, 'timed_out');
  assert.equal(diagnostics[1].signal, 'SIGKILL');
  assert.equal(diagnostics[1].pid, diagnostics[0].pid);
});

test('consumer cancellation waits for its owned Node to close before rejecting', async (t) => {
  const receipt = fixture(t);
  const controller = new AbortController();
  const diagnostics = [];
  const reading = readProjectMirrorsInConsumer({
    nodeExe: process.execPath,
    entries: [],
    signal: controller.signal,
    timeoutMs: 3000,
    onDiagnostic: (event) => diagnostics.push(event),
  });
  const rejected = assert.rejects(reading, /controlled consumer cancellation/);
  for (let i = 0; i < 200 && !fs.existsSync(receipt); i += 1) await delay(10);
  controller.abort(new Error('controlled consumer cancellation'));
  await rejected;
  assertExited(receipt);
  assert.equal(diagnostics.length, 2);
  assert.equal(diagnostics[1].status, 'aborted');
  assert.equal(diagnostics[1].pid, diagnostics[0].pid);
});

test('a missing consumer executable returns its spawn error without hanging', async () => {
  await assert.rejects(
    readProjectMirrorsInConsumer({
      nodeExe: path.join(os.tmpdir(), 'project-mirror-node-does-not-exist.exe'),
      entries: [],
      timeoutMs: 1000,
    }),
    { code: 'ENOENT' },
  );
});

test('the real consumer returns structured missing-file errors for source and mirror', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mirror-consumer-missing-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const entries = ['source', 'mirror'].map((kind) => ({
    kind,
    path: path.join(root, kind, 'missing.yaml'),
  }));
  const failures = await readProjectMirrorsInConsumer({ nodeExe: process.execPath, entries, timeoutMs: 5000 });
  assert.equal(failures.length, entries.length);
  for (const [index, failure] of failures.entries()) {
    assert.equal(failure.kind, entries[index].kind);
    assert.equal(failure.path, entries[index].path);
    assert.equal(failure.code, 'ENOENT');
    assert.equal(typeof failure.errno, 'number');
    assert.equal(failure.syscall, 'open');
  }
});
