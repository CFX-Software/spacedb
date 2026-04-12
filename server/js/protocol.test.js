const test = require('node:test');
const assert = require('node:assert/strict');
const { parseAddress, consumeFrames, createPendingMap, createMirror } = require('./protocol');

test('parseAddress splits host:port', () => {
  assert.deepEqual(parseAddress('10.0.0.1:5555'), { host: '10.0.0.1', port: 5555 });
});

test('parseAddress defaults host when missing', () => {
  assert.deepEqual(parseAddress(':37121'), { host: '127.0.0.1', port: 37121 });
});

test('parseAddress defaults port when missing', () => {
  assert.deepEqual(parseAddress('localhost'), { host: 'localhost', port: 37121 });
});

test('consumeFrames parses two frames in one chunk', () => {
  const frames = [];
  const remainder = consumeFrames('{"id":"a","ok":true}\n{"id":"b","ok":false}\n', (p) => frames.push(p));
  assert.equal(remainder, '');
  assert.equal(frames.length, 2);
  assert.equal(frames[0].id, 'a');
  assert.equal(frames[1].id, 'b');
});

test('consumeFrames preserves partial trailing frame', () => {
  const frames = [];
  const remainder = consumeFrames('{"id":"a","ok":true}\n{"id":"b","', (p) => frames.push(p));
  assert.equal(remainder, '{"id":"b","');
  assert.equal(frames.length, 1);
});

test('consumeFrames passes null for invalid JSON line', () => {
  const seen = [];
  consumeFrames('not-json\n{"id":"ok","ok":true}\n', (p, raw) => seen.push([p, raw]));
  assert.equal(seen.length, 2);
  assert.equal(seen[0][0], null);
  assert.equal(seen[0][1], 'not-json');
  assert.equal(seen[1][0].id, 'ok');
});

test('consumeFrames skips empty lines', () => {
  const frames = [];
  consumeFrames('\n\n{"id":"a","ok":true}\n', (p) => frames.push(p));
  assert.equal(frames.length, 1);
});

test('pending complete resolves on ok payload', async () => {
  const map = createPendingMap();
  const promise = new Promise((resolve, reject) => map.add('1', resolve, reject, 0, 'query'));
  assert.equal(map.complete('1', { id: '1', ok: true, result: { rows: [] } }), true);
  const result = await promise;
  assert.deepEqual(result, { rows: [] });
  assert.equal(map.size(), 0);
});

test('pending complete rejects on error payload', async () => {
  const map = createPendingMap();
  const promise = new Promise((resolve, reject) => map.add('1', resolve, reject, 0, 'query'));
  map.complete('1', { id: '1', ok: false, error: 'bad sql' });
  await assert.rejects(promise, /bad sql/);
});

test('pending complete returns false for unknown id', () => {
  const map = createPendingMap();
  assert.equal(map.complete('nope', { ok: true }), false);
});

test('pending timeout rejects with op name', async () => {
  let scheduled;
  const fakeSchedule = (fn) => { scheduled = fn; return 'timer'; };
  const fakeCancel = () => {};
  const map = createPendingMap({ schedule: fakeSchedule, cancel: fakeCancel });
  const promise = new Promise((resolve, reject) => map.add('1', resolve, reject, 30000, 'execute'));
  scheduled();
  await assert.rejects(promise, /timeout after 30000ms op=execute/);
  assert.equal(map.size(), 0);
});

test('pending timeout does not fire after complete', async () => {
  let scheduled;
  let cancelled = 0;
  const map = createPendingMap({
    schedule: (fn) => { scheduled = fn; return 'timer'; },
    cancel: () => { cancelled += 1; },
  });
  const promise = new Promise((resolve, reject) => map.add('1', resolve, reject, 30000, 'execute'));
  map.complete('1', { id: '1', ok: true, result: 1 });
  await promise;
  assert.equal(cancelled, 1);
  // Firing the stale timer after complete must not crash.
  scheduled();
  assert.equal(map.size(), 0);
});

test('pending closeAll rejects all entries', async () => {
  const map = createPendingMap();
  const p1 = new Promise((resolve, reject) => map.add('a', resolve, reject, 0, 'op'));
  const p2 = new Promise((resolve, reject) => map.add('b', resolve, reject, 0, 'op'));
  map.closeAll(new Error('socket closed'));
  await assert.rejects(p1, /socket closed/);
  await assert.rejects(p2, /socket closed/);
  assert.equal(map.size(), 0);
});

test('pending wantsMeta resolves with full meta object', async () => {
  const map = createPendingMap();
  const promise = new Promise((resolve, reject) => map.add('1', resolve, reject, 0, 'execute', { wantsMeta: true }));
  map.markRecv('1', 12345);
  map.complete('1', { id: '1', ok: true, result: { affected: 1 }, profile: { serverTotalNs: 500 } });
  const meta = await promise;
  assert.deepEqual(meta.result, { affected: 1 });
  assert.deepEqual(meta.profile, { serverTotalNs: 500 });
  assert.equal(meta.recvAtNs, 12345);
});

test('pending wantsMeta omitted gives raw result (back-compat)', async () => {
  const map = createPendingMap();
  const promise = new Promise((resolve, reject) => map.add('1', resolve, reject, 0, 'execute'));
  map.complete('1', { id: '1', ok: true, result: { affected: 1 }, profile: { serverTotalNs: 500 } });
  const result = await promise;
  assert.deepEqual(result, { affected: 1 });
});

test('markRecv on unknown id is a no-op', () => {
  const map = createPendingMap();
  assert.doesNotThrow(() => map.markRecv('missing', 1));
});

test('mirror get/set round-trip', () => {
  const m = createMirror({ maxEntries: 100 });
  m.set('users', '1', { id: 1, name: 'Jane' });
  assert.deepEqual(m.get('users', '1'), { id: 1, name: 'Jane' });
});

test('mirror get miss returns undefined', () => {
  const m = createMirror();
  assert.equal(m.get('users', 'missing'), undefined);
});

test('mirror invalidate drops one entry', () => {
  const m = createMirror();
  m.set('users', '1', { id: 1 });
  m.invalidate('users', '1');
  assert.equal(m.get('users', '1'), undefined);
});

test('mirror invalidateTable drops all rows of that table', () => {
  const m = createMirror();
  for (let i = 0; i < 5; i += 1) {
    m.set('users', String(i), { id: i });
    m.set('posts', String(i), { id: i });
  }
  m.invalidateTable('users');
  assert.equal(m.get('users', '3'), undefined);
  assert.deepEqual(m.get('posts', '3'), { id: 3 });
  assert.equal(m.size(), 5);
});

test('mirror evicts oldest when over cap', () => {
  const m = createMirror({ maxEntries: 3 });
  m.set('t', 'a', { v: 1 });
  m.set('t', 'b', { v: 2 });
  m.set('t', 'c', { v: 3 });
  m.set('t', 'd', { v: 4 }); // should evict 'a'
  assert.equal(m.get('t', 'a'), undefined);
  assert.deepEqual(m.get('t', 'd'), { v: 4 });
  assert.equal(m.size(), 3);
});

test('mirror get touches entry (LRU)', () => {
  const m = createMirror({ maxEntries: 3 });
  m.set('t', 'a', { v: 1 });
  m.set('t', 'b', { v: 2 });
  m.set('t', 'c', { v: 3 });
  // Touch 'a' so 'b' becomes the LRU victim.
  m.get('t', 'a');
  m.set('t', 'd', { v: 4 });
  assert.deepEqual(m.get('t', 'a'), { v: 1 });
  assert.equal(m.get('t', 'b'), undefined);
});

test('mirror clear empties the map', () => {
  const m = createMirror();
  m.set('t', '1', { v: 1 });
  m.clear();
  assert.equal(m.size(), 0);
});

test('pending fail rejects single entry and clears timer', async () => {
  let cancelled = 0;
  const map = createPendingMap({
    schedule: () => 'timer',
    cancel: () => { cancelled += 1; },
  });
  const promise = new Promise((resolve, reject) => map.add('a', resolve, reject, 30000, 'op'));
  map.fail('a', new Error('write failed'));
  await assert.rejects(promise, /write failed/);
  assert.equal(cancelled, 1);
});
