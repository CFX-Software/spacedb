// Pure protocol helpers for spacedb TCP bridge. No FiveM globals.
// Tested directly via node:test; consumed by server/js/spacedb.js.

function parseAddress(value) {
  const [host, port] = String(value || '').split(':');
  return { host: host || '127.0.0.1', port: Number(port || 37121) };
}

// Consume newline-delimited JSON frames from a buffer.
// onFrame(payload | null, rawLine) — null payload means JSON parse failed.
// Returns the remainder (incomplete trailing frame).
function consumeFrames(buffer, onFrame) {
  let buf = buffer;
  let index = buf.indexOf('\n');
  while (index >= 0) {
    const line = buf.slice(0, index);
    buf = buf.slice(index + 1);
    index = buf.indexOf('\n');
    if (!line) continue;

    let payload = null;
    try {
      payload = JSON.parse(line);
    } catch (_err) {
      payload = null;
    }
    onFrame(payload, line);
  }
  return buf;
}

// Pending-map manager with per-request timeouts.
// schedule(setTimeout-compatible) + cancel(clearTimeout-compatible) injected for testability.
function createPendingMap({ schedule = setTimeout, cancel = clearTimeout } = {}) {
  const pending = new Map();

  function add(id, resolve, reject, timeoutMs, op, opts = {}) {
    const entry = { resolve, reject, timer: null, wantsMeta: !!opts.wantsMeta, recvAtNs: 0 };
    if (timeoutMs > 0) {
      entry.timer = schedule(() => {
        if (pending.delete(id)) {
          reject(new Error(`spacedb timeout after ${timeoutMs}ms op=${op}`));
        }
      }, timeoutMs);
    }
    pending.set(id, entry);
    return entry;
  }

  function complete(id, payload) {
    const entry = pending.get(id);
    if (!entry) return false;
    pending.delete(id);
    if (entry.timer) cancel(entry.timer);
    if (payload && payload.ok) {
      // wantsMeta: caller asked for full payload (e.g., profile data) instead
      // of just .result. Keeps the default callers (.execute, .query, etc.)
      // backward compatible.
      if (entry.wantsMeta) {
        entry.resolve({ result: payload.result, profile: payload.profile || null, recvAtNs: entry.recvAtNs || 0 });
      } else {
        entry.resolve(payload.result);
      }
    } else {
      entry.reject(new Error((payload && payload.error) || 'spacedb transport error'));
    }
    return true;
  }

  function markRecv(id, recvAtNs) {
    const entry = pending.get(id);
    if (entry) entry.recvAtNs = recvAtNs;
  }

  function fail(id, err) {
    const entry = pending.get(id);
    if (!entry) return false;
    pending.delete(id);
    if (entry.timer) cancel(entry.timer);
    entry.reject(err);
    return true;
  }

  function closeAll(err) {
    for (const [, entry] of pending) {
      if (entry.timer) cancel(entry.timer);
      entry.reject(err);
    }
    pending.clear();
  }

  return { add, complete, fail, markRecv, closeAll, size: () => pending.size, has: (id) => pending.has(id) };
}

// Tiny insertion-ordered LRU mirror of the Go-side row cache. Keyed by
// `${table}\x00${key}`. Skips the TCP round trip on cache hits. Server
// pushes invalidation events over the same socket to keep it coherent.
function createMirror({ maxEntries = 10_000 } = {}) {
  const map = new Map();

  function get(table, key) {
    const k = `${table}\x00${key}`;
    if (!map.has(k)) return undefined;
    const row = map.get(k);
    // Touch: re-insert to move to back of insertion order (LRU tail).
    map.delete(k);
    map.set(k, row);
    return row;
  }

  function set(table, key, row) {
    const k = `${table}\x00${key}`;
    if (map.has(k)) map.delete(k);
    map.set(k, row);
    if (map.size > maxEntries) {
      // Evict oldest (insertion order = LRU head).
      const oldest = map.keys().next().value;
      if (oldest !== undefined) map.delete(oldest);
    }
  }

  function invalidate(table, key) {
    map.delete(`${table}\x00${key}`);
  }

  function invalidateTable(table) {
    const prefix = `${table}\x00`;
    for (const k of map.keys()) {
      if (k.startsWith(prefix)) map.delete(k);
    }
  }

  function size() { return map.size; }
  function clear() { map.clear(); }

  return { get, set, invalidate, invalidateTable, size, clear };
}

module.exports = { parseAddress, consumeFrames, createPendingMap, createMirror };
