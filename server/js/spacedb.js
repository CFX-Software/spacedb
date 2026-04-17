const net = require('net');
const path = require('path');
const { parseAddress, consumeFrames, createPendingMap, createMirror } = require('./server/js/protocol');
const { ensureCore } = require('./server/js/lifecycle');

const endpoint = GetConvar('spacedb_endpoint', 'http://127.0.0.1:37120');
const transportEndpoint = GetConvar('spacedb_transport', '127.0.0.1:37121');
const requestTimeoutMs = Number(GetConvar('spacedb_request_timeout_ms', '30000')) || 30000;
const manageCore = GetConvar('spacedb_manage_core', 'true') === 'true';
const coreMode = GetConvar('spacedb_core_mode', 'restart'); // 'restart' | 'reuse'
const resourceName = GetCurrentResourceName();
const resourceRoot = GetResourcePath(resourceName);
const subscriptions = new Set();
const pending = createPendingMap();
const mirrorMax = Number(GetConvar('spacedb_mirror_max_entries', '10000')) || 10000;
const mirror = createMirror({ maxEntries: mirrorMax });
let mirrorHits = 0;
let nextId = 0;
let socket = null;
let buffer = '';
let connecting = null;
let supervisedChild = null;
let shuttingDown = false;
let respawnAttempt = 0;

// Deferred so callers can `await coreReady()` and get blocked until the
// CURRENT bootCore() resolves — even across respawns. On crash we install a
// fresh deferred before the boot retry so transport() does not race a stale
// resolved promise into a dead socket.
function makeDeferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}
let coreDeferred = makeDeferred();
function coreReady() { return coreDeferred.promise; }

function watchChild(child) {
  if (!child) return;
  supervisedChild = child;
  child.once('exit', (code, signal) => {
    if (shuttingDown) return;
    console.log(`[spacedb] WARN core exited unexpectedly code=${code} signal=${signal}; respawning`);
    pending.closeAll(new Error('spacedb core crashed; respawning'));
    if (socket && !socket.destroyed) socket.destroy();
    socket = null;
    // Block new callers until the respawn finishes.
    coreDeferred = makeDeferred();
    const delay = Math.min(5000, 200 * 2 ** respawnAttempt);
    respawnAttempt += 1;
    setTimeout(bootCore, delay);
  });
}

function bootCore() {
  if (!manageCore) {
    coreDeferred.resolve({ skipped: true });
    return;
  }
  const isWindows = process.platform === 'win32';
  const binaryConvar = GetConvar('spacedb_core_path', '');
  const binary = binaryConvar !== ''
    ? binaryConvar
    : path.join(resourceRoot, 'bin', isWindows ? 'spacedb-core.exe' : 'spacedb-core');
  const config = path.join(resourceRoot, 'config.json');
  const logPath = path.join(resourceRoot, 'spacedb-core.log');
  const transportAddr = parseAddress(transportEndpoint);

  ensureCore({
    endpoint,
    transportPort: String(transportAddr.port),
    binary,
    config,
    logPath,
    mode: coreMode,
  }).then((result) => {
    if (result.reused && result.killFailed) {
      console.log(`[spacedb] could not kill stale core (different session?); reusing existing driver=${result.driver}`);
    } else if (result.reused) {
      console.log(`[spacedb] reusing existing core driver=${result.driver}`);
    } else {
      console.log(`[spacedb] core ready driver=${result.driver} pid=${result.pid} endpoint=${endpoint}`);
    }
    if (result.child) watchChild(result.child);
    respawnAttempt = 0;
    coreDeferred.resolve(result);
  }).catch((err) => {
    console.log(`[spacedb] core boot failed: ${err.message}`);
    if (shuttingDown) {
      coreDeferred.reject(err);
      return;
    }
    const delay = Math.min(5000, 200 * 2 ** respawnAttempt);
    respawnAttempt += 1;
    console.log(`[spacedb] retrying core boot in ${delay}ms (attempt ${respawnAttempt})`);
    setTimeout(bootCore, delay);
  });
}

bootCore();

function connectTransport() {
  if (socket && !socket.destroyed) return Promise.resolve(socket);
  if (connecting) return connecting;

  connecting = new Promise((resolve, reject) => {
    const address = parseAddress(transportEndpoint);
    const conn = net.createConnection(address, () => {
      socket = conn;
      connecting = null;
      console.log(`[spacedb] tcp transport connected ${transportEndpoint}`);
      resolve(conn);
    });

    conn.setNoDelay(true);
    conn.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      const recvNs = Number(process.hrtime.bigint());
      buffer = consumeFrames(buffer, (payload, line) => {
        if (!payload) {
          console.log(`[spacedb] invalid tcp response: ${line}`);
          return;
        }
        // Unsolicited events from the server (cache invalidations, etc.)
        // arrive without an id. Route on the event field.
        if (payload.event === 'invalidate') {
          if (payload.key) mirror.invalidate(payload.table, payload.key);
          else mirror.invalidateTable(payload.table);
          return;
        }
        pending.markRecv(payload.id, recvNs);
        pending.complete(payload.id, payload);
      });
    });

    conn.on('error', (err) => {
      if (connecting) {
        connecting = null;
        reject(err);
      }
    });

    conn.on('close', () => {
      if (socket === conn) socket = null;
      pending.closeAll(new Error('spacedb transport closed'));
    });
  });

  return connecting;
}

// Dead code: kept for emergency HTTP fallback. All JS exports use transport() (TCP).
// If this fires, hot path leaked to HTTP — investigate.
function request(method, path, body) {
  console.log(`[spacedb] WARN unexpected HTTP fallback method=${method} path=${path}`);
  return new Promise((resolve, reject) => {
    PerformHttpRequest(`${endpoint}${path}`, (status, response) => {
      let decoded = null;
      if (response) {
        try {
          decoded = JSON.parse(response);
        } catch (err) {
          reject(new Error(`invalid JSON response: ${err.message}`));
          return;
        }
      }

      if (status < 200 || status >= 300) {
        reject(new Error(decoded?.error || `HTTP ${status}`));
        return;
      }

      if (decoded?.error) {
        reject(new Error(decoded.error));
        return;
      }

      resolve(decoded);
    }, method, body ? JSON.stringify(body) : '', {
      'Content-Type': 'application/json',
    });
  });
}

async function transport(op, payload, opts = {}) {
  await coreReady();
  const conn = await connectTransport();
  const id = `${Date.now()}_${++nextId}`;
  const wire = { ...payload, id, op };
  if (opts.profile) wire.profile = true;
  const message = JSON.stringify(wire);

  return new Promise((resolve, reject) => {
    let sendAtNs = 0;
    if (opts.profile) {
      pending.add(id, (meta) => {
        // meta = { result, profile, recvAtNs } from protocol.js wantsMeta path.
        resolve({
          result: meta.result,
          profile: meta.profile,
          bridgeNs: meta.recvAtNs > 0 && sendAtNs > 0 ? meta.recvAtNs - sendAtNs : 0,
        });
      }, reject, requestTimeoutMs, op, { wantsMeta: true });
    } else {
      pending.add(id, resolve, reject, requestTimeoutMs, op);
    }
    sendAtNs = Number(process.hrtime.bigint());
    conn.write(`${message}\n`, 'utf8', (err) => {
      if (err) pending.fail(id, err);
    });
  });
}

// Best-effort extraction of the affected table from a write statement so
// the JS mirror can be invalidated *before* the server's invalidation
// event arrives (defense against the read-after-write race). The Go
// parser is authoritative; this is just a fast preemptive purge.
const writeTableRe = /^\s*(?:update|delete\s+from|insert\s+into|replace\s+into|truncate(?:\s+table)?)\s+([A-Za-z_][A-Za-z0-9_]*)/i;
function preInvalidateMirror(sql) {
  if (typeof sql !== 'string') return;
  const m = sql.match(writeTableRe);
  if (m) mirror.invalidateTable(m[1]);
  else mirror.clear(); // unknown shape — be safe
}

function query(sqlOrName, params = []) {
  return transport('query', { query: sqlOrName, params });
}

function single(sqlOrName, params = []) {
  return transport('single', { query: sqlOrName, params });
}

function execute(sqlOrName, params = []) {
  preInvalidateMirror(sqlOrName);
  return transport('execute', { query: sqlOrName, params });
}

function executeProfiled(sqlOrName, params = []) {
  return transport('execute', { query: sqlOrName, params }, { profile: true });
}

function queryProfiled(sqlOrName, params = []) {
  return transport('query', { query: sqlOrName, params }, { profile: true });
}

// In-process read cache. Two-tier: JS mirror (in-process Node memory)
// → Go cache (in-process Go memory) → MySQL. Mirror hits skip the TCP
// round trip entirely and return in <100 µs. PK defaults to "id".
async function getById(table, key, pkColumn = 'id') {
  const skey = String(key);
  const cached = mirror.get(table, skey);
  if (cached !== undefined) {
    mirrorHits += 1;
    return cached;
  }
  const result = await transport('cacheGet', { table, key: skey, pkColumn });
  const row = result && result.row != null ? result.row : null;
  if (row !== null) mirror.set(table, skey, row);
  return row;
}

// Pure cache update — caller is responsible for persisting the row via
// execute/insert/update first. Updates both tiers.
async function setById(table, key, row) {
  const skey = String(key);
  mirror.set(table, skey, row);
  return transport('cacheSet', { table, key: skey, row });
}

async function invalidate(table, key) {
  const skey = String(key);
  mirror.invalidate(table, skey);
  return transport('cacheInvalidate', { table, key: skey });
}

async function invalidateTable(table) {
  mirror.invalidateTable(table);
  return transport('cacheInvalidate', { table });
}

async function cacheStats() {
  const remote = await transport('cacheStats', {});
  return {
    server: remote,
    mirror: { entries: mirror.size(), hits: mirrorHits },
  };
}

function executeMany(sqlOrName, rows = []) {
  preInvalidateMirror(sqlOrName);
  return transport('executeMany', { query: sqlOrName, rows });
}

function prepare(name, sql, options = {}) {
  return transport('prepare', { name, sql, options });
}

function transaction(steps = []) {
  for (const step of steps || []) {
    if (step && typeof step.query === 'string') preInvalidateMirror(step.query);
  }
  return transport('transaction', { steps });
}

async function subscribe(sqlOrName, params = [], callback) {
  const result = await transport('subscribe', { query: sqlOrName, params });
  if (result?.id) subscriptions.add(result.id);
  if (callback && result?.id) {
    const id = result.id;
    const poll = async () => {
      try {
        const payload = await transport('events', { subId: id });
        for (const event of payload?.events || []) callback(event);
      } finally {
        setTimeout(poll, 250);
      }
    };
    poll();
  }
  return result;
}

function unsubscribe(id) {
  subscriptions.delete(id);
  return transport('unsubscribe', { subId: id });
}

function health() {
  return transport('health', {});
}

function stats() {
  return transport('stats', {});
}

exports('query', query);
exports('single', single);
exports('execute', execute);
exports('executeMany', executeMany);
exports('prepare', prepare);
exports('transaction', transaction);
exports('subscribe', subscribe);
exports('unsubscribe', unsubscribe);
exports('health', health);
exports('stats', stats);
exports('executeProfiled', executeProfiled);
exports('queryProfiled', queryProfiled);
exports('getById', getById);
exports('setById', setById);
exports('invalidate', invalidate);
exports('invalidateTable', invalidateTable);
exports('cacheStats', cacheStats);

on('onResourceStop', (stopped) => {
  if (stopped !== GetCurrentResourceName()) return;
  shuttingDown = true;
  for (const id of subscriptions) {
    transport('unsubscribe', { subId: id }).catch(() => {});
  }
  if (socket && !socket.destroyed) socket.destroy();
  // Leave supervisedChild alone — it's detached and may be reused by a
  // later resource boot in 'reuse' mode. 'restart' mode will kill it on the
  // next boot's killByPort sweep.
});
