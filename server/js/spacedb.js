const net = require('net');
const path = require('path');
const fs = require('fs');
const { parseAddress, consumeFrames, createPendingMap, createMirror } = require('./server/js/protocol');
const { ensureCore } = require('./server/js/lifecycle');
const { parseConnString } = require('./server/js/connstring');

const endpoint = GetConvar('spacedb_endpoint', 'http://127.0.0.1:37120');
const transportEndpoint = GetConvar('spacedb_transport', '127.0.0.1:37121');
const requestTimeoutMs = Number(GetConvar('spacedb_request_timeout_ms', '30000')) || 30000;
const manageCore = GetConvar('spacedb_manage_core', 'true') === 'true';
const coreMode = GetConvar('spacedb_core_mode', 'restart'); // 'restart' | 'reuse'

// Level-gated logger. Defaults to `info` so production boots with one
// "core ready" line plus warnings/errors only. Set `spacedb_log_level
// debug` to see TCP connects, retries, and per-callback details.
const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const logLevel = (GetConvar('spacedb_log_level', 'info') || 'info').toLowerCase();
const logThreshold = LOG_LEVELS[logLevel] !== undefined ? LOG_LEVELS[logLevel] : LOG_LEVELS.info;
function logAt(level, msg) {
  if (LOG_LEVELS[level] > logThreshold) return;
  const prefix = level === 'info' ? '' : level.toUpperCase() + ' ';
  console.log(`[spacedb] ${prefix}${msg}`);
}
const log = {
  error: (m) => logAt('error', m),
  warn: (m) => logAt('warn', m),
  info: (m) => logAt('info', m),
  debug: (m) => logAt('debug', m),
};
const resourceName = GetCurrentResourceName();
const resourceRoot = GetResourcePath(resourceName);
const subscriptions = new Set();
const subscriptionCallbacks = new Map();
// Buffers events that arrive between the subscribe response and the
// callback being registered (Go fires the initial check immediately,
// which can race the response trip back). Drained on callback set.
const subscriptionBuffer = new Map();
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
    log.warn(`core exited unexpectedly code=${code} signal=${signal}; respawning`);
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

// Aligns config.json with the FiveM convar `mysql_connection_string` (the
// same convar oxmysql uses). Behavior:
//   1. No config.json + no convar  -> error, tell user to set the convar.
//   2. No config.json + convar set -> generate a full config from convar.
//   3. Config.json exists + no convar -> use file as-is.
//   4. Config.json exists + convar set -> overwrite ONLY database.dsn and
//      database.driver if they differ. Pool size, ports, realtime config,
//      and any other tuning the user added are preserved.
function ensureConfig(configPath) {
  const raw = GetConvar('mysql_connection_string', '');
  const exists = fs.existsSync(configPath);

  if (!exists && raw === '') {
    log.error('no config.json and no `mysql_connection_string` convar set');
    log.error('add to server.cfg:  set mysql_connection_string "mysql://user:pass@host:port/db"');
    return false;
  }

  if (!exists) {
    const parsed = parseConnString(raw);
    if (!parsed) {
      log.error(`could not parse mysql_connection_string: ${raw}`);
      return false;
    }
    const cfg = {
      listen: '127.0.0.1:37120',
      transport: { listen: '127.0.0.1:37121' },
      database: {
        driver: parsed.driver,
        dsn: parsed.dsn,
        maxOpenConns: 128,
        maxIdleConns: 64,
        connMaxLifetimeSeconds: 1800,
        queryTimeoutMs: 5000,
        slowQueryMs: 100,
      },
      realtime: { enabled: true, pollIntervalMs: 250 },
    };
    try {
      fs.writeFileSync(configPath, JSON.stringify(cfg, null, 2));
      log.info(`generated config.json from mysql_connection_string (driver=${parsed.driver})`);
      return true;
    } catch (err) {
      log.error(`failed to write config.json: ${err.message}`);
      return false;
    }
  }

  if (raw === '') return true;

  const parsed = parseConnString(raw);
  if (!parsed) {
    log.warn(`could not parse mysql_connection_string, falling back to config.json: ${raw}`);
    return true;
  }
  try {
    const current = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    current.database = current.database || {};
    if (current.database.dsn === parsed.dsn && current.database.driver === parsed.driver) {
      return true;
    }
    current.database.dsn = parsed.dsn;
    current.database.driver = parsed.driver;
    fs.writeFileSync(configPath, JSON.stringify(current, null, 2));
    log.info(`synced database.dsn/driver from mysql_connection_string (driver=${parsed.driver})`);
    return true;
  } catch (err) {
    log.warn(`config sync failed: ${err.message}; using existing config.json`);
    return true;
  }
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

  if (!ensureConfig(config)) {
    coreDeferred.reject(new Error('spacedb has no config'));
    return;
  }

  ensureCore({
    endpoint,
    transportPort: String(transportAddr.port),
    binary,
    config,
    logPath,
    mode: coreMode,
  }).then((result) => {
    if (result.reused && result.killFailed) {
      log.warn(`could not kill stale core; reusing existing driver=${result.driver}`);
    } else if (result.reused) {
      log.debug(`reusing existing core driver=${result.driver}`);
    } else {
      log.info(`ready driver=${result.driver}`);
    }
    if (result.child) watchChild(result.child);
    respawnAttempt = 0;
    coreDeferred.resolve(result);
  }).catch((err) => {
    log.error(`core boot failed: ${err.message}`);
    // FiveM's Node sandbox denies child_process.spawn unless the server
    // operator opts in via server.cfg. The grant is global to the resource,
    // so the resource itself cannot enable it from fxmanifest.lua. Emit a
    // clear setup hint instead of leaving the user to decode Node's error.
    const msg = String(err && err.message || '');
    if (err && (err.code === 'ERR_ACCESS_DENIED' || /allow-child-process|Access to this API has been restricted/i.test(msg))) {
      log.error('spacedb core is spawned as a child process. Add this to your server.cfg BEFORE every `start` line:');
      log.error('    add_unsafe_child_process_permission spacedb');
      log.error('(this MUST be set before resources start; it cannot be granted from fxmanifest.lua)');
      coreDeferred.reject(err);
      return;
    }
    if (shuttingDown) {
      coreDeferred.reject(err);
      return;
    }
    const delay = Math.min(5000, 200 * 2 ** respawnAttempt);
    respawnAttempt += 1;
    log.debug(`retrying core boot in ${delay}ms (attempt ${respawnAttempt})`);
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
      log.debug(`tcp transport connected ${transportEndpoint}`);
      resolve(conn);
    });

    conn.setNoDelay(true);
    conn.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      const recvNs = Number(process.hrtime.bigint());
      buffer = consumeFrames(buffer, (payload, line) => {
        if (!payload) {
          log.warn(`invalid tcp response: ${line}`);
          return;
        }
        // Unsolicited events from the server (cache invalidations,
        // realtime subscription updates) arrive without an id. Route on
        // the event field.
        if (payload.event === 'invalidate') {
          if (payload.key) mirror.invalidate(payload.table, payload.key);
          else mirror.invalidateTable(payload.table);
          return;
        }
        if (payload.event === 'subscription') {
          const eventObj = {
            id: payload.subId,
            type: payload.type,
            query: payload.query,
            rows: payload.rows,
            error: payload.error,
            createdAt: payload.createdAt,
          };
          const cb = subscriptionCallbacks.get(payload.subId);
          if (cb) {
            try { cb(eventObj); }
            catch (err) { log.warn(`subscription callback threw: ${err.message}`); }
          } else {
            let buf = subscriptionBuffer.get(payload.subId);
            if (!buf) { buf = []; subscriptionBuffer.set(payload.subId, buf); }
            buf.push(eventObj);
            if (buf.length > 64) buf.shift();
          }
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

// Batched read. One export call returns N rows. Amortizes the FiveM
// Lua↔JS export overhead across the whole batch; per-row cost drops to
// microseconds on a warm cache. Returns rows in input-key order, with
// `null` for keys that have no matching row.
async function getMany(table, keys, pkColumn = 'id') {
  if (!Array.isArray(keys) || keys.length === 0) return [];
  const out = new Array(keys.length);
  const misses = [];
  const missIndexes = [];
  for (let i = 0; i < keys.length; i += 1) {
    const skey = String(keys[i]);
    const cached = mirror.get(table, skey);
    if (cached !== undefined) {
      out[i] = cached;
      mirrorHits += 1;
    } else {
      misses.push(keys[i]);
      missIndexes.push(i);
    }
  }
  if (misses.length === 0) return out;

  const result = await transport('cacheGetMany', { table, keys: misses, pkColumn });
  const rows = (result && result.rows) || {};
  for (let i = 0; i < missIndexes.length; i += 1) {
    const skey = String(misses[i]);
    const row = rows[skey];
    if (row != null) {
      mirror.set(table, skey, row);
      out[missIndexes[i]] = row;
    } else {
      out[missIndexes[i]] = null;
    }
  }
  return out;
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
  if (result?.id) {
    subscriptions.add(result.id);
    if (typeof callback === 'function') {
      subscriptionCallbacks.set(result.id, callback);
      const buffered = subscriptionBuffer.get(result.id);
      if (buffered) {
        subscriptionBuffer.delete(result.id);
        for (const ev of buffered) {
          try { callback(ev); }
          catch (err) { log.warn(`subscription callback threw: ${err.message}`); }
        }
      }
    }
  }
  return result;
}

function unsubscribe(id) {
  subscriptions.delete(id);
  subscriptionCallbacks.delete(id);
  subscriptionBuffer.delete(id);
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
exports('getMany', getMany);
exports('setById', setById);
exports('invalidate', invalidate);
exports('invalidateTable', invalidateTable);
exports('cacheStats', cacheStats);

// `spacelog` console command: pulls /diagnostics, writes the bundle to
// `spacedb-diag-<timestamp>.json` next to the resource, prints the path.
// Users attach this file to a GitHub issue or DM. Server console only
// (restricted=true means a player needs the `command.spacelog` ace).
function fetchDiagnostics() {
  return new Promise((resolve, reject) => {
    const http = require('http');
    const url = new URL('/diagnostics', endpoint);
    const req = http.request(
      { hostname: url.hostname, port: url.port, path: url.pathname, method: 'GET', timeout: 5000 },
      (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (c) => { body += c; });
        res.on('end', () => {
          if (res.statusCode < 200 || res.statusCode >= 300) {
            reject(new Error(`HTTP ${res.statusCode}`));
            return;
          }
          resolve(body);
        });
      },
    );
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

RegisterCommand('spacelog', async (source) => {
  try {
    const body = await fetchDiagnostics();
    const ts = new Date().toISOString().replace(/[:.]/g, '-').replace('T', '_').replace('Z', '');
    const file = path.join(resourceRoot, `spacedb-diag-${ts}.json`);
    fs.writeFileSync(file, body);
    log.info(`diagnostics written: ${file}`);
    log.info('attach this file to a GitHub issue or DM (passwords are redacted)');
  } catch (err) {
    log.error(`spacelog failed: ${err.message}`);
  }
}, true);

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
