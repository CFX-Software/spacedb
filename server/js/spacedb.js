const net = require('net');
const path = require('path');
const { parseAddress, consumeFrames, createPendingMap } = require('./server/js/protocol');
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
let nextId = 0;
let socket = null;
let buffer = '';
let connecting = null;
let coreReady = null;

function bootCore() {
  if (!manageCore) {
    coreReady = Promise.resolve({ skipped: true });
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

  coreReady = ensureCore({
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
    return result;
  }).catch((err) => {
    console.log(`[spacedb] FATAL core failed to start: ${err.message}`);
    throw err;
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
        // Stamp recv hrtime before complete so wantsMeta callers see it.
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
  if (coreReady) await coreReady;
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

function query(sqlOrName, params = []) {
  return transport('query', { query: sqlOrName, params });
}

function single(sqlOrName, params = []) {
  return transport('single', { query: sqlOrName, params });
}

function execute(sqlOrName, params = []) {
  return transport('execute', { query: sqlOrName, params });
}

function executeProfiled(sqlOrName, params = []) {
  return transport('execute', { query: sqlOrName, params }, { profile: true });
}

function queryProfiled(sqlOrName, params = []) {
  return transport('query', { query: sqlOrName, params }, { profile: true });
}

function executeMany(sqlOrName, rows = []) {
  return transport('executeMany', { query: sqlOrName, rows });
}

function prepare(name, sql, options = {}) {
  return transport('prepare', { name, sql, options });
}

function transaction(steps = []) {
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

on('onResourceStop', (stopped) => {
  if (stopped !== GetCurrentResourceName()) return;
  for (const id of subscriptions) {
    transport('unsubscribe', { subId: id }).catch(() => {});
  }
  if (socket && !socket.destroyed) socket.destroy();
});
