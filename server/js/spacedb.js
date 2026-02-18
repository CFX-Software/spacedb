const net = require('net');

const endpoint = GetConvar('spacedb_endpoint', 'http://127.0.0.1:37120');
const transportEndpoint = GetConvar('spacedb_transport', '127.0.0.1:37121');
const subscriptions = new Set();
let nextId = 0;
let socket = null;
let buffer = '';
let connecting = null;
const pending = new Map();

function parseAddress(value) {
  const [host, port] = value.split(':');
  return { host: host || '127.0.0.1', port: Number(port || 37121) };
}

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
      let index = buffer.indexOf('\n');
      while (index >= 0) {
        const line = buffer.slice(0, index);
        buffer = buffer.slice(index + 1);
        index = buffer.indexOf('\n');
        if (!line) continue;

        let payload;
        try {
          payload = JSON.parse(line);
        } catch (err) {
          console.log(`[spacedb] invalid tcp response: ${err.message}`);
          continue;
        }

        const request = pending.get(payload.id);
        if (!request) continue;
        pending.delete(payload.id);

        if (payload.ok) {
          request.resolve(payload.result);
        } else {
          request.reject(new Error(payload.error || 'spacedb transport error'));
        }
      }
    });

    conn.on('error', (err) => {
      if (connecting) {
        connecting = null;
        reject(err);
      }
    });

    conn.on('close', () => {
      if (socket === conn) socket = null;
      for (const request of pending.values()) {
        request.reject(new Error('spacedb transport closed'));
      }
      pending.clear();
    });
  });

  return connecting;
}

function request(method, path, body) {
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

async function transport(op, payload) {
  const conn = await connectTransport();
  const id = `${Date.now()}_${++nextId}`;
  const message = JSON.stringify({ ...payload, id, op });

  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    conn.write(`${message}\n`, 'utf8', (err) => {
      if (!err) return;
      pending.delete(id);
      reject(err);
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

function executeMany(sqlOrName, rows = []) {
  const steps = rows.map((params) => ({ query: sqlOrName, params, mode: 'execute' }));
  return transaction(steps);
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

on('onResourceStop', (stopped) => {
  if (stopped !== GetCurrentResourceName()) return;
  for (const id of subscriptions) {
    transport('unsubscribe', { subId: id }).catch(() => {});
  }
  if (socket && !socket.destroyed) socket.destroy();
});
