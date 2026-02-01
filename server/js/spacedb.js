const endpoint = GetConvar('spacedb_endpoint', 'http://127.0.0.1:37120');

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

function query(sqlOrName, params = []) {
  return request('POST', '/v1/query', { query: sqlOrName, params });
}

function single(sqlOrName, params = []) {
  return request('POST', '/v1/single', { query: sqlOrName, params });
}

function execute(sqlOrName, params = []) {
  return request('POST', '/v1/execute', { query: sqlOrName, params });
}

function prepare(name, sql, options = {}) {
  return request('POST', '/v1/prepare', { name, sql, options });
}

function transaction(steps = []) {
  return request('POST', '/v1/transaction', { steps });
}

async function subscribe(sqlOrName, params = [], callback) {
  const result = await request('POST', '/v1/subscribe', { query: sqlOrName, params });
  if (callback && result?.id) {
    const id = result.id;
    const poll = async () => {
      try {
        const payload = await request('GET', `/v1/events?id=${encodeURIComponent(id)}`);
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
  return request('POST', '/v1/unsubscribe', { id });
}

function health() {
  return request('GET', '/health');
}

function stats() {
  return request('GET', '/v1/stats');
}

exports('query', query);
exports('single', single);
exports('execute', execute);
exports('prepare', prepare);
exports('transaction', transaction);
exports('subscribe', subscribe);
exports('unsubscribe', unsubscribe);
exports('health', health);
exports('stats', stats);
