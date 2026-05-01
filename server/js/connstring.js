// Parses oxmysql / mysql-async style connection strings into a Go
// go-sql-driver/mysql DSN. Accepts two formats:
//
//   URL:        mysql://user:pass@host:port/dbname?charset=utf8mb4
//   Semicolon:  server=host;port=3306;userid=user;password=pass;database=db
//
// Returns { driver, dsn } or null if input is empty / unparseable.

function parseConnString(raw) {
  if (typeof raw !== 'string' || raw.trim() === '') return null;
  const trimmed = raw.trim();
  if (/^mysql:\/\//i.test(trimmed) || /^mariadb:\/\//i.test(trimmed)) {
    return parseUrl(trimmed);
  }
  if (/^postgres(ql)?:\/\//i.test(trimmed)) {
    return { driver: 'postgres', dsn: trimmed };
  }
  if (trimmed.includes('=') && trimmed.includes(';')) {
    return parseSemicolon(trimmed);
  }
  return null;
}

function parseUrl(input) {
  let u;
  try {
    u = new URL(input);
  } catch (_e) {
    return null;
  }
  const host = u.hostname || '127.0.0.1';
  const port = u.port || '3306';
  const user = decodeURIComponent(u.username || '');
  const pass = decodeURIComponent(u.password || '');
  const db = decodeURIComponent((u.pathname || '/').replace(/^\//, ''));
  const params = new URLSearchParams(u.search);
  // Always force parseTime so DATETIME columns come back as time.Time and
  // the JS bridge sees ISO strings rather than raw byte slices.
  if (!params.has('parseTime')) params.set('parseTime', 'true');
  const userInfo = user ? (pass ? `${user}:${pass}` : user) : '';
  const query = params.toString();
  const dsn = `${userInfo}${userInfo ? '@' : ''}tcp(${host}:${port})/${db}${query ? '?' + query : ''}`;
  return { driver: 'mysql', dsn };
}

function parseSemicolon(input) {
  const kv = {};
  for (const pair of input.split(';')) {
    const idx = pair.indexOf('=');
    if (idx < 0) continue;
    const k = pair.slice(0, idx).trim().toLowerCase();
    const v = pair.slice(idx + 1).trim();
    if (k) kv[k] = v;
  }
  const host = kv.server || kv.host || '127.0.0.1';
  const port = kv.port || '3306';
  const user = kv.userid || kv.user || kv.username || '';
  const pass = kv.password || kv.pwd || '';
  const db = kv.database || kv.db || '';
  if (!db) return null;
  const params = new URLSearchParams();
  params.set('parseTime', 'true');
  if (kv.charset) params.set('charset', kv.charset);
  const userInfo = user ? (pass ? `${user}:${pass}` : user) : '';
  const dsn = `${userInfo}${userInfo ? '@' : ''}tcp(${host}:${port})/${db}?${params.toString()}`;
  return { driver: 'mysql', dsn };
}

module.exports = { parseConnString };
