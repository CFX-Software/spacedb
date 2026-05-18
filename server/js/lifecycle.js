// spacedb core lifecycle: spawn the Go binary detached, poll /health until
// ready. Node's child_process.spawn bypasses cmd.exe so it dodges the
// FxServer-host SmartScreen / EACCES path that breaks Lua os.execute.

const { spawn } = require('child_process');
const http = require('http');
const path = require('path');
const fs = require('fs');

function probeHealth(endpoint, timeoutMs) {
  return new Promise((resolve) => {
    const url = new URL('/health', endpoint);
    const req = http.request(
      { hostname: url.hostname, port: url.port, path: url.pathname, method: 'GET', timeout: timeoutMs },
      (res) => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          res.resume();
          resolve(null);
          return;
        }
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => { body += chunk; });
        res.on('end', () => {
          try {
            const decoded = JSON.parse(body);
            if (decoded && !decoded.error) {
              resolve(decoded);
              return;
            }
          } catch (_err) { /* fall through */ }
          resolve(null);
        });
      }
    );
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.end();
  });
}

function waitForReady(endpoint, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  return new Promise((resolve, reject) => {
    const tick = async () => {
      const health = await probeHealth(endpoint, 1000);
      if (health) {
        resolve(health);
        return;
      }
      if (Date.now() >= deadline) {
        reject(new Error(`spacedb core did not become ready within ${deadlineMs}ms`));
        return;
      }
      setTimeout(tick, 200);
    };
    tick();
  });
}

async function killByPort(port) {
  // Best-effort kill of any process listening on the given port. Uses
  // netstat + taskkill on Windows, fuser/lsof on Linux. Returns true if at
  // least one process was killed.
  if (process.platform === 'win32') {
    return new Promise((resolve) => {
      const ns = spawn('cmd.exe', ['/c', `netstat -ano -p tcp | findstr :${port}`], { windowsHide: true });
      let out = '';
      ns.stdout.on('data', (c) => { out += c.toString(); });
      ns.on('close', () => {
        const pids = new Set();
        for (const line of out.split(/\r?\n/)) {
          const m = line.match(/LISTENING\s+(\d+)/);
          if (m) pids.add(m[1]);
        }
        if (pids.size === 0) { resolve(false); return; }
        let pending = pids.size;
        let killed = false;
        for (const pid of pids) {
          const tk = spawn('taskkill.exe', ['/F', '/T', '/PID', pid], { windowsHide: true });
          tk.on('close', (code) => {
            if (code === 0) killed = true;
            if (--pending === 0) resolve(killed);
          });
        }
      });
      ns.on('error', () => resolve(false));
    });
  }
  return new Promise((resolve) => {
    // Discover PIDs first so we can return false when nothing was listening
    // (rather than always returning true because xargs -r exits 0 on empty
    // input). Fall back to fuser when lsof isn't available.
    const script =
      `pids=$(lsof -ti tcp:${port} 2>/dev/null); ` +
      `if [ -z "$pids" ]; then pids=$(fuser ${port}/tcp 2>/dev/null); fi; ` +
      `if [ -z "$pids" ]; then exit 1; fi; ` +
      `echo $pids | xargs -r kill -9 2>/dev/null`;
    const tk = spawn('sh', ['-c', script]);
    tk.on('close', (code) => resolve(code === 0));
    tk.on('error', () => resolve(false));
  });
}

function spawnCore({ binary, config, logPath }) {
  // Detached + stdio:ignore + unref makes the child survive the parent and
  // not block the FxServer event loop. windowsHide suppresses console flash.
  const stdio = ['ignore', 'ignore', 'ignore'];
  if (logPath) {
    try {
      const dir = path.dirname(logPath);
      // Skip mkdir when the directory already exists. FiveM's Node permission
      // layer (FilesystemPermissions.cpp) rejects writes whose path resolves
      // to the resource-root directory with an empty remainder, so an
      // unconditional mkdir on `<resource>/` would emit a confusing
      // "write not allowed" trace even though the dir is right there.
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      const fd = fs.openSync(logPath, 'a');
      stdio[1] = fd;
      stdio[2] = fd;
    } catch (_err) { /* fall back to ignore */ }
  }
  const child = spawn(binary, ['-config', config], {
    detached: true,
    stdio,
    windowsHide: true,
  });
  child.unref();
  return child;
}

async function ensureCore({ endpoint, transportPort, binary, config, logPath, mode = 'restart' }) {
  // mode: 'restart' kills any existing core then spawns fresh. 'reuse' keeps
  // an existing core if one is already responding on /health.
  const existing = await probeHealth(endpoint, 1000);

  if (existing && mode === 'reuse') {
    return { reused: true, pid: null, driver: existing.driver };
  }

  if (existing) {
    const killed = await killByPort(new URL(endpoint).port);
    if (killed) {
      // Wait for port to actually release.
      for (let i = 0; i < 20; i += 1) {
        if (!(await probeHealth(endpoint, 500))) break;
        await new Promise((r) => setTimeout(r, 100));
      }
    } else if (await probeHealth(endpoint, 500)) {
      // Couldn't kill it (probably owned by a different user/session).
      // Fall back to reusing it; better than throwing.
      return { reused: true, pid: null, driver: existing.driver, killFailed: true };
    }
    // Also try to release the transport port if different from health port.
    if (transportPort && transportPort !== new URL(endpoint).port) {
      await killByPort(transportPort);
    }
  }

  const child = spawnCore({ binary, config, logPath });
  const health = await waitForReady(endpoint, 30000);
  return { reused: false, pid: child.pid, driver: health.driver, child };
}

module.exports = { ensureCore, probeHealth, killByPort, spawnCore };
