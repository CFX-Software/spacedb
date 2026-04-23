# spacedb

High-performance database resource for FiveM. Drop-in replacement for OxMySQL / mysql-async / ghmattimysql with sub-10ms realtime subscriptions and built-in caching.

A Go service runs alongside the resource over a local TCP transport. Lua callers see synchronous exports; the bridge handles supervision, reconnect, and backpressure.

## What you get over OxMySQL

| Capability | OxMySQL | spacedb |
|---|---|---|
| `query` / `execute` / `transaction` | yes | yes, +15-38% qps |
| Concurrent insert qps (50 workers) | 1321 | **3144 (+58%)** |
| In-process row cache (`getById`) | no | yes, ~50µs cache hit |
| Realtime subscriptions | no | yes, **~7ms latency** on writes via spacedb |
| Batched `getMany` (100 keys) | no | yes |
| Prepared statement cache | partial | yes, auto |
| `/metrics` endpoint with p50/p95/p99 | no | yes |
| Drop-in API shims for mysql-async, ghmattimysql, oxmysql | n/a | yes |

Benchmarks: 1000 iterations per phase, MariaDB localhost, 50-worker concurrency, real `oxmysql` resource as baseline. Numbers in [Performance](#performance).

## Quickstart

```cfg
ensure spacedb
```

```lua
local rows  = exports.spacedb:query('SELECT * FROM users WHERE level > ?', { 10 })
local user  = exports.spacedb:single('SELECT * FROM users WHERE id = ?', { 1 })
local r     = exports.spacedb:execute('UPDATE users SET name = ? WHERE id = ?', { 'Jane', 1 })
                                                                          -- r.rowsAffected, r.lastInsertId
local cached = exports.spacedb:getById('users', 1)                        -- cached read
local many   = exports.spacedb:getMany('users', { 1, 2, 3, 5, 8 })        -- batched
```

```lua
-- Realtime: callback fires every time the row set changes.
exports.spacedb:subscribe('SELECT * FROM players WHERE online = 1', {}, function(event)
    -- event.type = 'changed' | 'error'
    -- event.rows = fresh row set
end)
```

## Install

1. Drop the binary into `bin/` from [Releases](https://github.com/VexoaXYZ/SPACEDB/releases):

| Platform | File |
|---|---|
| Windows x64 | `spacedb-core-windows-amd64.exe` → `bin/spacedb-core.exe` |
| Linux x64 | `spacedb-core-linux-amd64` → `bin/spacedb-core` |
| Linux arm64 | `spacedb-core-linux-arm64` → `bin/spacedb-core` |

2. Copy `config.example.json` to `config.json`, set your DSN.

3. `ensure spacedb` in `server.cfg`. Linux users add `setr spacedb_core_platform linux`.

The JS bridge spawns and supervises the Go core. On crash it rejects in-flight requests, holds new ones, and retries with exponential backoff (200ms → 5s).

## Build from source

```powershell
docker compose up -d
cd core
go build -o ../bin/spacedb-core.exe ./cmd/spacedb-core
bin\spacedb-core.exe -config config.json -check-config
```

## Migration from other resources

Three compat shim resources ship alongside spacedb. Each provides the target API surface verbatim — to use as a literal drop-in, **rename the shim folder to the target name**:

| Target resource | Shim folder | Rename to |
|---|---|---|
| `oxmysql` | `spacedb-oxmysql` | `oxmysql` |
| `mysql-async` | `spacedb-mysql-async` | `mysql-async` |
| `ghmattimysql` | `spacedb-ghmattimysql` | `ghmattimysql` |

Then remove the original resource, `ensure spacedb` and `ensure <renamed-shim>` in server.cfg. Existing `exports['oxmysql']:query(...)` etc resolve to the shim with no caller changes.

Each shim handles named-param translation (`@name` → `?` + reordering), callback-or-return signatures, and the original return shapes (`lastInsertId`, `rowsAffected`, scalar extraction).

## API

### Reads

```lua
local rows  = exports.spacedb:query(sql, params)            -- []row
local row   = exports.spacedb:single(sql, params)           -- row | nil
local row   = exports.spacedb:getById(table, key, pkCol)    -- cached, pkCol defaults to 'id'
local many  = exports.spacedb:getMany(table, {keys}, pkCol) -- batched, in input order, nil for misses
```

### Writes

```lua
local r = exports.spacedb:execute(sql, params)              -- r.rowsAffected, r.lastInsertId
local r = exports.spacedb:executeMany(sql, {rowParams...})  -- single multi-row INSERT
local r = exports.spacedb:transaction({
    { mode = 'execute', query = '...', params = {...} },
    { mode = 'query',   query = '...', params = {...} },
})
```

`execute`, `executeMany`, `transaction` auto-invalidate the cache. UPDATE/DELETE by single PK drops just that entry; multi-row writes drop the whole table.

### Realtime

```lua
local sub = exports.spacedb:subscribe(sql, params, function(event)
    -- event.id, event.type ('changed' | 'error'), event.rows, event.error, event.createdAt
end)
exports.spacedb:unsubscribe(sub.id)
```

Latency: **single-digit ms** when the change comes through spacedb's `execute`/`executeMany`/`transaction` (push over TCP via cache invalidation broadcaster). Falls back to polling at `realtime.pollIntervalMs` (default 250ms) for writes coming from outside spacedb (raw MySQL, other resources).

### Prepared statements

```lua
exports.spacedb:prepare('topPlayers', 'SELECT * FROM players ORDER BY score DESC LIMIT ?')
local rows = exports.spacedb:query('topPlayers', { 10 })
```

### Cache control

```lua
exports.spacedb:setById('users', 5, { id = 5, name = 'Jane', score = 200 })  -- manual upsert
exports.spacedb:invalidate('users', 5)                                       -- drop one
exports.spacedb:invalidateTable('users')                                     -- drop whole table
local s = exports.spacedb:cacheStats()                                       -- s.server + s.mirror
```

### Metrics + introspection

```lua
local info = exports.spacedb:stats()  -- { db = pool stats, subscriptions = N, ... }
```

HTTP endpoints on `127.0.0.1:37120`:

- `GET /health` — `{ ok, driver }`
- `GET /metrics` — full snapshot: DB pool, cache hit/miss/evict, per-op count/avg/p50/p95/p99/max/errors, transport conn count

## Cache

Two-tier row cache keyed by `(table, primary_key)`:

```
Lua → JS mirror (~5-50µs hit) → Go cache (~50µs hit) → MySQL (1-3ms)
```

Tier 1 (JS, 10k entries default) skips the TCP round trip. Tier 2 (Go, 100k entries) catches mirror misses. Both stay coherent via TCP push: writes broadcast invalidation events on the same socket that carries responses; the JS mirror drops the entry on receipt. Writes also pre-invalidate the JS mirror by table before sending, so read-after-write cannot race the broadcast.

The cache parser handles backticked identifiers (`` UPDATE `users` SET ... ``), Postgres double-quoted identifiers, REPLACE INTO, ON DUPLICATE KEY UPDATE, ON CONFLICT, and TRUNCATE. SQL it can't pin to a specific row drops the whole table (correctness over efficiency). Plain INSERT is a no-op (new rows can't be cached yet).

Tune via convars: `spacedb_mirror_max_entries` (JS-side cap), or edit `config.json` for Go-side `maxOpenConns` / `maxIdleConns`.

## Profiling

```lua
local meta = exports.spacedb:executeProfiled(sql, params)
-- meta.result, meta.bridgeNs (JS hrtime), meta.profile {serverTotalNs, dispatchNs, dbDurNs}

local meta = exports.spacedb:queryProfiled(sql, params)
```

## Convars

| Convar | Default | Purpose |
|---|---|---|
| `spacedb_endpoint` | `http://127.0.0.1:37120` | HTTP base (`/health`, `/metrics`, legacy probes) |
| `spacedb_transport` | `127.0.0.1:37121` | TCP transport |
| `spacedb_manage_core` | `true` | Spawn+supervise core. `false` if running externally |
| `spacedb_core_path` | `<resource>/bin/spacedb-core.exe` | Override binary path |
| `spacedb_core_platform` | `windows` | Set `linux` on Linux servers |
| `spacedb_core_mode` | `restart` | `restart` kills + respawns each boot; `reuse` keeps an existing healthy core |
| `spacedb_request_timeout_ms` | `30000` | Per-request TCP timeout |
| `spacedb_mirror_max_entries` | `10000` | JS mirror cache cap |

## Performance

1000 iterations per phase, MariaDB localhost, real `oxmysql` resource baseline, pool size 128:

| Phase | spacedb qps | oxmysql qps | delta |
|---|---|---|---|
| query sequential | 762 | 524 | **+31%** |
| query concurrent (50 workers) | 3731 | 2198 | **+41%** |
| insert sequential | 222 | 201 | +9% |
| insert concurrent (50 workers) | 3145 | 1321 | **+58%** |
| insert bulk multi-row (1000 rows in one statement) | 35714 | 31250 | **+12%** |
| get-by-id (cache hit) vs ox single-by-id | 1742 | 551 | **+71%** |

### Per-call latency floor — sequential insert

```
lua-total      4.40 ms   end-to-end Lua wall clock
bridge-rtt     4.10 ms   JS write → JS recv
server-total   3.79 ms   Go handler entry → return
db-exec        3.79 ms   driver Exec()

derived:
  Lua → JS bridge       0.33 ms  (7.5%)  — FiveM scheduler + interop
  JS bridge → Go core   0.31 ms  (7.0%)  — TCP + JSON
  Go core non-DB        ~0       (0.1%)  — handler overhead
  MySQL driver round trip 3.79 ms (86%)  — the database floor
```

86% of single-insert latency is the MySQL driver itself. spacedb adds ~0.6ms of bridge overhead. For workloads that exceed 220 sequential single-insert qps, use `executeMany` (35k qps) or `transaction`.

### Concurrent insert tail (p99)

```
25 workers × 20 inserts, per-stage p99 ms:

server-total   ~14.6     spacedb server time
db-exec        ~14.6     driver Exec time

OxMySQL concurrent insert p99: ~48 ms
```

spacedb server-total tracks db-exec to within 0.1ms at p99 — zero measurable queueing under 25-way concurrency. The structural win comes from a single-socket id-multiplexed transport vs OxMySQL's per-call Node-side allocation.

### Realtime subscription latency

Write via spacedb → callback fires: **7ms** measured end-to-end (UPDATE issue → Lua callback entry). Polling fallback (for external writes) at 250ms config default.

## Tests

- `examples/spacedb-test` — integration tests (health, query/single/execute, prepared, transaction, stats, subscription)
- `examples/spacedb-bench` — comparative benchmark vs `oxmysql`
- `spacedb-realtime-test` (resources/[db]/) — realtime push end-to-end with latency measurement
- `spacedb-compat-test` — verifies the 3 compat shims (named-param translation, return shapes, routing)
- `core/internal/cache` Go unit tests — SQL parser coverage

```powershell
cd core
go test ./...
```

## Architecture

```
Lua exports
    ↓
spacedb-oxmysql / spacedb-mysql-async / spacedb-ghmattimysql shims (optional)
    ↓
spacedb (Lua bridge → Node.js JS bridge)
    ↓ TCP newline-delimited JSON over 127.0.0.1:37121
spacedb-core (Go)
    ↓ database/sql with prepared-statement cache
MySQL / Postgres
```

The Go core is the source of truth for cache state, prepared statements, realtime subscriptions, and metrics. The Node.js bridge is supervision + JS mirror cache + FiveM interop only. Lua sees synchronous exports.

## Notes

`config.json` and `bin/` are gitignored. DB passwords, generated binaries, and machine setup don't get committed.
