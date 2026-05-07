# spacedb

A faster database resource for FiveM. It does what oxmysql does (queries, inserts, transactions, prepared statements) but adds an in-process row cache and live query subscriptions that fire when data changes. There are also compatibility resources so you can swap it in without rewriting your other scripts.

Under the hood there's a small Go program that talks to MySQL or Postgres. Your Lua scripts call exports the way they would with any other database resource, and the Go program does the heavy lifting. The Go program is started, watched, and restarted automatically by the spacedb resource when your server boots, so you don't have to set it up as a separate service.

## Why bother

If you have a server running oxmysql today and you swap to spacedb, here's what changes for you:

- Queries and writes run faster. A lot faster in some cases (see the benchmark table at the bottom).
- You can subscribe to a query and get a callback the moment the result changes. No polling.
- You get a real cache. `getById` is around 50 microseconds when warm, instead of a full round trip to MySQL every time.
- You can keep your existing code. There are wrapper resources that pretend to be oxmysql, mysql-async, or ghmattimysql, so the rest of your scripts don't need to know.

## Quick start

Drop the resource folder into `resources/`. If you already run oxmysql or mysql-async, you almost certainly have this line in `server.cfg`:

```cfg
set mysql_connection_string "mysql://user:pass@127.0.0.1:3306/yourdb?charset=utf8mb4"
```

spacedb reads the same convar. No second config file needed. Just add:

```cfg
ensure spacedb
```

On first boot the resource writes a `config.json` next to the binary using your existing connection string. If you want to tune ports, pool size, or use Postgres, edit `config.json` after that first boot. The semicolon style oxmysql accepts (`server=...;userid=...;password=...;database=...`) also works.

Then in any server script:

```lua
local rows = exports.spacedb:query('SELECT * FROM users WHERE level > ?', { 10 })

local user = exports.spacedb:single('SELECT * FROM users WHERE id = ?', { 1 })

local result = exports.spacedb:execute('UPDATE users SET name = ? WHERE id = ?', { 'Jane', 1 })
-- result.rowsAffected, result.lastInsertId

local cached = exports.spacedb:getById('users', 1)
local many   = exports.spacedb:getMany('users', { 1, 2, 3, 5, 8 })
```

Live subscriptions:

```lua
exports.spacedb:subscribe('SELECT * FROM players WHERE online = 1', {}, function(event)
    -- event.type is 'changed' or 'error'
    -- event.rows is the fresh row set
    print('online players changed, now ' .. #event.rows)
end)
```

## Installing the binary

The Go core ships as a single executable. Grab the one for your server from the GitHub releases page and put it in the `bin` folder.

| Platform | File | Goes to |
|---|---|---|
| Windows x64 | `spacedb-core-windows-amd64.exe` | `bin\spacedb-core.exe` |
| Linux x64 | `spacedb-core-linux-amd64` | `bin/spacedb-core` |
| Linux arm64 | `spacedb-core-linux-arm64` | `bin/spacedb-core` |

On Linux servers, also add `setr spacedb_core_platform linux` to your `server.cfg` so the bridge looks for the right filename.

If the core crashes for any reason, the resource notices, rejects whatever requests were in flight, and respawns it. Backoff starts at 200 ms and caps at 5 seconds.

## Switching from oxmysql, mysql-async, or ghmattimysql

This is the part most people care about. You probably have a server full of scripts that depend on oxmysql or one of the older mysql resources. You don't want to rewrite all of them.

You don't have to. spacedb ships with three small wrapper resources, one per legacy resource. Each wrapper exposes the exact same exports the original does, so existing scripts don't notice the swap.

The wrappers live in their own folders:

| Old resource | Wrapper folder | What to do |
|---|---|---|
| `oxmysql` | `spacedb-oxmysql` | Rename the folder to `oxmysql` and replace the real oxmysql. |
| `mysql-async` | `spacedb-mysql-async` | Rename to `mysql-async` and remove the original. |
| `ghmattimysql` | `spacedb-ghmattimysql` | Rename to `ghmattimysql` and remove the original. |

The wrappers handle the awkward parts. Named parameters like `@playerId` get translated to positional `?`. Callback style and synchronous return style both keep working. Insert calls return `lastInsertId`. Update and execute calls return the affected row count. Transactions accept the formats the originals accepted.

After renaming and adding `ensure <name>` to your `server.cfg`, your existing scripts run as normal but the queries actually hit spacedb.

## API in plain English

### Reads

```lua
local rows = exports.spacedb:query(sql, params)
-- Returns an array of rows. Each row is a Lua table keyed by column name.

local row = exports.spacedb:single(sql, params)
-- Returns one row (the first match) or nil.

local row = exports.spacedb:getById(table, key, pkColumn)
-- Cached lookup. First call hits MySQL, second call comes from memory.
-- pkColumn is optional, defaults to 'id'.

local rows = exports.spacedb:getMany(table, { 1, 2, 3 }, pkColumn)
-- Batched version of getById. Cache hits are free, misses become one
-- SELECT ... WHERE id IN (...). Returns rows in the same order you asked,
-- with nil where the row didn't exist.
```

### Writes

```lua
local result = exports.spacedb:execute(sql, params)
-- result.rowsAffected, result.lastInsertId

local result = exports.spacedb:executeMany(sql, {
    { 'Jane', 50 },
    { 'Alex', 75 },
    { 'Sam',  90 },
})
-- One SQL statement, many rows. Use this for bulk inserts.

local result = exports.spacedb:transaction({
    { mode = 'execute', query = 'INSERT INTO users (name) VALUES (?)', params = { 'Jane' } },
    { mode = 'query',   query = 'SELECT * FROM users WHERE name = ?',  params = { 'Jane' } },
})
-- All steps run in one transaction. If any step errors, the whole thing rolls back.
```

After any write, the cache figures out which rows were touched and drops them. You don't need to call `invalidate` yourself in the normal case.

### Subscriptions

```lua
local sub = exports.spacedb:subscribe(sql, params, function(event)
    -- event.type    'changed' or 'error'
    -- event.rows    fresh result set
    -- event.error   set when type is 'error'
end)

exports.spacedb:unsubscribe(sub.id)
```

The callback fires once on subscribe (so you get the initial state), then again every time the result set actually changes. If your other code writes to that table through spacedb, the callback fires in single-digit milliseconds. If something else writes to the table directly (a different resource, an external tool), the callback fires within the poll interval (250 ms by default).

### Cache control

You almost never need these because writes auto-invalidate, but they're here:

```lua
exports.spacedb:setById('users', 5, { id = 5, name = 'Jane', score = 200 })
exports.spacedb:invalidate('users', 5)
exports.spacedb:invalidateTable('users')

local stats = exports.spacedb:cacheStats()
-- stats.server.hits, .misses, .entries, .evictions  (Go side)
-- stats.mirror.entries, .hits                       (JS side)
```

### Stats

```lua
local info = exports.spacedb:stats()
-- info.db (pool stats), info.subscriptions (count)
```

You can also hit `http://127.0.0.1:37120/metrics` from a browser or curl. It returns a full snapshot: how many of each operation ran, average and p99 timings, cache hit rate, active TCP connections, MySQL pool usage.

## How the cache works

Reads through `getById` and `getMany` are cached in two places: in the Node bridge (about 5 to 50 microseconds per hit) and in the Go core (about 50 microseconds per hit). On a cold cache, both miss and the request goes to MySQL. On a warm cache, you skip everything.

When you write to a row through spacedb, both layers get invalidated. The Go core figures out which rows the write touched and broadcasts an invalidation message back to the bridge over the same TCP connection, so the in-memory mirror drops the stale entry.

The cache parser understands `UPDATE`, `DELETE`, `INSERT`, `REPLACE INTO`, `ON DUPLICATE KEY UPDATE`, `ON CONFLICT`, and `TRUNCATE`. It also handles backtick-quoted identifiers from MySQL and double-quoted identifiers from Postgres. If it sees something it can't pin to a specific row (a join, a range update, an unusual shape), it drops the whole table from the cache. That's the safe choice. Plain `INSERT` is a no-op because the row obviously can't be cached yet.

## Convars

| Convar | Default | What it does |
|---|---|---|
| `spacedb_endpoint` | `http://127.0.0.1:37120` | HTTP base for `/health`, `/metrics`, and legacy endpoints. |
| `spacedb_transport` | `127.0.0.1:37121` | TCP socket the bridge uses to talk to the Go core. |
| `spacedb_manage_core` | `true` | If true the bridge spawns and supervises the Go core. Set false if you run it as a system service. |
| `spacedb_core_path` | resource `bin\spacedb-core.exe` | Override the binary path. |
| `spacedb_core_platform` | `windows` | Set to `linux` on Linux servers. |
| `spacedb_core_mode` | `restart` | `restart` kills any existing core and starts fresh on boot. `reuse` keeps a healthy one running. |
| `spacedb_request_timeout_ms` | `30000` | Per-request TCP timeout. |
| `spacedb_mirror_max_entries` | `10000` | Max rows in the bridge cache (the fast tier). |
| `spacedb_log_level` | `info` | One of `error`, `warn`, `info`, `debug`. Default emits one ready line plus warnings and errors. Set `debug` when troubleshooting. |
| `mysql_connection_string` | (none) | Same convar oxmysql uses. When set, spacedb generates or syncs `config.json` automatically. |

## Performance

Benchmark setup: 1000 iterations per phase, MariaDB on localhost, 50 concurrent workers where applicable, real `oxmysql` resource as the comparison. Pool size set to 128. The "how much faster" column is the ratio of throughput, so 2.38x means spacedb did 2.38 times as many operations per second.

| Phase | spacedb qps | oxmysql qps | delta | how much faster |
|---|---|---|---|---|
| query sequential | 762 | 524 | +31% | 1.45x |
| query concurrent (50 workers) | 3731 | 2198 | +41% | 1.70x |
| insert sequential | 222 | 201 | +9% | 1.10x |
| insert concurrent (50 workers) | 3145 | 1321 | +58% | 2.38x |
| insert bulk multi-row (1000 rows in one statement) | 35714 | 31250 | +12% | 1.14x |
| getById cache hit vs oxmysql single-by-id | 1742 | 551 | +71% | 3.16x |

### Where time goes on a single insert

```
lua-total      4.40 ms   Lua call to Lua return
bridge-rtt     4.10 ms   bridge sends, bridge receives
server-total   3.79 ms   Go handler entry to return
db-exec        3.79 ms   actual MySQL Exec
```

86% of the time is MySQL itself. That's the floor and there's nothing spacedb can do about it. The other 0.6 ms is the Lua-to-bridge hop and the bridge-to-core hop together. For workloads that need more than 220 inserts per second, use `executeMany` (which got 35,714 qps in the bench) or batch them in a transaction.

### Concurrent insert tail latency

```
25 workers doing 20 inserts each, p99 in milliseconds:

spacedb server-total   ~14.6
spacedb db-exec        ~14.6

oxmysql concurrent insert p99: ~48
```

spacedb server time and database time are within a hair of each other at p99. There's effectively zero queueing inside the Go core under that concurrency level.

### Live subscription latency

When a write goes through spacedb, the subscriber callback fires in about 7 milliseconds end to end. When a write hits MySQL through some other path, the subscriber catches it on the next poll (default 250 ms).

## Build it yourself

```powershell
docker compose up -d
cd core
go build -o ../bin/spacedb-core.exe ./cmd/spacedb-core
bin\spacedb-core.exe -config config.json -check-config
```

There are unit tests in the Go code:

```powershell
cd core
go test ./...
```

There are also integration test resources in `examples`. `spacedb-test` runs through queries, writes, transactions, subscriptions. `spacedb-bench` is the benchmark resource that compares spacedb against the real oxmysql.

## What lives where

```
Lua exports
   |
spacedb-oxmysql, spacedb-mysql-async, spacedb-ghmattimysql wrappers (optional)
   |
spacedb (Lua bridge plus Node.js JS bridge)
   |
   v  newline-delimited JSON over TCP on 127.0.0.1:37121
   |
spacedb-core (Go program)
   |
   v
MySQL or Postgres
```

The Go core is where caching, prepared statements, subscriptions, and metrics actually live. The Node bridge is mostly supervision, the small fast-tier cache, and the FiveM interop. Lua code just sees normal exports.

## Notes

`config.json` and `bin` are gitignored. Database passwords, generated binaries, and machine-specific stuff stays out of source control.

## Credits

Big shout out to Claude Code and Codex for finding and fixing some catastrophic bugs in this codebase along the way. A few of them would have shipped to production without that second pair of eyes, including a write race in the realtime push path, a stale binary path that masked an unrelated fix for a full session, and a parser case that would have over-invalidated cache on every backticked identifier. Pair-programming with an LLM works when you check the work, and this project is better for it.
