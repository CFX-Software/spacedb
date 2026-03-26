# spacedb

spacedb is a fast database resource for FiveM servers.

It runs a small Go service beside the FiveM resource and gives server scripts a simple export API for queries, writes, transactions, metrics, and live query subscriptions.

The runtime path uses a persistent local TCP transport for query calls. HTTP stays available for health checks, stats, subscriptions, and debugging.

The first target is a clean native API. Compatibility layers for older resources such as OxMySQL and mysql async belong in the next pass once the core behavior is stable.

## Install (prebuilt)

1. Grab the binary for your platform from [Releases](https://github.com/VexoaXYZ/SPACEDB/releases) and drop it into `bin/`:

| Platform | File |
|---|---|
| Windows x64 | `spacedb-core-windows-amd64.exe` → `bin/spacedb-core.exe` |
| Linux x64 | `spacedb-core-linux-amd64` → `bin/spacedb-core` |
| Linux arm64 | `spacedb-core-linux-arm64` → `bin/spacedb-core` |

Each artifact ships with a `.sha256` next to it.

2. Copy `config.example.json` to `config.json`, set your database DSN.

3. Add the resource to `server.cfg`:

```cfg
ensure spacedb
```

The JS bridge spawns the core automatically on resource start. Linux users set `setr spacedb_core_platform linux` in `server.cfg`.

## Build from source

1. Start the dev databases.

```powershell
docker compose up -d
```

2. Build the core service.

```powershell
cd core
go build -o ../bin/spacedb-core.exe ./cmd/spacedb-core
```

3. Validate the config.

```powershell
bin\spacedb-core.exe -config config.json -check-config
```

4. `ensure spacedb` in `server.cfg`.

## Databases

The local `config.json` uses MariaDB on port `53306`.

Postgres also starts on port `55432`. To test it, set the driver to `postgres` and use this DSN.

```text
postgres://spacedb:spacedb@127.0.0.1:55432/spacedb?sslmode=disable
```

## Exports

```lua
local rows = exports.spacedb:query('SELECT 1 AS ok', {})
print(json.encode(rows))
```

```lua
local user = exports.spacedb:single('SELECT * FROM users WHERE id = ?', { 1 })
```

```lua
local result = exports.spacedb:execute('UPDATE users SET name = ? WHERE id = ?', { 'Jane', 1 })
```

```lua
local batch = exports.spacedb:executeMany('INSERT INTO users (name) VALUES (?)', {
    { 'Jane' },
    { 'Alex' },
    { 'Sam' }
})
```

```lua
local tx = exports.spacedb:transaction({
    { mode = 'execute', query = 'INSERT INTO users (name) VALUES (?)', params = { 'Jane' } },
    { mode = 'query', query = 'SELECT * FROM users', params = {} }
})
```

## Convars

| Convar | Default | Purpose |
|---|---|---|
| `spacedb_endpoint` | `http://127.0.0.1:37120` | HTTP base for `/health` and legacy probes |
| `spacedb_transport` | `127.0.0.1:37121` | TCP transport for query/execute/transaction |
| `spacedb_manage_core` | `true` | When true, the JS bridge spawns and supervises `spacedb-core.exe`. Set to `false` if you run the core as a system service or external process |
| `spacedb_core_path` | `<resource>/bin/spacedb-core.exe` | Override binary path |
| `spacedb_core_platform` | `windows` | Set to `linux` on Linux servers |
| `spacedb_core_mode` | `restart` | `restart` kills any process owning the transport port and starts fresh on every resource boot. `reuse` keeps an existing running core if one already responds on `/health` |
| `spacedb_request_timeout_ms` | `30000` | Per-request TCP timeout. Pending promises reject with `spacedb timeout after Nms` if the core never replies |

The bridge supervises the spawned core. On unexpected exit it rejects in-flight requests, holds new ones until respawn finishes, and retries with backoff (200 ms → 400 → 800 → ... capped at 5 s). Crashes during heavy load surface as a single batch of rejected promises plus a `core exited unexpectedly` log line rather than silent timeouts.

## Profile exports

Two extra exports surface end-to-end timing per call:

```lua
local meta = exports.spacedb:executeProfiled('INSERT INTO t (a, b) VALUES (?, ?)', { 1, 2 })
-- meta.result   = whatever execute would return
-- meta.bridgeNs = JS hrtime delta between socket write and response receive
-- meta.profile  = {
--   serverTotalNs = Go handler entry → return
--   dispatchNs    = pre-dispatch → post-dispatch
--   dbDurNs       = driver Exec duration
-- }

local meta = exports.spacedb:queryProfiled('SELECT * FROM t', {})
```

Use these to confirm where time is going on your own workload before reaching for batching or `executeMany`.

## Notes

`config.json` and `bin` are ignored on purpose. Local database passwords, generated binaries, logs, and machine setup should not be committed.

## Compatibility

`compat/spacedb-oxmysql` contains an OxMySQL style adapter resource. It forwards common exports to `spacedb` without taking the real `oxmysql` resource name:

```lua
exports['spacedb-oxmysql']:query('SELECT 1 AS ok', {}, function(rows)
    print(json.encode(rows))
end)
```

To test it locally, copy `compat/spacedb-oxmysql` into your resources folder, then ensure it after `spacedb`.

```cfg
ensure spacedb
ensure spacedb-oxmysql
```

The adapter covers every export documented at https://overextended.dev/oxmysql/: `query`, `single`, `scalar`, `execute`, `insert`, `update`, `prepare`, `transaction`, `rawExecute`, plus their `_async` aliases. Drop-in for resources that import `oxmysql` exports — change the export resource name from `oxmysql` to `spacedb-oxmysql`.

## Performance

Bench harness in `examples/spacedb-bench` against the real `oxmysql` resource, MariaDB on localhost, 1000 iterations per phase. Negative delta means OxMySQL won that phase.

| Phase | spacedb qps | oxmysql qps | delta |
|---|---|---|---|
| query sequential | 710 | 509 | spacedb +28% |
| query concurrent (50 workers) | 3134 | 1956 | spacedb +37% |
| insert sequential | 219 | 199 | spacedb +9% |
| insert concurrent (50 workers) | 1618 | 1239 | spacedb +23% |
| insert bulk multi-row (1000 rows in one statement) | 30303 | 33333 | within noise |

### Where the time goes — single sequential insert

Profile path: every request carries `profile: true`, the Go core stamps `ServerTotalNs` / `DispatchNs` / `DbDurNs`, the JS bridge stamps hrtime around the socket write+receive, and the Lua bench harness times the export call. Run `spacedb_bench_iterations 500` and watch the `spacedb insert sequential profile` phase.

```
per-call avg over 500 inserts (ms):

lua-total      4.40    end-to-end Lua wall clock
bridge-rtt     4.10    JS write → JS recv
server-total   3.79    Go handler entry → return
db-exec        3.79    driver Exec()

derived:
  Lua → JS bridge       0.33    (7.5%)  — FiveM scheduler + interop
  JS bridge → Go core   0.31    (7.0%)  — TCP + JSON
  Go core non-DB        ~0      (0.1%)  — handler overhead
  MySQL driver round trip 3.79  (86%)   — the floor
```

86% of single-insert latency is the MySQL driver itself: network round trip, server-side statement prep, disk fsync. That number is set by the database, not by spacedb. spacedb adds ~0.6 ms of bridge overhead per call — Lua interop and TCP/JSON each cost roughly the same.

For workloads that need more than 220 sequential single-insert qps, use `executeMany` (22k–30k qps in the bench) or `transaction`. Single sequential inserts are an artificial worst case and the bench includes them only to keep OxMySQL parity honest.

## Tests

`examples/spacedb-test` is the integration test resource used during development. It checks health, selects, inserts, single row reads, named prepared queries, transactions, stats, and subscriptions.

`examples/spacedb-bench` is a simple in-game benchmark resource. It compares native `spacedb` exports against the real `oxmysql` resource for sequential queries, concurrent queries, and inserts.
