# Arc: in-process read cache

Status: DESIGN
Target: 100x on cache-hit reads. 1.5 ms → 15 µs. Convex-tier latency for hot keys.
Why: every `query()`/`single()` path is bottlenecked by the MySQL driver RTT (3.79 ms profile data). The only way past that floor is to serve reads from Go-side memory.

## API shape

Row-level cache keyed by `(table, primary_key)`. Not query-level.

Reason: FiveM hot path is overwhelmingly `SELECT * FROM users WHERE id = ?`. Query-level cache misses on different params, can't invalidate cleanly. Row-level matches what FiveM resources actually do.

```lua
-- read-through: cache hit returns immediately; miss fetches + caches
local user = exports.spacedb:getById('users', 5)

-- write-through: writes to MySQL + updates cache
exports.spacedb:setById('users', 5, { name = 'Jane', score = 100 })

-- manual invalidation for queries we can't auto-track
exports.spacedb:invalidate('users', 5)
exports.spacedb:invalidateTable('users')  -- wipe entire table from cache

-- existing exports (query/execute) keep working — cache is opt-in
```

## Cache module (core/internal/cache/cache.go)

```
type Cache struct {
    shards [256]shard       // hash(table+pk) >> 8 for shard index
    maxBytes int64          // total memory bound
    ttl time.Duration       // optional, 0 = no TTL
}

type shard struct {
    mu sync.RWMutex
    items map[string]*entry // key = "table:pk"
    lru *list.List          // LRU ordering for eviction
}

type entry struct {
    row map[string]any
    bytes int
    expiresAt time.Time
    elem *list.Element  // LRU position
}
```

Shard count tuned so write contention stays low (256 shards = ~256-way concurrency before any locking matters).

LRU + size-bounded eviction. TTL optional (zero by default — eviction only on memory pressure or explicit invalidate).

## Transport ops (server.go)

Three new ops: `cacheGet`, `cacheSet`, `cacheInvalidate`.

cacheGet payload `{table, key}` → on hit returns `{row, cached: true}` in <50µs. On miss the SERVER does the SELECT, caches, returns `{row, cached: false}` so the caller can't tell the difference except by the cached flag.

cacheSet payload `{table, key, row}` → server runs INSERT or UPDATE, on success updates the cache entry.

cacheInvalidate payload `{table, key?}` → drop one entry or the whole table.

## Auto-invalidation (Phase 3)

Parse `UPDATE table SET ... WHERE pk = ?` and `DELETE FROM table WHERE pk = ?` to extract `(table, pk)` and call `cache.Invalidate(table, pk)` after the write commits.

Fallback: any UPDATE/DELETE without a simple `pk = literal` WHERE drops the entire table from the cache (correctness first; tune later).

INSERT: cache the new row by `LastInsertId()` if the table has been registered.

## Consistency model

Read-after-write consistency: setById updates the cache synchronously before returning. Subsequent getById sees the new value.

Cross-process: spacedb-core is single-process, so no cache coherency problem at the cache layer. The MySQL connection pool can produce stale reads if another resource bypasses spacedb to write — that's out of scope.

Eventual: not used. Writes wait for both cache update + MySQL commit. We don't trade durability for speed; we just skip the read RTT.

## Phases

| Phase | Scope | Files | Ship gate |
|---|---|---|---|
| 1 | Cache module + Go unit tests | `core/internal/cache/cache.go`, `cache_test.go` | go test ./internal/cache passes |
| 2 | Transport ops + JS exports | `core/internal/server/server.go`, `server/js/spacedb.js`, `fxmanifest.lua` | getById hits the cache after first call (bench reveals) |
| 3 | Auto-invalidation parser | `core/internal/cache/parser.go`, `parser_test.go` | UPDATE/DELETE on cached row invalidates the entry, verified in test |
| 4 | Bench + README docs | `examples/spacedb-bench/server.lua`, `README.md` | Bench shows >50x on cache-hit reads vs query() |

## NOT in scope

- Distributed cache (single-process only)
- Cross-machine invalidation (one FxServer per core process)
- Result-set caching for multi-row SELECTs (only single-row by PK)
- WAL / persistence (cache is volatile; MySQL is source of truth)
- Pre-warming on startup (cold start is fine; first read fetches + caches)
- Reactive subscription integration (Arc 2)

## Risks

- **Auto-invalidation false negatives** if SQL parser misses a case. Mitigation: explicit invalidate() always available; aggressive `invalidateTable` on any UPDATE/DELETE that doesn't match the simple shape.
- **Memory growth** with many tables/keys. Mitigation: size-bounded LRU, `stats` export to monitor.
- **API confusion** with new exports alongside query/execute. Mitigation: README + clear examples that show "use getById for hot PK lookups, keep query for analytics."
