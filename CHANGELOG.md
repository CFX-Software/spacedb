# Changelog

## 0.2.0

### Performance
- ExecuteMany now chunks at 10,000 rows instead of 500 and reuses prepared statements. Bulk insert went from 33% slower than OxMySQL to 12% faster.
- Concurrent insert throughput up to 3,145 qps with a tuned 128-conn pool (2.38x OxMySQL).
- Concurrent query throughput at 3,731 qps (1.70x OxMySQL).
- `getById` cache hits at ~50 microseconds (3.16x OxMySQL single-by-id).

### Realtime subscriptions
- Subscribe events push over the same TCP socket instead of HTTP polling.
- Writes through spacedb fire the subscriber callback within single-digit milliseconds (measured 7ms end-to-end).
- Race-safe callback registration: events arriving before the subscribe response settles are buffered and drained when the callback is set.
- External writes still caught by the 250ms poll fallback.

### Drop-in compatibility
- `compat/spacedb-oxmysql` now ships a `lib/MySQL.lua` wrapper that builds the `MySQL` global from `exports.spacedb` directly. Drop-in for QBCore, ESX, and anything that includes `@oxmysql/lib/MySQL.lua`.
- Named parameter translation: handles both `:name` and `@name` syntaxes, reorders params to match positional `?` placeholders, ignores digit-leading patterns so string literals like `'12:00:00'` survive.
- `MySQL.prepare.await` smart unwrap: single-row single-column returns the scalar (QBCore's CreateCitizenId uniqueness loop relies on this), single-row multi-column returns the row, multi-row returns the array.
- Verb-aware prepare routing: INSERT/REPLACE return lastInsertId, UPDATE/DELETE return rowsAffected.
- Batched param sets `{{...}, {...}}` route through executeMany for insert/update/prepare.
- Two more shim resources: `compat/spacedb-mysql-async` and `compat/spacedb-ghmattimysql`.

### Auto-configuration
- Reads the same `mysql_connection_string` convar OxMySQL uses. If `config.json` is absent, spacedb generates one on first boot using the convar.
- If the convar changes (different host, new password), spacedb syncs `database.dsn` and `database.driver` into `config.json` on next boot. Pool size, ports, and other tuning are preserved.

### Diagnostics
- `/metrics` HTTP endpoint with per-op count, errors, avg, p50, p95, p99, max.
- `/diagnostics` HTTP endpoint with the full bundle: version, redacted DSN, uptime, config, pool stats, cache stats, last 100 SQL errors with timestamps.
- `spacelog` server console command writes the bundle to a timestamped JSON file for bug reports. Passwords are masked.
- Level-gated logger via `spacedb_log_level` convar (error, warn, info, debug). Default `info` emits one ready line plus warnings and errors.

### Core hardening
- Per-connection backpressure: TCP server caps in-flight handlers per socket at 128 to prevent goroutine blowups under load.
- Cache parser handles backtick-quoted MySQL identifiers and ANSI double-quoted Postgres identifiers in UPDATE/DELETE/INSERT/REPLACE/ON DUPLICATE/ON CONFLICT/TRUNCATE.
- Server package split from a 763-line god file into transport, cache_ops, cache_invalidation, http_handlers, metrics, diagnostics, and server.

## 0.1.0

Initial release. TCP transport, Go core, basic cache, polling subscriptions, OxMySQL adapter.
