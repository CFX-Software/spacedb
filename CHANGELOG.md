# Changelog

## 0.2.6

OxMySQL shim: correct prepare/rawExecute parameter handling — fixes data corruption.

### spacedb-oxmysql
- **Critical:** `prepare` and `rawExecute` now treat parameters as an array of parameter SETS (oxmysql's `parseExecute` semantics), never as IN-list arrays. Previously a single-row prepare batch like `prepare('UPDATE players SET inventory = ? WHERE citizenid = ?', { { json, citizenid } })` (exactly what ox_inventory sends when saving one inventory) was mistaken for an `IN (?)` array and rewritten to `UPDATE players SET inventory = ?,? WHERE citizenid = NULL`, producing `Error 1064 ... near '? WHERE citizenid = NULL'` and dropping the write. ox_inventory saves now persist correctly.
- The two pipelines now match real oxmysql exactly:
  - `query` / `single` / `scalar` / `update` / `insert` / `execute` / `fetch` → single param set; inner arrays expand for `IN (?)`.
  - `prepare` / `rawExecute` → array of param sets, executed once each (via `executeMany` for multi-row), no array expansion, missing slots null-padded.
- `prepare` smart-unwrap unchanged for single sets (INSERT → insertId, UPDATE/DELETE → rowsAffected, SELECT → scalar/row/array); multi-set returns an array.
- `sqlVerb` is now nil-safe for queries that start with `(` or a comment.
- Applied identically to both the resource-exports path (`server.lua`) and the `@oxmysql/lib/MySQL.lua` shared-script path.
- Validated against real Lua 5.4 (loadfile parse + 11 behavioral assertions covering the ox_inventory case, IN-list expansion, empty `IN ()`, and return shapes).
- Bumps spacedb-oxmysql to 0.2.3.

### Not affected
- `spacedb` core, benchmarks, and other call paths: unchanged. Benchmarks call `exports.spacedb` directly and never touch the shim. `spacedb-oxmysql` is the only compat resource (it also `provide`s mysql-async / ghmattimysql).

## 0.2.5

Subscription leak on consumer resource restart (issue #16).

### Realtime
- `onResourceStop` now releases subscriptions owned by **any** stopped resource, not just spacedb itself. Previously the handler early-returned unless spacedb was the resource stopping, so when a consumer resource restarted its subscriptions stayed registered — the Go core kept polling those queries and firing change events into the consumer's now-dead funcref.
- `subscribe` records the invoking resource (`GetInvokingResource()`, captured synchronously before the transport await) as the subscription owner.
- On consumer stop, every subscription owned by that resource is dropped locally and `unsubscribe` is sent to the core. Resources that re-subscribe on boot receive fresh ids.
- spacedb's own stop path now also clears the owner map alongside the existing teardown.

## 0.2.4

OxMySQL shim parity with real oxmysql on the resource-exports path.

### spacedb-oxmysql
- `exports.oxmysql:query` / `:execute` / `:fetch` now route by SQL verb and return the real oxmysql shape: array of rows for SELECT/SHOW/EXPLAIN/DESC/WITH, `{affectedRows, insertId, rowsAffected, lastInsertId}` for INSERT/UPDATE/DELETE/DDL. The previous shim returned a bare number for `:execute`, which crashed JS callers (e.g. nteam-nitro) doing `result.affectedRows` with `attempt to index a number value`.
- Resource-exports path now applies the same SQL preprocessing the `MySQL.*` shared-script path already did: named placeholder translation (`:name` / `@name` / `{['@name']=v}` / `{name=v}`), `IN (?)` array expansion, empty-array → `IN (NULL)`.
- Missing positional placeholders now substitute literal `NULL` instead of erroring. Fixes `sql: expected 1 arguments, got 0` on `DELETE FROM vehicle_parking WHERE id IN (?)` when callers (AdvancedParking) pass an empty list.
- `prepare` resource-export now smart-unwraps based on SQL verb the same way `MySQL.prepare` does (INSERT/REPLACE → insertId, UPDATE/DELETE → rowsAffected, SELECT → row/scalar/array depending on shape).
- `transaction` + `rawExecute` resource-exports also run the preprocessing pipeline.
- Mirrors the empty-placeholder → NULL fix into `lib/MySQL.lua` so `MySQL.*` callers benefit too.
- Bump spacedb-oxmysql to 0.2.2.

### Verified against
- qb-core, qbx_core, esx_core (es_extended + esx_identity + esx_multicharacter + esx_skin) — no `exports.oxmysql` direct callers in any of them; all `MySQL.*` callers still behave as before. ESX identity's `IN (?)` patterns continue to expand correctly.

## 0.2.3

OxMySQL shim export-surface parity with real oxmysql.

### spacedb-oxmysql
- Adds the `Sync` suffix variant for every method. Real oxmysql registers every method three times (`<m>`, `<m>_async`, `<m>Sync`); the shim previously only registered the first two. Older scripts (and a chunk of QBCore/ESX modules) call `exports.oxmysql:fetchSync` / `executeSync` / `insertSync` / `updateSync` / etc, which threw `No such export fetchSync in resource oxmysql` after the spacedb swap.
- Adds the deprecated aliases real oxmysql still ships: `fetch` (alias of `query`), `store` (returns the SQL unchanged), `isReady`, `awaitConnection`. Each now exists under all three name forms.
- Bumps spacedb-oxmysql to 0.2.1.

## 0.2.2

FiveM sandbox compatibility fixes for `c-scripting-node` permission errors.

### Boot
- Detect Node permission denial during `child_process.spawn` of the Go core and emit explicit setup instructions pointing at `add_unsafe_child_process_permission spacedb` in `server.cfg`. Previously surfaced as a raw "Access to this API has been restricted. Use --allow-child-process to manage permissions." trace with no hint that the grant must come from server.cfg (it cannot be granted from `fxmanifest.lua`).
- `spawnCore` no longer calls `fs.mkdirSync` when the log directory already exists. FiveM's `FilesystemPermissions.cpp` rejects writes that resolve to the resource-root directory with an empty path remainder, which produced a misleading "Filesystem write permission check ... write not allowed" trace on every boot even though spacedb's own resource folder is auto-allowed.

### Docs
- README quick-start now lists `add_unsafe_child_process_permission spacedb` ahead of the `ensure` line and explains why it can't live in `fxmanifest.lua`.

## 0.2.1

OxMySQL compatibility hardening from real-world QBCore and ESX Legacy drop-in tests.

### Shim parameter handling
- Named parameter lookup now tries the stripped name, the sigil-prefixed key, and explicit `@name` / `:name` shapes. QBCore uses `{ name = value }`, ESX uses `{ ['@name'] = value }`, both work.
- Array-valued positional params expand inline into multiple `?` placeholders for `WHERE col IN (?)` patterns. ESX multichar relies on this.
- Empty arrays render as `NULL` instead of producing `IN ()` syntax errors.
- Batched param sets require at least two rows to be treated as `executeMany`. Single-element outer shapes (`{{a,b}}`) fall through to array expansion so single-row IN-list calls aren't mistaken for batched inserts.

### Shim result shapes
- `MySQL.prepare.await` smart-unwraps: single-row single-column returns the scalar value, single-row multi-column returns the row, multi-row returns the rows array, zero rows returns nil. QBCore's `CreateCitizenId` uniqueness loop and player loader rely on this.
- `MySQL.prepare` routes by SQL verb: INSERT/REPLACE return lastInsertId, UPDATE/DELETE return rowsAffected.
- `MySQL.query` with non-SELECT verbs (ALTER, CREATE, INSERT, UPDATE, DELETE) routes through execute and returns `{ affectedRows, insertId }` instead of an empty rows array. `esx_property`'s `result?.affectedRows` check on ALTER TABLE works.

### Distribution
- Shim ships `lib/MySQL.lua` so consumer scripts importing `@oxmysql/lib/MySQL.lua` via shared_script get a working `MySQL` global without changes.
- fxmanifest declares `provide 'mysql-async'` and `provide 'ghmattimysql'`, `lua54 'yes'`, `game 'common'` to match real oxmysql.

### Tested
- QBCore (qb-core 1.x + qb-multicharacter, qb-banking, qb-houses, qb-inventory, qb-policejob, qb-vehiclekeys and friends) up through resource boot and player connect.
- ESX Legacy 1.13.5 (es_extended, esx_multicharacter, esx_identity, esx_skin, esx_property, esx_datastore, esx_addoninventory, esx_addonaccount, esx_boat) up through ESX initialization.
- Test DB: MariaDB 11.8.6 on Docker. End-user FiveM boxes ship MariaDB by default, so this matches real deployment.

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
