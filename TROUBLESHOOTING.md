# Troubleshooting

## "spacedb-core failed to start"

Almost always one of:

1. Port `37121` already taken. Check with `netstat -ano | findstr 37121` (Windows) or `ss -tlnp | grep 37121` (Linux). Either kill the other process or change `spacedb_transport`.
2. Binary missing from `bin/`. See the platform table in the README. On Linux, set `spacedb_core_platform linux`.
3. Binary not executable on Linux. `chmod +x bin/spacedb-core`.

The supervisor will keep retrying with exponential backoff up to 5s. Run `spacelog` after a minute and look at the `lastErrors` field for the actual cause.

## "cannot connect to database"

- `mysql_connection_string` is empty or has the wrong DSN. spacedb accepts both `mysql://user:pass@host:port/db` and the semicolon form `server=...;userid=...;password=...;database=...`.
- The DB is on a Docker container and `127.0.0.1` doesn't resolve from where the core is running. Use the container's network alias or the host's LAN IP.
- The user doesn't have grants. Test with the same DSN from a regular `mysql` CLI client first.

## "query is fast in isolation but slow under load"

Pool size. Default is 32. Bump `pool.maxOpen` in `config.json` to your concurrent worker count + headroom. For 50 workers, try 64-128.

If you're hitting `db-exec` p99 above 50ms under concurrency, the DB itself is the bottleneck — check `SHOW PROCESSLIST` for the actual slow queries.

## "subscribe callback never fires"

- You subscribed but nothing has changed the result set. Subscriptions fire on result-set change, not on every write to the table.
- A different resource is writing to the table directly (not through spacedb). spacedb will catch it on the poll interval (250ms default), not instantly. Lower `subscribe.pollIntervalMs` if you need faster reaction.
- Your SQL has `ORDER BY RAND()` or similar — the diff check sees the result as "changed" every poll. Don't use non-deterministic queries with subscriptions.

## "cache hit rate is 0"

You're not using `getById` / `getMany`. Plain `query` / `single` bypass the row cache by design — they're general SQL. The cache is keyed by `(table, primary_key)` and only the `getById` family populates and reads it.

## "I want to reset everything"

Stop the resource, delete `config.json`, delete `bin/spacedb-core*`. Then `restart spacedb`. The first boot will regenerate config from `mysql_connection_string` and you'll get a fresh state.
