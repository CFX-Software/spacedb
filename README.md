# spacedb

spacedb is a fast database resource for FiveM servers.

It runs a small Go service beside the FiveM resource and gives server scripts a simple export API for queries, writes, transactions, metrics, and live query subscriptions.

The runtime path uses a persistent local TCP transport for query calls. HTTP stays available for health checks, stats, subscriptions, and debugging.

The first target is a clean native API. Compatibility layers for older resources such as OxMySQL and mysql async belong in the next pass once the core behavior is stable.

## Local setup

1. Start the dev databases.

```powershell
docker compose up -d
```

2. Build the core service.

```powershell
cd core
go build -o ../bin/spacedb-core.exe ./cmd/spacedb-core
```

3. Make sure the config is valid.

```powershell
bin\spacedb-core.exe -config config.json -check-config
```

4. Add the resource to `server.cfg`.

```cfg
ensure spacedb
```

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

The adapter currently covers `query`, `single`, `scalar`, `execute`, `insert`, `update`, `prepare`, and `transaction`.

## Tests

`examples/spacedb-test` is the integration test resource used during development. It checks health, selects, inserts, single row reads, named prepared queries, transactions, stats, and subscriptions.

`examples/spacedb-bench` is a simple in-game benchmark resource. It compares native `spacedb` exports against the real `oxmysql` resource for sequential queries, concurrent queries, and inserts.
