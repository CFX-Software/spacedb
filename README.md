# spacedb

spacedb is a fast database resource for FiveM servers.

It runs a small Go service beside the FiveM resource and gives server scripts a simple export API for queries, writes, transactions, metrics, and live query subscriptions.

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

The local `config.json` uses Postgres on port `55432`.

MariaDB also starts on port `53306`. To test it, set the driver to `mysql` and use this DSN.

```text
spacedb:spacedb@tcp(127.0.0.1:53306)/spacedb?parseTime=true
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
local tx = exports.spacedb:transaction({
    { mode = 'execute', query = 'INSERT INTO users (name) VALUES (?)', params = { 'Jane' } },
    { mode = 'query', query = 'SELECT * FROM users', params = {} }
})
```

## Notes

`config.json` and `bin` are ignored on purpose. Local database passwords, generated binaries, logs, and machine setup should not be committed.

## Compatibility

`compat/oxmysql` contains an OxMySQL style adapter resource. It forwards common exports to `spacedb`:

```lua
exports.oxmysql:query('SELECT 1 AS ok', {}, function(rows)
    print(json.encode(rows))
end)
```

To test it as a drop in replacement, copy `compat/oxmysql` to a resource folder named `oxmysql`, then ensure it after `spacedb`.

```cfg
ensure spacedb
ensure oxmysql
```

The adapter currently covers `query`, `single`, `scalar`, `execute`, `insert`, `update`, `prepare`, and `transaction`.

## Tests

`examples/spacedb-test` is the integration test resource used during development. It checks health, selects, inserts, single row reads, named prepared queries, transactions, stats, and subscriptions.
