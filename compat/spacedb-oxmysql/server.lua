local function callbackOrReturn(value, cb)
    if cb then
        cb(value)
        return nil
    end
    return value
end

local function normalizeParams(params, cb)
    if type(params) == 'function' then
        return {}, params
    end
    return params or {}, cb
end

-- Helpers ported from lib/MySQL.lua so resource-exports callers
-- (exports.oxmysql:query / :update / :execute / ...) get the same SQL
-- preprocessing the shared_script consumers get. Real oxmysql relies on
-- the mysql2 driver to expand `?` against array values and to silently
-- fill missing placeholders with NULL; the Go MySQL driver under spacedb
-- does neither, so we must do it here before handing the query off.

-- Returns the leading verb (SELECT, INSERT, etc) of a SQL statement.
local function sqlVerb(sql)
    return ((sql or ''):match('^%s*([%a]+)') or ''):upper()
end

-- True when params is a list of >=2 tables, e.g. {{1,'a'},{2,'b'}}.
local function isBatchedParams(params)
    if type(params) ~= 'table' or #params < 2 then return false end
    for i = 1, #params do
        if type(params[i]) ~= 'table' then return false end
    end
    return true
end

-- Translates oxmysql named parameter SQL (:name or @name) into positional
-- `?` and reorders params to match. Looks up the key under all four
-- shapes QBCore / ESX / mysql-async / oxmysql historically use.
local function translateNamed(sql, params)
    if type(params) ~= 'table' then return sql, params or {} end
    local named = false
    for k in pairs(params) do
        if type(k) ~= 'number' then named = true; break end
    end
    if not named then return sql, params end
    local positional = {}
    local newSql = sql:gsub('([@:])([%a_][%w_]*)', function(sigil, name)
        local v = params[name]
        if v == nil then v = params[sigil .. name] end
        if v == nil then v = params['@' .. name] end
        if v == nil then v = params[':' .. name] end
        positional[#positional + 1] = v
        return '?'
    end)
    return newSql, positional
end

-- Expands array-valued positional params into multiple `?` placeholders.
-- - `IN (?)` bound to `{1,2,3}` becomes `IN (?,?,?)` bound to 1,2,3.
-- - `IN (?)` bound to `{}` becomes `IN (NULL)` (no placeholder), so the
--   query stays valid and the row count is zero — matches what callers
--   would see under real oxmysql + mysql2.
-- - Batched param sets ({{...},{...}}) are NOT expanded; they go through
--   executeMany instead.
local function expandArrayParams(sql, params)
    if type(params) ~= 'table' then return sql, params end
    if isBatchedParams(params) then return sql, params end
    local hasArray = false
    local nParams = #params
    for i = 1, nParams do
        if type(params[i]) == 'table' then hasArray = true; break end
    end
    -- Count placeholders. If params is fully populated AND no array
    -- values, nothing to do.
    local _, placeholderCount = sql:gsub('%?', '')
    if not hasArray and placeholderCount <= nParams then
        return sql, params
    end
    local out, flat = {}, {}
    local pIdx = 1
    for i = 1, #sql do
        local c = sql:sub(i, i)
        if c == '?' then
            local v = params[pIdx]
            pIdx = pIdx + 1
            if v == nil then
                -- Real oxmysql + mysql2 silently binds missing params as
                -- NULL. The Go driver errors instead, so substitute a
                -- literal so `IN (?)` with no params stays valid.
                out[#out + 1] = 'NULL'
            elseif type(v) == 'table' then
                local n = #v
                if n == 0 then
                    out[#out + 1] = 'NULL'
                else
                    local placeholders = {}
                    for j = 1, n do
                        placeholders[j] = '?'
                        flat[#flat + 1] = v[j]
                    end
                    out[#out + 1] = table.concat(placeholders, ',')
                end
            else
                out[#out + 1] = '?'
                flat[#flat + 1] = v
            end
        else
            out[#out + 1] = c
        end
    end
    return table.concat(out), flat
end

-- Final preprocessing: named -> positional, then array expansion. The
-- expansion step also substitutes literal NULL for missing-positional
-- slots so `IN (?)` with no params stays valid (matches real oxmysql
-- parseArguments + mysql2 behavior).
local function preprocess(sql, params)
    params = params or {}
    sql, params = translateNamed(sql, params)
    sql, params = expandArrayParams(sql, params)
    return sql, params
end

local SELECT_LIKE = {
    SELECT = true, SHOW = true, EXPLAIN = true,
    DESCRIBE = true, DESC = true, WITH = true,
}

-- Real oxmysql's `query` / `fetch` / `execute` (they're aliases of one
-- another) returns:
--   - SELECT-like: array of rows
--   - INSERT/UPDATE/DELETE/DDL: mysql2 ResultSetHeader, i.e. an object
--     with .affectedRows and .insertId fields
-- The shim previously returned a bare number for non-SELECT, which made
-- JS callers doing `result.affectedRows` crash with "attempt to index a
-- number value" (nteam-nitro). Mirror the object shape; also keep the
-- spacedb-native keys (rowsAffected, lastInsertId) so anything that
-- already learned to read those keeps working.
local function writeShape(result)
    local affected = result and (result.rowsAffected or result.affectedRows) or 0
    local insertId = result and (result.lastInsertId or result.insertId) or 0
    return {
        affectedRows  = affected,
        insertId      = insertId,
        rowsAffected  = affected,
        lastInsertId  = insertId,
    }
end

local function queryLike(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    sql, params = preprocess(sql, params)
    local verb = sqlVerb(sql)
    if SELECT_LIKE[verb] then
        local rows = exports.spacedb:query(sql, params) or {}
        return callbackOrReturn(rows, cb)
    end
    local result = exports.spacedb:execute(sql, params)
    return callbackOrReturn(writeShape(result), cb)
end

local function query(sql, params, cb)
    return queryLike(sql, params, cb)
end

local function single(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    sql, params = preprocess(sql, params)
    local result = exports.spacedb:single(sql, params)
    return callbackOrReturn(result, cb)
end

local function scalar(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    sql, params = preprocess(sql, params)
    local row = exports.spacedb:single(sql, params)
    local result = nil

    if row then
        for _, value in pairs(row) do
            result = value
            break
        end
    end

    return callbackOrReturn(result, cb)
end

local function execute(sql, params, cb)
    -- Real oxmysql: MySQL.execute = MySQL.query, returns the result object.
    return queryLike(sql, params, cb)
end

local function insert(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    sql, params = preprocess(sql, params)
    local result = exports.spacedb:execute(sql, params)
    return callbackOrReturn(result and (result.lastInsertId or result.insertId) or 0, cb)
end

local function update(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    sql, params = preprocess(sql, params)
    local result = exports.spacedb:execute(sql, params)
    return callbackOrReturn(result and (result.rowsAffected or result.affectedRows) or 0, cb)
end

-- Real oxmysql `prepare(sql, params, cb)` smart-unwraps the result:
--   INSERT/REPLACE -> lastInsertId (number)
--   UPDATE/DELETE  -> rowsAffected (number)
--   SELECT zero rows         -> nil
--   SELECT 1 row x 1 col     -> scalar value
--   SELECT 1 row x multi col -> row table
--   SELECT N rows            -> rows array
-- The previous shim also supported a (name, sql, params) form. Keep that
-- shape working for any existing caller, but don't preprocess the name
-- form's "params" arg since it's actually the SQL string.
local function prepare(nameOrSql, sqlOrParams, maybeOptions, maybeCb)
    if type(sqlOrParams) == 'string' then
        local result = exports.spacedb:prepare(nameOrSql, sqlOrParams, maybeOptions or {})
        return callbackOrReturn(result, maybeCb)
    end

    local sql, params = preprocess(nameOrSql, sqlOrParams or {})
    local verb = sqlVerb(sql)
    if verb == 'INSERT' or verb == 'REPLACE' then
        local r
        if isBatchedParams(params) then
            r = exports.spacedb:executeMany(sql, params)
        else
            r = exports.spacedb:execute(sql, params)
        end
        return callbackOrReturn(r and (r.lastInsertId or r.insertId) or 0, maybeOptions)
    elseif verb == 'UPDATE' or verb == 'DELETE' then
        local r
        if isBatchedParams(params) then
            r = exports.spacedb:executeMany(sql, params)
        else
            r = exports.spacedb:execute(sql, params)
        end
        return callbackOrReturn(r and (r.rowsAffected or r.affectedRows) or 0, maybeOptions)
    end
    local rows = exports.spacedb:query(sql, params) or {}
    if #rows == 0 then return callbackOrReturn(nil, maybeOptions) end
    if #rows == 1 then
        local row = rows[1]
        local n, only = 0, nil
        for _, v in pairs(row) do
            n = n + 1
            only = v
            if n > 1 then return callbackOrReturn(row, maybeOptions) end
        end
        return callbackOrReturn(only, maybeOptions)
    end
    return callbackOrReturn(rows, maybeOptions)
end

local function transaction(queries, paramsOrCb, maybeCb)
    local cb = maybeCb
    local sharedParams = nil

    if type(paramsOrCb) == 'function' then
        cb = paramsOrCb
    elseif type(paramsOrCb) == 'table' then
        sharedParams = paramsOrCb
    end

    local steps = {}
    for _, item in ipairs(queries or {}) do
        local sql, params
        if type(item) == 'string' then
            sql, params = preprocess(item, sharedParams or {})
            steps[#steps + 1] = { mode = 'execute', query = sql, params = params }
        else
            sql, params = preprocess(item.query, item.values or item.params or item.parameters or {})
            steps[#steps + 1] = { mode = item.mode or 'execute', query = sql, params = params }
        end
    end

    local ok = pcall(function()
        exports.spacedb:transaction(steps)
    end)

    return callbackOrReturn(ok, cb)
end

local function rawExecute(sql, rows, cb)
    -- OxMySQL rawExecute: one SQL, many param sets, batched. On SELECT it
    -- returns rows like query. Map writes to executeMany; map SELECT to a
    -- single query against the first param set (rare path — most callers
    -- use rawExecute for INSERT/UPDATE only).
    rows, cb = normalizeParams(rows, cb)
    if sqlVerb(sql) == 'SELECT' then
        local first = rows[1] or {}
        local s, p = preprocess(sql, first)
        local result = exports.spacedb:query(s, p)
        return callbackOrReturn(result, cb)
    end
    -- For batched writes we cannot rebuild placeholder counts per-row
    -- (every row must match the same SQL), but we can still translate
    -- named params on the SQL string against the first row's keys. The
    -- common case here is positional `?` already, so the no-op fast path
    -- in translateNamed handles it.
    local sqlOut = sql
    if type(rows) == 'table' and rows[1] then
        sqlOut = (translateNamed(sql, rows[1]))
    end
    local result = exports.spacedb:executeMany(sqlOut, rows)
    return callbackOrReturn(result, cb)
end

-- Deprecated aliases real oxmysql still ships:
-- MySQL.fetch == MySQL.query, MySQL.store returns the SQL unchanged.
local function fetch(sql, params, cb)
    return query(sql, params, cb)
end

local function store(sql, cb)
    return callbackOrReturn(sql, cb)
end

local function isReady()
    return true
end

local function awaitConnection()
    return true
end

-- Real oxmysql registers every method under THREE export names:
--   <key>            sync-style (callback or return)
--   <key>_async      promise/async variant (modern code)
--   <key>Sync        deprecated alias of _async (older code still uses it)
-- Scripts written before the _async rename (and a lot of QBCore/ESX
-- modules) call fetchSync/executeSync/insertSync/etc. Missing any one
-- of these throws "No such export <name> in resource oxmysql".
local methods = {
    query = query,
    single = single,
    scalar = scalar,
    execute = execute,
    insert = insert,
    update = update,
    prepare = prepare,
    transaction = transaction,
    rawExecute = rawExecute,
    fetch = fetch,
    store = store,
    isReady = isReady,
    awaitConnection = awaitConnection,
}

for name, fn in pairs(methods) do
    exports(name, fn)
    exports(name .. '_async', fn)
    exports(name .. 'Sync', fn)
end
