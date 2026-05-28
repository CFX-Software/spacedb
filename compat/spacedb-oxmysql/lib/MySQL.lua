-- spacedb shim for the oxmysql MySQL.lua wrapper.
--
-- This file is loaded by other resources via `shared_script '@oxmysql/lib/MySQL.lua'`
-- in their fxmanifests. It runs inside the consumer's Lua state and builds
-- the same `MySQL` global the original oxmysql ships, but every call routes
-- to `exports.spacedb` instead of to oxmysql's Node-side dist/build.js.

local spacedb = exports.spacedb

-- Translates oxmysql named parameter SQL (:name or @name) into positional
-- `?` and reorders the params table to match. mysql-async historically used
-- `@name`, oxmysql accepts both, QBCore uses `:name`, and ESX stores the
-- sigil inside the key (`{ ['@type'] = 'boat' }`) instead of stripping it
-- (`{ type = 'boat' }`). Look up under all three shapes so the same shim
-- handles every caller style. Skip translation when params is an array.
-- Falls back to NDB.NULL for genuinely missing keys (matches oxmysql).
local function translateNamed(sql, params)
    if type(params) ~= 'table' then return sql, params or {} end
    local named = false
    for k in pairs(params) do
        if type(k) ~= 'number' then named = true; break end
    end
    if not named then return sql, params end
    local positional = {}
    -- First char must be letter/underscore so `'12:00:00'` literals and
    -- `::` casts aren't mis-parsed as named bind tokens.
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

-- Returns the leading verb (SELECT, INSERT, etc) of a SQL statement.
-- nil-safe: queries that start with `(` (wrapped SELECT) or a comment
-- yield no verb match; return '' rather than erroring on :upper().
local function sqlVerb(query)
    local v = (query or ''):match('^%s*([%a]+)')
    return v and v:upper() or ''
end

-- Counts real `?` placeholders (oxmysql ignores `??` identifier escapes,
-- but the Go driver/our callers never use those, so a plain count matches
-- query.split('?').length - 1 from oxmysql's parseExecute).
local function countPlaceholders(sql)
    local _, n = (sql or ''):gsub('%?', '')
    return n
end

-- Mirrors oxmysql's parseExecute (used by prepare / rawExecute). Unlike the
-- query path, prepare params are an ARRAY OF PARAMETER SETS, one set per
-- statement execution — they are NEVER array-expanded for IN() lists.
--   {a, b}            -> {{a, b}}              (one set; flat list wrapped)
--   {{a,b},{c,d}}     -> {{a,b},{c,d}}         (already sets; used as-is)
--   {{a,b}}           -> {{a,b}}               (one set; NOT IN-expansion)
-- Each set is padded with nil up to the placeholder count, matching
-- oxmysql filling missing values with null. This is the fix for the
-- ox_inventory corruption where a 1-row prepare batch ({{json,cid}}) was
-- wrongly treated as an IN-list array and rewritten to `inventory = ?,?
-- WHERE citizenid = NULL`.
local function parseExecuteSets(placeholders, params)
    if type(params) ~= 'table' then return {} end

    -- Every element a table -> already an array of sets.
    local everyTable = true
    local n = #params
    for i = 1, n do
        if type(params[i]) ~= 'table' then everyTable = false; break end
    end
    local sets
    if everyTable and n > 0 then
        sets = params
    else
        -- Flat positional list -> a single set.
        sets = { params }
    end

    -- Pad each set up to the placeholder count (oxmysql null-fills).
    for i = 1, #sets do
        local set = sets[i]
        if type(set) == 'table' then
            for j = #set + 1, placeholders do
                set[j] = nil
            end
        end
    end
    return sets
end

-- True when params is a table of >=2 tables, e.g. {{1,'a'},{2,'b'}} — used
-- by oxmysql's batched prepare to run the same SQL with N param sets.
-- Single-element outer tables ({{...}}) are ambiguous: they could be a
-- one-row batch, or one positional param whose value is an array used
-- for IN-list expansion. ESX multichar uses the IN form so we prefer it
-- and route single-element shapes through expandArrayParams.
local function isBatchedParams(params)
    if type(params) ~= 'table' or #params < 2 then return false end
    for i = 1, #params do
        if type(params[i]) ~= 'table' then return false end
    end
    return true
end

-- Expands array values in positional params into multiple `?` placeholders.
-- ESX writes `WHERE col IN (?)` and binds `{ {"a","b","c"} }`; real oxmysql
-- inlines the array, the Go MySQL driver does not. Walks the SQL once,
-- substituting each `?` whose bound value is a table with `?,?,?...`.
-- Batched param sets ({{...},{...}}) are NOT expanded here — they get
-- routed to executeMany by the caller and would otherwise be mistaken
-- for IN-list arrays.
local function expandArrayParams(sql, params)
    if type(params) ~= 'table' then return sql, params end
    if isBatchedParams(params) then return sql, params end
    local hasArray = false
    local nParams = #params
    for i = 1, nParams do
        if type(params[i]) == 'table' then hasArray = true; break end
    end
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

-- Smart-unwraps a single prepare result the way oxmysql does when
-- unpack=true: INSERT/REPLACE -> insertId, UPDATE/DELETE -> rowsAffected,
-- SELECT zero rows -> nil, 1 row 1 col -> scalar, 1 row N cols -> row,
-- N rows -> array.
local function prepareUnwrap(verb, sql, set)
    if verb == 'INSERT' or verb == 'REPLACE' then
        local r = spacedb:execute(sql, set)
        return r and (r.lastInsertId or r.insertId) or 0
    elseif verb == 'UPDATE' or verb == 'DELETE' then
        local r = spacedb:execute(sql, set)
        return r and (r.rowsAffected or r.affectedRows) or 0
    end
    local rows = spacedb:query(sql, set) or {}
    if #rows == 0 then return nil end
    if #rows == 1 then
        local row = rows[1]
        local cols, only = 0, nil
        for _, v in pairs(row) do
            cols = cols + 1
            only = v
            if cols > 1 then return row end
        end
        return only
    end
    return rows
end

-- prepare / rawExecute pipeline. params are parameter SETS (see
-- parseExecuteSets), executed once each. NO IN-list array expansion.
local function runExecute(method, query, params)
    query = (translateNamed(query, type(params) == 'table' and params or {}))
    local placeholders = countPlaceholders(query)
    local sets = parseExecuteSets(placeholders, params or {})
    local verb = sqlVerb(query)

    if method == 'rawExecute' then
        -- rawExecute returns raw result(s), unpack=false in oxmysql.
        if #sets <= 1 then
            local set = sets[1] or {}
            if verb == 'SELECT' or verb == 'SHOW' or verb == 'WITH' then
                return spacedb:query(query, set) or {}
            end
            local r = spacedb:execute(query, set)
            return {
                affectedRows = r and (r.rowsAffected or r.affectedRows) or 0,
                insertId     = r and (r.lastInsertId or r.insertId) or 0,
            }
        end
        return spacedb:executeMany(query, sets)
    end

    -- prepare: unpack=true.
    if #sets <= 1 then
        return prepareUnwrap(verb, query, sets[1] or {})
    end
    -- Multi-set prepare. INSERT/UPDATE/DELETE batch through executeMany;
    -- SELECT batch loops (rare).
    if verb == 'INSERT' or verb == 'REPLACE' then
        local r = spacedb:executeMany(query, sets)
        return r and (r.lastInsertId or r.insertId) or 0
    elseif verb == 'UPDATE' or verb == 'DELETE' then
        local r = spacedb:executeMany(query, sets)
        return r and (r.rowsAffected or r.affectedRows) or 0
    end
    local out = {}
    for i = 1, #sets do
        out[i] = prepareUnwrap(verb, query, sets[i])
    end
    return out
end

local function runSync(method, query, params)
    if method == 'prepare' or method == 'rawExecute' then
        return runExecute(method, query, params)
    end
    query, params = translateNamed(query, params or {})
    query, params = expandArrayParams(query, params)
    if method == 'query' or method == 'fetchAll' then
        -- oxmysql.query accepts any statement, returning a result object
        -- for DDL/DML callers (esx_property checks `result?.affectedRows`).
        -- Route non-SELECT statements through execute so we can return
        -- a shape with affectedRows + insertId. Empty SELECT returns {}.
        local verb = sqlVerb(query)
        if verb ~= 'SELECT' and verb ~= 'SHOW' and verb ~= 'EXPLAIN'
           and verb ~= 'DESCRIBE' and verb ~= 'DESC' and verb ~= 'WITH' then
            local r = spacedb:execute(query, params or {})
            return { affectedRows = r and r.rowsAffected or 0, insertId = r and r.lastInsertId or 0 }
        end
        return spacedb:query(query, params) or {}
    elseif method == 'single' or method == 'fetchSingle' then
        return spacedb:single(query, params)
    elseif method == 'scalar' or method == 'fetchScalar' then
        local row = spacedb:single(query, params)
        if not row then return nil end
        for _, v in pairs(row) do return v end
        return nil
    elseif method == 'insert' then
        -- Bulk insert: caller passed {{...}, {...}} param sets.
        if isBatchedParams(params) then
            local r = spacedb:executeMany(query, params)
            return r and (r.lastInsertId or r.insertId) or 0
        end
        local r = spacedb:execute(query, params)
        return r and (r.lastInsertId or r.insertId) or 0
    elseif method == 'execute' or method == 'update' then
        if isBatchedParams(params) then
            local r = spacedb:executeMany(query, params)
            return r and r.rowsAffected or 0
        end
        local r = spacedb:execute(query, params)
        return r and r.rowsAffected or 0
    end
end

local function callbackArgsShift(params, cb)
    if type(params) == 'function' then return nil, params end
    if type(params) == 'table' and params.__cfx_functionReference then
        return nil, params
    end
    return params, cb
end

local function runAsync(method, query, params, cb)
    params, cb = callbackArgsShift(params, cb)
    local result = runSync(method, query, params)
    if cb then
        local ok, err = pcall(cb, result)
        if not ok then
            print(('[oxmysql shim] callback for %s errored: %s'):format(method, tostring(err)))
        end
    end
    return result
end

local function makeMethod(name)
    local t = setmetatable({ method = name }, {
        __call = function(_, query, params, cb)
            return runAsync(name, query, params, cb)
        end,
    })
    t.await = function(query, params) return runSync(name, query, params) end
    return t
end

local MySQL = {}

for _, m in ipairs({ 'query', 'scalar', 'single', 'insert', 'update', 'prepare', 'rawExecute' }) do
    MySQL[m] = makeMethod(m)
end

local function buildTxSteps(steps)
    local out = {}
    for _, s in ipairs(steps or {}) do
        local q, p
        if type(s) == 'table' then
            q, p = translateNamed(s.query, s.values or s.params or s.parameters or {})
        elseif type(s) == 'string' then
            q, p = s, {}
        end
        if q then out[#out + 1] = { mode = 'execute', query = q, params = p } end
    end
    return out
end

MySQL.transaction = setmetatable({ method = 'transaction' }, {
    __call = function(_, steps, params, cb)
        steps, cb = callbackArgsShift(steps, cb)
        if type(params) == 'function' then cb = params end
        local ok = pcall(function() spacedb:transaction(buildTxSteps(steps)) end)
        if cb then pcall(cb, ok) end
        return ok
    end,
})
MySQL.transaction.await = function(steps)
    local ok = pcall(function() spacedb:transaction(buildTxSteps(steps)) end)
    return ok
end

MySQL.Async = {
    fetchAll    = MySQL.query,
    fetchSingle = MySQL.single,
    fetchScalar = MySQL.scalar,
    execute     = MySQL.update,
    insert      = MySQL.insert,
    prepare     = MySQL.prepare,
    transaction = MySQL.transaction,
}
MySQL.Sync = {
    fetchAll    = function(q, p) return MySQL.query.await(q, p) end,
    fetchSingle = function(q, p) return MySQL.single.await(q, p) end,
    fetchScalar = function(q, p) return MySQL.scalar.await(q, p) end,
    execute     = function(q, p) return MySQL.update.await(q, p) end,
    insert      = function(q, p) return MySQL.insert.await(q, p) end,
    prepare     = function(q, p) return MySQL.prepare.await(q, p) end,
    transaction = function(s) return MySQL.transaction.await(s) end,
}

MySQL.ready = setmetatable({
    await = function() return true end,
}, {
    __call = function(_, cb)
        if type(cb) == 'function' then
            Citizen.CreateThread(function() cb() end)
        end
    end,
})

function MySQL.awaitConnection() return true end

function MySQL.startTransaction(cb)
    if type(cb) == 'function' then
        local handle = {
            query   = function(_, q, p) return runSync('query', q, p) end,
            execute = function(_, q, p) return runSync('update', q, p) end,
            commit  = function() return true end,
            rollback = function() return false end,
        }
        Citizen.CreateThread(function() cb(handle) end)
    end
end

_ENV.MySQL = MySQL
