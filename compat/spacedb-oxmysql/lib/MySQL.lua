-- spacedb shim for the oxmysql MySQL.lua wrapper.
--
-- This file is loaded by other resources via `shared_script '@oxmysql/lib/MySQL.lua'`
-- in their fxmanifests. It runs inside the consumer's Lua state and builds
-- the same `MySQL` global the original oxmysql ships, but every call routes
-- to `exports.spacedb` instead of to oxmysql's Node-side dist/build.js.

local spacedb = exports.spacedb

-- Translates oxmysql named parameter SQL ({:name} or {@name}) into
-- positional `?` and reorders the params table to match. mysql-async
-- historically used `@name`, oxmysql accepts both, and QBCore uses
-- `:name` in INSERT/UPDATE statements. Skip translation when params is
-- already an array.
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
    local newSql = sql:gsub('[@:]([%a_][%w_]*)', function(name)
        positional[#positional + 1] = params[name]
        return '?'
    end)
    return newSql, positional
end

-- Returns the leading verb (SELECT, INSERT, etc) of a SQL statement.
local function sqlVerb(query)
    return (query or ''):match('^%s*([%a]+)'):upper()
end

-- True when params is a table of tables, e.g. {{1, 'a'}, {2, 'b'}} — used
-- by oxmysql's batched prepare to run the same SQL with N param sets.
local function isBatchedParams(params)
    if type(params) ~= 'table' or #params == 0 then return false end
    for i = 1, #params do
        if type(params[i]) ~= 'table' then return false end
    end
    return true
end

local function runSync(method, query, params)
    query, params = translateNamed(query, params or {})
    if method == 'query' or method == 'fetchAll' then
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
    elseif method == 'prepare' then
        local verb = sqlVerb(query)
        -- INSERT/UPDATE/DELETE/REPLACE: actually run as execute and
        -- return the right shape. oxmysql's prepare returns lastInsertId
        -- for INSERT/REPLACE and rowsAffected for UPDATE/DELETE.
        if verb == 'INSERT' or verb == 'REPLACE' then
            if isBatchedParams(params) then
                local r = spacedb:executeMany(query, params)
                return r and (r.lastInsertId or r.insertId) or 0
            end
            local r = spacedb:execute(query, params)
            return r and (r.lastInsertId or r.insertId) or 0
        elseif verb == 'UPDATE' or verb == 'DELETE' then
            if isBatchedParams(params) then
                local r = spacedb:executeMany(query, params)
                return r and r.rowsAffected or 0
            end
            local r = spacedb:execute(query, params)
            return r and r.rowsAffected or 0
        end
        -- SELECT path: oxmysql smart-unwraps. Single row single column
        -- returns the scalar; single row multi-column returns the row;
        -- multi-row returns the array. Zero rows returns nil.
        local rows = spacedb:query(query, params or {}) or {}
        if #rows == 0 then return nil end
        if #rows == 1 then
            local row = rows[1]
            local n, single = 0, nil
            for _, v in pairs(row) do
                n = n + 1
                single = v
                if n > 1 then return row end
            end
            return single
        end
        return rows
    elseif method == 'rawExecute' then
        local r = spacedb:executeMany(query, params or {})
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
