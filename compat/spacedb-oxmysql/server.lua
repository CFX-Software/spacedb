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

local function query(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    local result = exports.spacedb:query(sql, params)
    return callbackOrReturn(result, cb)
end

local function single(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    local result = exports.spacedb:single(sql, params)
    return callbackOrReturn(result, cb)
end

local function scalar(sql, params, cb)
    params, cb = normalizeParams(params, cb)
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
    params, cb = normalizeParams(params, cb)
    local result = exports.spacedb:execute(sql, params)
    return callbackOrReturn(result.rowsAffected or 0, cb)
end

local function insert(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    local result = exports.spacedb:execute(sql, params)
    return callbackOrReturn(result.lastInsertId or result.insertId or 0, cb)
end

local function update(sql, params, cb)
    params, cb = normalizeParams(params, cb)
    local result = exports.spacedb:execute(sql, params)
    return callbackOrReturn(result.rowsAffected or 0, cb)
end

local function prepare(nameOrSql, sqlOrParams, maybeOptions, maybeCb)
    if type(sqlOrParams) == 'string' then
        local result = exports.spacedb:prepare(nameOrSql, sqlOrParams, maybeOptions or {})
        return callbackOrReturn(result, maybeCb)
    end

    local result = exports.spacedb:query(nameOrSql, sqlOrParams or {})
    return callbackOrReturn(result, maybeOptions)
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
        if type(item) == 'string' then
            steps[#steps + 1] = {
                mode = 'execute',
                query = item,
                params = sharedParams or {}
            }
        else
            steps[#steps + 1] = {
                mode = item.mode or 'execute',
                query = item.query,
                params = item.values or item.params or item.parameters or {}
            }
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
    local upper = sql:gsub('^%s+', ''):sub(1, 6):upper()
    if upper == 'SELECT' then
        local first = rows[1] or {}
        local result = exports.spacedb:query(sql, first)
        return callbackOrReturn(result, cb)
    end
    local result = exports.spacedb:executeMany(sql, rows)
    return callbackOrReturn(result, cb)
end

exports('query', query)
exports('single', single)
exports('scalar', scalar)
exports('execute', execute)
exports('insert', insert)
exports('update', update)
exports('prepare', prepare)
exports('transaction', transaction)
exports('rawExecute', rawExecute)

-- _async aliases: OxMySQL exposes promise variants under <fn>_async.
-- spacedb exports already return synchronously (the JS bridge yields the
-- caller via FiveM scheduler), so callers awaiting the promise on the JS
-- side get the same value. Lua callers receive the value directly.
exports('query_async', query)
exports('single_async', single)
exports('scalar_async', scalar)
exports('execute_async', execute)
exports('insert_async', insert)
exports('update_async', update)
exports('prepare_async', prepare)
exports('transaction_async', transaction)
exports('rawExecute_async', rawExecute)
