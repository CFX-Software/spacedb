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

exports('query', query)
exports('single', single)
exports('scalar', scalar)
exports('execute', execute)
exports('insert', insert)
exports('update', update)
exports('prepare', prepare)
exports('transaction', transaction)
