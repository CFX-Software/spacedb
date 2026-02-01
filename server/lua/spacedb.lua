local resourceName = GetCurrentResourceName()
local endpoint = GetConvar('spacedb_endpoint', 'http://127.0.0.1:37120')
local subscriptions = {}

local function resourcePath(path)
    return GetResourcePath(resourceName) .. '/' .. path
end

local function readConfig()
    local file = LoadResourceFile(resourceName, 'config.json')
    if not file or file == '' then
        file = LoadResourceFile(resourceName, 'config.example.json')
    end
    return file or '{}'
end

local function startCore()
    if GetConvar('spacedb_manage_core', 'true') ~= 'true' then
        return
    end

    local binary = GetConvar('spacedb_core_path', '')
    if binary == '' then
        local suffix = package.config:sub(1, 1) == '\\' and '.exe' or ''
        binary = resourcePath('bin/spacedb-core' .. suffix)
    end

    if not io.open(binary, 'r') then
        print(('[spacedb] core binary not found at %s; build core/cmd/spacedb-core first'):format(binary))
        return
    end

    local configPath = resourcePath('config.json')
    if not io.open(configPath, 'r') then
        configPath = resourcePath('config.example.json')
    end

    local isWindows = package.config:sub(1, 1) == '\\'
    local command
    if isWindows then
        command = ('start "" /B "%s" -config "%s"'):format(binary, configPath)
    else
        command = ('"%s" -config "%s" >/dev/null 2>&1 &'):format(binary, configPath)
    end
    os.execute(command)
end

local function request(method, path, body, cb)
    local payload = body and json.encode(body) or ''
    PerformHttpRequest(endpoint .. path, function(status, response)
        local decoded = nil
        if response and response ~= '' then
            local ok, result = pcall(json.decode, response)
            if ok then decoded = result end
        end

        if status < 200 or status >= 300 then
            local err = decoded and decoded.error or ('HTTP ' .. tostring(status))
            cb(nil, err)
            return
        end

        if decoded and decoded.error then
            cb(nil, decoded.error)
            return
        end

        cb(decoded, nil)
    end, method, payload, {
        ['Content-Type'] = 'application/json'
    })
end

local function call(method, path, body, cb)
    if cb then
        request(method, path, body, cb)
        return nil
    end

    local p = promise.new()
    request(method, path, body, function(result, err)
        if err then
            p:reject(err)
        else
            p:resolve(result)
        end
    end)
    return Citizen.Await(p)
end

local function query(sqlOrName, params, cb)
    return call('POST', '/v1/query', { query = sqlOrName, params = params or {} }, cb)
end

local function single(sqlOrName, params, cb)
    return call('POST', '/v1/single', { query = sqlOrName, params = params or {} }, cb)
end

local function execute(sqlOrName, params, cb)
    return call('POST', '/v1/execute', { query = sqlOrName, params = params or {} }, cb)
end

local function prepare(name, sql, options, cb)
    return call('POST', '/v1/prepare', { name = name, sql = sql, options = options or {} }, cb)
end

local function transaction(steps, cb)
    return call('POST', '/v1/transaction', { steps = steps or {} }, cb)
end

local function pollSubscription(id, callback)
    CreateThread(function()
        while subscriptions[id] do
            request('GET', '/v1/events?id=' .. id, nil, function(result, err)
                if not err and result and result.events then
                    for _, event in ipairs(result.events) do
                        callback(event)
                    end
                end
            end)
            Wait(250)
        end
    end)
end

local function subscribe(sqlOrName, params, callback)
    local result = call('POST', '/v1/subscribe', { query = sqlOrName, params = params or {} })
    if result and result.id then
        subscriptions[result.id] = true
        if callback then
            pollSubscription(result.id, callback)
        end
    end
    return result
end

local function unsubscribe(id, cb)
    subscriptions[id] = nil
    return call('POST', '/v1/unsubscribe', { id = id }, cb)
end

local function health(cb)
    return call('GET', '/health', nil, cb)
end

local function stats(cb)
    return call('GET', '/v1/stats', nil, cb)
end

exports('query', query)
exports('single', single)
exports('execute', execute)
exports('prepare', prepare)
exports('transaction', transaction)
exports('subscribe', subscribe)
exports('unsubscribe', unsubscribe)
exports('health', health)
exports('stats', stats)

CreateThread(function()
    startCore()
    Wait(500)
    local cfg = readConfig()
    print(('[spacedb] resource ready; config bytes=%d endpoint=%s'):format(#cfg, endpoint))
end)

AddEventHandler('onResourceStop', function(stopped)
    if stopped ~= resourceName then return end
    for id in pairs(subscriptions) do
        request('POST', '/v1/unsubscribe', { id = id }, function() end)
    end
end)
