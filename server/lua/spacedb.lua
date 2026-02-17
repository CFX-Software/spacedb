local resourceName = GetCurrentResourceName()
local endpoint = GetConvar('spacedb_endpoint', 'http://127.0.0.1:37120')
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
        binary = resourcePath('bin/spacedb-core.exe')
    end

    local configPath = resourcePath('config.json')
    if not LoadResourceFile(resourceName, 'config.json') then
        configPath = resourcePath('config.example.json')
    end

    local command = ('cmd /c start "" /B "%s" -config "%s"'):format(binary, configPath)
    if GetConvar('spacedb_core_platform', 'windows') == 'linux' then
        binary = GetConvar('spacedb_core_path', resourcePath('bin/spacedb-core'))
        command = ('"%s" -config "%s" >/dev/null 2>&1 &'):format(binary, configPath)
    end

    local ok, reason, code = os.execute(command)
    print(('[spacedb] core start command dispatched ok=%s reason=%s code=%s'):format(tostring(ok), tostring(reason), tostring(code)))
end

local function request(method, path, body, cb, attempt)
    attempt = attempt or 1
    local payload = body and json.encode(body) or ''
    PerformHttpRequest(endpoint .. path, function(status, response)
        local decoded = nil
        if response and response ~= '' then
            local ok, result = pcall(json.decode, response)
            if ok then decoded = result end
        end

        if status < 200 or status >= 300 then
            if status == 0 and attempt < 25 then
                CreateThread(function()
                    Wait(200)
                    request(method, path, body, cb, attempt + 1)
                end)
                return
            end

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

CreateThread(function()
    startCore()
    local cfg = readConfig()
    request('GET', '/health', nil, function(result, err)
        if err then
            print(('[spacedb] core not reachable after startup: %s endpoint=%s'):format(err, endpoint))
            return
        end

        print(('[spacedb] core ready driver=%s config bytes=%d endpoint=%s'):format(result.driver or 'unknown', #cfg, endpoint))
    end)
end)

AddEventHandler('onResourceStop', function(stopped)
    if stopped ~= resourceName then return end
end)
