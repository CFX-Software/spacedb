local iterations = tonumber(GetConvar('spacedb_bench_iterations', '1000')) or 1000
local concurrency = tonumber(GetConvar('spacedb_bench_concurrency', '50')) or 50

local function log(message)
    print(('[spacedb-bench] %s'):format(message))
end

local function now()
    return GetGameTimer()
end

local function elapsed(start)
    return now() - start
end

local function round(value)
    return math.floor(value * 100 + 0.5) / 100
end

local results = {}

local function report(name, count, durationMs)
    local perQuery = durationMs / count
    local qps = count / (durationMs / 1000)
    results[name] = {
        count = count,
        totalMs = durationMs,
        avgMs = perQuery,
        qps = qps
    }
    log(('%s count=%d totalMs=%d avgMs=%s qps=%s'):format(name, count, durationMs, round(perQuery), round(qps)))
end

local function pad(value, width)
    value = tostring(value)
    if #value >= width then
        return value
    end
    return value .. string.rep(' ', width - #value)
end

local function compare(label, spacedbName, oxmysqlName)
    local native = results[spacedbName]
    local real = results[oxmysqlName]
    if not native or not real then
        return nil
    end

    local faster = ((real.totalMs - native.totalMs) / real.totalMs) * 100
    local verdict = faster >= 0 and ('spacedb +' .. round(faster) .. '%') or ('spacedb ' .. round(faster) .. '%')
    return {
        label = label,
        spacedbMs = native.totalMs,
        oxmysqlMs = real.totalMs,
        spacedbQps = native.qps,
        oxmysqlQps = real.qps,
        verdict = verdict
    }
end

local function printSummary()
    local rows = {
        compare('query sequential', 'spacedb query sequential', 'oxmysql real query sequential'),
        compare('query concurrent', 'spacedb query concurrent', 'oxmysql real query concurrent'),
        compare('insert sequential', 'spacedb insert sequential', 'oxmysql real insert sequential'),
        compare('insert bulk multi-row', 'spacedb insert bulk multi-row', 'oxmysql real insert bulk multi-row'),
        compare('insert concurrent', 'spacedb insert concurrent', 'oxmysql real insert concurrent')
    }

    log('summary')
    log(pad('phase', 24) .. pad('spacedb ms', 13) .. pad('oxmysql ms', 13) .. pad('spacedb qps', 14) .. pad('oxmysql qps', 14) .. 'delta')
    for _, row in ipairs(rows) do
        if row then
            log(
                pad(row.label, 24) ..
                pad(row.spacedbMs, 13) ..
                pad(row.oxmysqlMs, 13) ..
                pad(round(row.spacedbQps), 14) ..
                pad(round(row.oxmysqlQps), 14) ..
                row.verdict
            )
        end
    end
end

local function awaitOxmysql(method, sql, params)
    local p = promise.new()
    exports.oxmysql[method](exports.oxmysql, sql, params or {}, function(result)
        p:resolve(result)
    end)
    return Citizen.Await(p)
end

local function awaitOxmysqlTransaction(queries)
    local p = promise.new()
    exports.oxmysql:transaction(queries, function(result)
        p:resolve(result)
    end)
    return Citizen.Await(p)
end

local function runSequential(name, fn)
    collectgarbage('collect')
    log(('phase started %s'):format(name))
    local start = now()
    for i = 1, iterations do
        fn()
        if i % 100 == 0 then
            log(('%s progress=%d/%d'):format(name, i, iterations))
            Wait(0)
        end
    end
    report(name, iterations, elapsed(start))
end

local function runConcurrent(name, fn)
    collectgarbage('collect')
    log(('phase started %s'):format(name))
    local completed = 0
    local start = now()

    for _ = 1, concurrency do
        CreateThread(function()
            local perWorker = math.floor(iterations / concurrency)
            for i = 1, perWorker do
                fn()
                if i % 25 == 0 then
                    Wait(0)
                end
            end
            completed = completed + 1
        end)
    end

    local deadline = now() + 60000
    local lastLogged = -1
    while completed < concurrency and now() < deadline do
        if completed > 0 and completed % 10 == 0 and completed ~= lastLogged then
            log(('%s workersComplete=%d/%d'):format(name, completed, concurrency))
            lastLogged = completed
        end
        Wait(0)
    end

    local count = math.floor(iterations / concurrency) * concurrency
    report(name, count, elapsed(start))
end

local function runBatch(name, fn, count)
    collectgarbage('collect')
    log(('phase started %s'):format(name))
    local start = now()
    fn()
    report(name, count, elapsed(start))
end

local function buildBulkInsert(name, count)
    local groups = {}
    local params = {}
    for i = 1, count do
        groups[i] = '(?, ?)'
        params[#params + 1] = name
        params[#params + 1] = 1
    end
    return 'INSERT INTO spacedb_bench_items (name, score) VALUES ' .. table.concat(groups, ','), params
end

local function setup()
    local health = exports.spacedb:health()
    exports.spacedb:execute('DROP TABLE IF EXISTS spacedb_bench_items', {})
    if health.driver == 'mysql' or health.driver == 'mariadb' then
        exports.spacedb:execute([[
            CREATE TABLE spacedb_bench_items (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                name TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0
            )
        ]], {})
        return
    end

    exports.spacedb:execute([[
        CREATE TABLE spacedb_bench_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0
        )
    ]], {})
end

local function run()
    Wait(2500)
    log(('starting iterations=%d concurrency=%d'):format(iterations, concurrency))
    setup()

    runSequential('spacedb query sequential', function()
        exports.spacedb:query('SELECT 1 AS ok', {})
    end)

    runSequential('oxmysql real query sequential', function()
        awaitOxmysql('query', 'SELECT 1 AS ok', {})
    end)

    runConcurrent('spacedb query concurrent', function()
        exports.spacedb:query('SELECT 1 AS ok', {})
    end)

    runConcurrent('oxmysql real query concurrent', function()
        awaitOxmysql('query', 'SELECT 1 AS ok', {})
    end)

    runSequential('spacedb insert sequential', function()
        exports.spacedb:execute('INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'native', 1 })
    end)

    runSequential('oxmysql real insert sequential', function()
        awaitOxmysql('execute', 'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'real', 1 })
    end)

    runBatch('spacedb insert bulk multi-row', function()
        local rows = {}
        for i = 1, iterations do
            rows[i] = { 'native-bulk', 1 }
        end
        exports.spacedb:executeMany('INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', rows)
    end, iterations)

    runBatch('oxmysql real insert batch transaction', function()
        local queries = {}
        for i = 1, iterations do
            queries[i] = {
                query = 'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)',
                values = { 'real-batch', 1 }
            }
        end
        awaitOxmysqlTransaction(queries)
    end, iterations)

    runBatch('oxmysql real insert bulk multi-row', function()
        for i = 1, iterations, 500 do
            local remaining = iterations - i + 1
            local count = math.min(500, remaining)
            local sql, params = buildBulkInsert('real-bulk', count)
            awaitOxmysql('execute', sql, params)
        end
    end, iterations)

    runConcurrent('spacedb insert concurrent', function()
        exports.spacedb:execute('INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'native-concurrent', 1 })
    end)

    runConcurrent('oxmysql real insert concurrent', function()
        awaitOxmysql('execute', 'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'real-concurrent', 1 })
    end)

    printSummary()
    exports.spacedb:execute('DROP TABLE IF EXISTS spacedb_bench_items', {})
    log('finished')
end

CreateThread(run)
