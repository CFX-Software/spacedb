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

local function report(name, count, durationMs)
    local perQuery = durationMs / count
    local qps = count / (durationMs / 1000)
    log(('%s count=%d totalMs=%d avgMs=%s qps=%s'):format(name, count, durationMs, round(perQuery), round(qps)))
end

local function awaitOxmysql(method, sql, params)
    local p = promise.new()
    exports.oxmysql[method](exports.oxmysql, sql, params or {}, function(result)
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

    runBatch('spacedb insert batch transaction', function()
        local rows = {}
        for i = 1, iterations do
            rows[i] = { 'native-batch', 1 }
        end
        exports.spacedb:executeMany('INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', rows)
    end, iterations)

    runConcurrent('spacedb insert concurrent', function()
        exports.spacedb:execute('INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'native-concurrent', 1 })
    end)

    runConcurrent('oxmysql real insert concurrent', function()
        awaitOxmysql('execute', 'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'real-concurrent', 1 })
    end)

    exports.spacedb:execute('DROP TABLE IF EXISTS spacedb_bench_items', {})
    log('finished')
end

CreateThread(run)
