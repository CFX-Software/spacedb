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

-- Hi-res sample clock: os.clock() returns seconds with sub-ms resolution on
-- most platforms. GetGameTimer() is ms-only and fine for wall clock but loses
-- signal on fast calls.
local function sampleNow()
    return os.clock() * 1000
end

local function percentile(sorted, p)
    local n = #sorted
    if n == 0 then return 0 end
    local idx = math.ceil(p * n)
    if idx < 1 then idx = 1 end
    if idx > n then idx = n end
    return sorted[idx]
end

local results = {}

local function report(name, count, durationMs, samples)
    local perQuery = durationMs / count
    local qps = count / (durationMs / 1000)
    local entry = {
        count = count,
        totalMs = durationMs,
        avgMs = perQuery,
        qps = qps
    }
    if samples and #samples > 0 then
        table.sort(samples)
        entry.p50 = percentile(samples, 0.50)
        entry.p95 = percentile(samples, 0.95)
        entry.p99 = percentile(samples, 0.99)
        entry.max = samples[#samples]
        log(('%s count=%d totalMs=%d avgMs=%s qps=%s p50=%s p95=%s p99=%s max=%s'):format(
            name, count, durationMs, round(perQuery), round(qps),
            round(entry.p50), round(entry.p95), round(entry.p99), round(entry.max)))
    else
        log(('%s count=%d totalMs=%d avgMs=%s qps=%s'):format(name, count, durationMs, round(perQuery), round(qps)))
    end
    results[name] = entry
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
        compare('insert concurrent', 'spacedb insert concurrent', 'oxmysql real insert concurrent'),
        compare('get-by-id (cache vs ox)', 'spacedb getById cache hit', 'oxmysql real single by id (no cache)')
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
    local samples = {}
    local start = now()
    for i = 1, iterations do
        local s = sampleNow()
        fn()
        samples[i] = sampleNow() - s
        if i % 100 == 0 then
            log(('%s progress=%d/%d'):format(name, i, iterations))
            Wait(0)
        end
    end
    report(name, iterations, elapsed(start), samples)
end

local function runConcurrent(name, fn)
    collectgarbage('collect')
    log(('phase started %s'):format(name))
    local completed = 0
    local samples = {}
    local start = now()

    for _ = 1, concurrency do
        CreateThread(function()
            local perWorker = math.floor(iterations / concurrency)
            for i = 1, perWorker do
                local s = sampleNow()
                fn()
                samples[#samples + 1] = sampleNow() - s
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
    report(name, count, elapsed(start), samples)
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

local function waitForHealth(timeoutMs)
    local deadline = now() + timeoutMs
    while now() < deadline do
        local ok, health = pcall(function() return exports.spacedb:health() end)
        if ok and health and health.driver then
            log(('core ready driver=%s'):format(health.driver))
            return true
        end
        Wait(200)
    end
    return false
end

local function warmup()
    -- Cold DB pages, unprepared statements, cold JIT in the JS bridge add 3x
    -- variance to first-phase numbers. Run a small throwaway batch against
    -- both spacedb and oxmysql so every measured phase starts hot.
    log('phase started warmup (untimed)')
    for _ = 1, 100 do
        exports.spacedb:query('SELECT 1 AS ok', {})
        exports.spacedb:execute('INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'warmup', 1 })
    end
    for _ = 1, 100 do
        awaitOxmysql('query', 'SELECT 1 AS ok', {})
        awaitOxmysql('execute', 'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'warmup', 1 })
    end
    exports.spacedb:execute('DELETE FROM spacedb_bench_items WHERE name = ?', { 'warmup' })
    log('warmup complete')
end

local function run()
    if not waitForHealth(30000) then
        log('FATAL core /health did not respond within 30s; aborting bench')
        return
    end
    log(('starting iterations=%d concurrency=%d'):format(iterations, concurrency))
    setup()
    warmup()

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

    -- Profile phase: same sequential workload, but each call returns timing
    -- breakdown. Lua total, JS bridge round trip, server total, DB exec, and
    -- derived overheads. Identifies the dominant cost band per Step 1 plan.
    do
        collectgarbage('collect')
        local name = 'spacedb insert sequential profile'
        log(('phase started %s'):format(name))
        local profIters = math.min(iterations, 500)
        local luaTotal = {}
        local bridge = {}
        local serverTotal = {}
        local serverDispatch = {}
        local dbExec = {}
        local start = now()
        for i = 1, profIters do
            local t0 = sampleNow()
            local meta = exports.spacedb:executeProfiled(
                'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)',
                { 'native-profile', 1 }
            )
            local t1 = sampleNow()
            luaTotal[i] = t1 - t0
            if meta and meta.profile then
                bridge[i] = (meta.bridgeNs or 0) / 1e6
                serverTotal[i] = (meta.profile.serverTotalNs or 0) / 1e6
                serverDispatch[i] = (meta.profile.dispatchNs or 0) / 1e6
                dbExec[i] = (meta.profile.dbDurNs or 0) / 1e6
            end
            if i % 100 == 0 then
                log(('%s progress=%d/%d'):format(name, i, profIters))
                Wait(0)
            end
        end
        local totalMs = elapsed(start)

        local function describe(label, samples)
            if #samples == 0 then
                log(('  %-22s no samples'):format(label))
                return
            end
            table.sort(samples)
            local sum = 0
            for _, v in ipairs(samples) do sum = sum + v end
            local avg = sum / #samples
            log(('  %-22s avg=%s p50=%s p95=%s p99=%s max=%s'):format(
                label, round(avg),
                round(percentile(samples, 0.50)),
                round(percentile(samples, 0.95)),
                round(percentile(samples, 0.99)),
                round(samples[#samples])))
        end

        log(('%s count=%d totalMs=%d (per-stage ms)'):format(name, profIters, totalMs))
        describe('lua-total', luaTotal)
        describe('bridge-rtt', bridge)
        describe('server-total', serverTotal)
        describe('server-dispatch', serverDispatch)
        describe('db-exec', dbExec)

        -- Derived bands: where the time goes.
        local function derive(a, b)
            local n = math.min(#a, #b)
            local out = {}
            for i = 1, n do out[i] = math.max(0, a[i] - b[i]) end
            return out
        end
        describe('derived: lua→jsBridge', derive(luaTotal, bridge))
        describe('derived: jsBridge→server', derive(bridge, serverTotal))
        describe('derived: server→db', derive(serverTotal, dbExec))
    end

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

    -- Concurrent profile: same workload, profile flag on. Reveals whether
    -- the p99 tail is DB-bound (driver contention) or server-side queueing.
    do
        collectgarbage('collect')
        local name = 'spacedb insert concurrent profile'
        log(('phase started %s'):format(name))
        local profIters = math.min(iterations, 500)
        local profWorkers = math.min(concurrency, 25)
        local perWorker = math.floor(profIters / profWorkers)
        local luaTotal = {}
        local bridge = {}
        local serverTotal = {}
        local serverDispatch = {}
        local dbExec = {}
        local completed = 0
        local start = now()
        for _ = 1, profWorkers do
            CreateThread(function()
                for i = 1, perWorker do
                    local t0 = sampleNow()
                    local meta = exports.spacedb:executeProfiled(
                        'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)',
                        { 'native-concurrent-profile', 1 }
                    )
                    local t1 = sampleNow()
                    luaTotal[#luaTotal + 1] = t1 - t0
                    if meta and meta.profile then
                        bridge[#bridge + 1] = (meta.bridgeNs or 0) / 1e6
                        serverTotal[#serverTotal + 1] = (meta.profile.serverTotalNs or 0) / 1e6
                        serverDispatch[#serverDispatch + 1] = (meta.profile.dispatchNs or 0) / 1e6
                        dbExec[#dbExec + 1] = (meta.profile.dbDurNs or 0) / 1e6
                    end
                    if i % 25 == 0 then Wait(0) end
                end
                completed = completed + 1
            end)
        end
        local deadline = now() + 60000
        while completed < profWorkers and now() < deadline do Wait(0) end
        local totalMs = elapsed(start)
        local count = perWorker * profWorkers

        local function describe(label, samples)
            if #samples == 0 then return end
            table.sort(samples)
            local sum = 0
            for _, v in ipairs(samples) do sum = sum + v end
            log(('  %-22s avg=%s p50=%s p95=%s p99=%s max=%s'):format(
                label, round(sum / #samples),
                round(percentile(samples, 0.50)),
                round(percentile(samples, 0.95)),
                round(percentile(samples, 0.99)),
                round(samples[#samples])))
        end

        log(('%s count=%d totalMs=%d workers=%d (per-stage ms)'):format(name, count, totalMs, profWorkers))
        describe('lua-total', luaTotal)
        describe('bridge-rtt', bridge)
        describe('server-total', serverTotal)
        describe('server-dispatch', serverDispatch)
        describe('db-exec', dbExec)
    end

    runConcurrent('oxmysql real insert concurrent', function()
        awaitOxmysql('execute', 'INSERT INTO spacedb_bench_items (name, score) VALUES (?, ?)', { 'real-concurrent', 1 })
    end)

    -- Get-by-id workload. Three runners do the SAME logical operation
    -- (look up one row by primary key) — spacedb with its in-process
    -- cache, spacedb without (single() path), and OxMySQL which has no
    -- in-process cache at all and pays the full MySQL RTT each call.
    -- The cache vs OxMySQL number is the headline competitive win.
    exports.spacedb:execute('INSERT INTO spacedb_bench_items (id, name, score) VALUES (?, ?, ?)', { 999999, 'cache-target', 42 })
    exports.spacedb:getById('spacedb_bench_items', 999999) -- prime
    runSequential('spacedb getById cache hit', function()
        exports.spacedb:getById('spacedb_bench_items', 999999)
    end)
    runSequential('spacedb single by id (no cache)', function()
        exports.spacedb:single('SELECT * FROM spacedb_bench_items WHERE id = ?', { 999999 })
    end)
    runSequential('oxmysql real single by id (no cache)', function()
        awaitOxmysql('single', 'SELECT * FROM spacedb_bench_items WHERE id = ?', { 999999 })
    end)
    local cacheStats = exports.spacedb:cacheStats()
    log(('cache stats hits=%d misses=%d entries=%d'):format(
        cacheStats.hits or 0, cacheStats.misses or 0, cacheStats.entries or 0))

    printSummary()
    exports.spacedb:execute('DROP TABLE IF EXISTS spacedb_bench_items', {})
    log('finished')
end

CreateThread(run)
