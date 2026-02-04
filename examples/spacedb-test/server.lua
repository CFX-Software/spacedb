local tests = {}
local passed = 0
local failed = 0

local function log(message)
    print(('[spacedb-test] %s'):format(message))
end

local function encode(value)
    local ok, result = pcall(json.encode, value)
    if ok then
        return result
    end
    return tostring(value)
end

local function assertTrue(value, message)
    if not value then
        error(message or 'expected truthy value', 2)
    end
end

local function assertEquals(actual, expected, message)
    if actual ~= expected then
        error(('%s expected=%s actual=%s'):format(message or 'values did not match', encode(expected), encode(actual)), 2)
    end
end

local function test(name, fn)
    tests[#tests + 1] = { name = name, fn = fn }
end

local function db()
    return exports.spacedb
end

local function setup()
    local health = db():health()
    db():execute('DROP TABLE IF EXISTS spacedb_test_items', {})

    if health.driver == 'mysql' or health.driver == 'mariadb' then
        db():execute([[
            CREATE TABLE spacedb_test_items (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                name TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ]], {})
        return
    end

    db():execute([[
        CREATE TABLE spacedb_test_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ]], {})
end

local function cleanup()
    db():execute('DROP TABLE IF EXISTS spacedb_test_items', {})
end

test('health', function()
    local health = db():health()
    assertTrue(health.ok, 'health.ok should be true')
    assertTrue(health.driver == 'postgres' or health.driver == 'mysql' or health.driver == 'mariadb', 'driver should be known')
end)

test('query select literal', function()
    local rows = db():query('SELECT 1 AS ok', {})
    assertEquals(#rows, 1, 'select should return one row')
    assertEquals(tonumber(rows[1].ok), 1, 'ok should equal 1')
end)

test('execute insert', function()
    local result = db():execute('INSERT INTO spacedb_test_items (name, score) VALUES (?, ?)', { 'alpha', 10 })
    assertTrue(result.rowsAffected >= 1, 'insert should affect rows')
end)

test('single row', function()
    local row = db():single('SELECT name, score FROM spacedb_test_items WHERE name = ?', { 'alpha' })
    assertEquals(row.name, 'alpha', 'name should match')
    assertEquals(tonumber(row.score), 10, 'score should match')
end)

test('prepared named query', function()
    local prepared = db():prepare('spacedb_test_by_name', 'SELECT name, score FROM spacedb_test_items WHERE name = ?')
    assertTrue(prepared.ok, 'prepare should return ok')

    local rows = db():query('spacedb_test_by_name', { 'alpha' })
    assertEquals(#rows, 1, 'prepared query should return one row')
    assertEquals(rows[1].name, 'alpha', 'prepared row name should match')
end)

test('transaction', function()
    local result = db():transaction({
        { mode = 'execute', query = 'INSERT INTO spacedb_test_items (name, score) VALUES (?, ?)', params = { 'beta', 20 } },
        { mode = 'single', query = 'SELECT name, score FROM spacedb_test_items WHERE name = ?', params = { 'beta' } }
    })

    assertEquals(#result, 2, 'transaction should return two step results')
    assertTrue(result[1].rowsAffected >= 1, 'transaction insert should affect rows')
    assertEquals(result[2].rows[1].name, 'beta', 'transaction query should see inserted row')
end)

test('stats', function()
    local stats = db():stats()
    assertTrue(stats.db ~= nil, 'stats.db should exist')
    assertTrue(stats.subscriptions ~= nil, 'stats.subscriptions should exist')
end)

test('subscription changed event', function()
    local received = nil
    local sub = db():subscribe('SELECT name, score FROM spacedb_test_items ORDER BY id', {}, function(event)
        received = event
    end)

    assertTrue(sub and sub.id, 'subscription id should exist')
    Wait(500)
    db():execute('INSERT INTO spacedb_test_items (name, score) VALUES (?, ?)', { 'gamma', 30 })

    local deadline = GetGameTimer() + 5000
    while not received and GetGameTimer() < deadline do
        Wait(100)
    end

    db():unsubscribe(sub.id)
    assertTrue(received ~= nil, 'subscription should receive an event')
    assertEquals(received.type, 'changed', 'subscription event should be changed')
end)

local function run()
    Wait(2000)
    log('starting')

    local ok, err = pcall(setup)
    if not ok then
        log(('setup failed: %s'):format(err))
        return
    end

    for _, item in ipairs(tests) do
        local testOk, testErr = pcall(item.fn)
        if testOk then
            passed = passed + 1
            log(('PASS %s'):format(item.name))
        else
            failed = failed + 1
            log(('FAIL %s: %s'):format(item.name, testErr))
        end
        Wait(100)
    end

    pcall(cleanup)
    log(('finished passed=%d failed=%d total=%d'):format(passed, failed, #tests))
end

CreateThread(run)
