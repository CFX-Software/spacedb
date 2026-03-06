-- spacedb Lua bridge: thin consumer.
-- Core lifecycle (spawn, kill stale, wait for ready) is owned by
-- server/js/spacedb.js via Node child_process. Lua just calls the JS
-- exports, which gate every call on the core-ready promise.

local resourceName = GetCurrentResourceName()

AddEventHandler('onResourceStop', function(stopped)
    if stopped ~= resourceName then return end
end)
