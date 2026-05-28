fx_version 'cerulean'
game 'gta5'

author 'Inkwell'
description 'spacedb - high-performance database bridge for FiveM'
version '0.2.9'

server_scripts {
    'server/lua/spacedb.lua',
    'server/js/spacedb.js'
}

files {
    'config.example.json'
}

server_exports {
    'query',
    'single',
    'execute',
    'executeMany',
    'prepare',
    'transaction',
    'subscribe',
    'unsubscribe',
    'health',
    'stats',
    'executeProfiled',
    'queryProfiled',
    'getById',
    'getMany',
    'setById',
    'invalidate',
    'invalidateTable',
    'cacheStats'
}
