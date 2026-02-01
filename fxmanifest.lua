fx_version 'cerulean'
game 'gta5'

author 'Inkwell'
description 'spacedb - high-performance database bridge for FiveM'
version '0.1.0'

server_scripts {
    'server/lua/spacedb.lua'
}

files {
    'config.example.json'
}

server_exports {
    'query',
    'single',
    'execute',
    'prepare',
    'transaction',
    'subscribe',
    'unsubscribe',
    'health',
    'stats'
}
