fx_version 'cerulean'
game 'gta5'

author 'Inkwell'
description 'spacedb OxMySQL compatibility adapter'
version '0.1.0'

dependency 'spacedb'

server_script 'server.lua'

server_exports {
    'query',
    'single',
    'scalar',
    'execute',
    'insert',
    'update',
    'prepare',
    'transaction'
}
