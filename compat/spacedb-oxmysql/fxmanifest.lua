fx_version 'cerulean'
game 'common'
lua54 'yes'

name 'oxmysql'
author 'Inkwell'
version '0.2.0'
description 'spacedb oxmysql / mysql-async / ghmattimysql compatibility shim'

dependency 'spacedb'

provide 'mysql-async'
provide 'ghmattimysql'

server_script 'server.lua'

files {
    'lib/MySQL.lua',
}

server_exports {
    'query',
    'single',
    'scalar',
    'execute',
    'insert',
    'update',
    'prepare',
    'transaction',
    'rawExecute',
    'query_async',
    'single_async',
    'scalar_async',
    'execute_async',
    'insert_async',
    'update_async',
    'prepare_async',
    'transaction_async',
    'rawExecute_async'
}
