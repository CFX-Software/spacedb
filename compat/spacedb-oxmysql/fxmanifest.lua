fx_version 'cerulean'
game 'common'
lua54 'yes'

name 'oxmysql'
author 'Inkwell'
version '0.2.4'
description 'spacedb oxmysql / mysql-async / ghmattimysql compatibility shim'

dependency 'spacedb'

provide 'mysql-async'
provide 'ghmattimysql'

server_script 'server.lua'

files {
    'lib/MySQL.lua',
}

server_exports {
    'query',          'query_async',          'querySync',
    'single',         'single_async',         'singleSync',
    'scalar',         'scalar_async',         'scalarSync',
    'execute',        'execute_async',        'executeSync',
    'insert',         'insert_async',         'insertSync',
    'update',         'update_async',         'updateSync',
    'prepare',        'prepare_async',        'prepareSync',
    'transaction',    'transaction_async',    'transactionSync',
    'rawExecute',     'rawExecute_async',     'rawExecuteSync',
    'fetch',          'fetch_async',          'fetchSync',
    'store',          'store_async',          'storeSync',
    'isReady',        'isReady_async',        'isReadySync',
    'awaitConnection','awaitConnection_async','awaitConnectionSync',
    'startTransaction',
}
