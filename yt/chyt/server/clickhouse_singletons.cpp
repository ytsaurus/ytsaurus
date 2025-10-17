#include "clickhouse_singletons.h"

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Dictionaries/registerDictionaries.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/registerInterpreters.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{
    void registerStorageMemory(StorageFactory& factory);
    void registerStorageBuffer(StorageFactory& factory);
    void registerStorageDictionary(StorageFactory& factory);
}

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterClickHouseSingletons()
{
    DB::registerInterpreters();
    DB::registerFormats();
    DB::registerFunctions();
    DB::registerAggregateFunctions();
    DB::registerTableFunctions(/*use_legacy_mongodb_integration*/ false);
    DB::registerStorageMemory(DB::StorageFactory::instance());
    DB::registerStorageBuffer(DB::StorageFactory::instance());
    DB::registerStorageDictionary(DB::StorageFactory::instance());
    DB::registerDictionaries(/*use_legacy_mongodb_integration*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
