#include "clickhouse_singletons.h"

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Dictionaries/registerDictionaries.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/SystemLog.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{
    void registerStorageMemory(StorageFactory& factory);
    void registerStorageBuffer(StorageFactory& factory);
}

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterClickHouseSingletons()
{
    DB::registerFormats();
    DB::registerFunctions();
    DB::registerAggregateFunctions();
    DB::registerTableFunctions();
    DB::registerStorageMemory(DB::StorageFactory::instance());
    DB::registerStorageBuffer(DB::StorageFactory::instance());
    DB::registerDictionaries();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
