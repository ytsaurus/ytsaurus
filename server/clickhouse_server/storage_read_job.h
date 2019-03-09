#pragma once

#include "private.h"

#include "table.h"

#include <Storages/IStorage.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// TODO: ITablePartPtr instead of tables
DB::StoragePtr CreateStorageReadJob(
    TQueryContext* queryContext,
    std::vector<TClickHouseTablePtr> tables,
    std::string jobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
