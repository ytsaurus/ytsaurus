#pragma once

#include "private.h"

#include "table.h"

#include <Storages/IStorage.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// TODO: ITablePartPtr instead of tables
DB::StoragePtr CreateStorageSubquery(
    TQueryContext* queryContext,
    std::string subquerySpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
