#pragma once

#include "private.h"

#include "subquery_spec.h"

#include <Storages/IStorage.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSubquery(
    TQueryContext* queryContext,
    TSubquerySpec subquerySpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
