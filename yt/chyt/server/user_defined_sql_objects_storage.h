#pragma once

#include "private.h"

#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Interpreters/Context_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUserDefinedSQLObjectsStorage> CreateUserDefinedSqlObjectsYTStorage(
    DB::ContextPtr globalContext,
    TUserDefinedSqlObjectsStorageConfigPtr config,
    THost* host);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
