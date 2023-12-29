#pragma once

#include "private.h"

#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Interpreters/Context_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUserDefinedSQLObjectsStorage> CreateUserDefinedSQLObjectsYTStorage(
    DB::ContextPtr globalContext,
    TUserDefinedSQLObjectsStorageConfigPtr config,
    THost* host);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
