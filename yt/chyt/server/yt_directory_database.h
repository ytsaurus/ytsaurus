#pragma once

#include "private.h"

#include <yt/yt/core/ypath/public.h>

#include <Databases/IDatabase.h>
#include <base/types.h>


namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDirectoryDatabase(String databaseName, THost* host, NYPath::TYPath root);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
