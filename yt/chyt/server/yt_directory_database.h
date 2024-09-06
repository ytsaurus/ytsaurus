#pragma once

#include <yt/yt/core/ypath/public.h>

#include <Databases/IDatabase.h>
#include <base/types.h>


namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDirectoryDatabase(String databaseName, NYPath::TYPath root);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
