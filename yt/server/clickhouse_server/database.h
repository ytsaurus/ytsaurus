#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/public.h>

#include <Databases/IDatabase.h>
#include <Interpreters/Cluster.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDatabase(
    IStoragePtr storage,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
