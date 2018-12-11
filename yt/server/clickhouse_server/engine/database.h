#pragma once

#include "clickhouse.h"

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/native/public.h>

//#include <Databases/IDatabase.h>
//#include <Interpreters/Cluster.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDatabase(
    NNative::IStoragePtr storage,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
