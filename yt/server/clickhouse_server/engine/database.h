#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/native/public.h>

#include <Databases/IDatabase.h>
#include <Interpreters/Cluster.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDatabase(
    NNative::IStoragePtr storage,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
