#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <Databases/IDatabase.h>
#include <Interpreters/Cluster.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDatabase(
    NInterop::IStoragePtr storage,
    IExecutionClusterPtr cluster);

}   // namespace NClickHouse
}   // namespace NYT
