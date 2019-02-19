#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/table_reader.h>

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageConcat(
    std::vector<TTablePtr> tables,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
