#pragma once

#include "cluster_tracker.h"

#include "table_reader.h"

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageConcat(
    std::vector<TClickHouseTablePtr> tables,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
