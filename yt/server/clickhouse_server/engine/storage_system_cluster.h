#pragma once

#include "clickhouse.h"

//#include <Storages/IStorage.h>

#include "cluster_tracker.h"

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemCluster(
    IClusterNodeTrackerPtr clusterNodeTracker,
    std::string tableName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
