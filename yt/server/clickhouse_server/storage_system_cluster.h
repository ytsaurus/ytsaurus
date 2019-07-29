#pragma once

#include <Storages/IStorage.h>

#include "cluster_tracker.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemCluster(
    IClusterNodeTrackerPtr clusterNodeTracker,
    std::string tableName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
