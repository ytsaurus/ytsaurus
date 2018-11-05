#pragma once

#include <Storages/IStorage.h>

#include "cluster_tracker.h"

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemCluster(
    IClusterNodeTrackerPtr clusterNodeTracker,
    std::string tableName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
