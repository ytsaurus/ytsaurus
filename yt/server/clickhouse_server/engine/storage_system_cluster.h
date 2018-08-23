#pragma once

#include <Storages/IStorage.h>

#include "cluster_tracker.h"

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemCluster(
    IClusterNodeTrackerPtr clusterNodeTracker,
    std::string tableName);

}   // namespace NClickHouse
}   // namespace NYT
