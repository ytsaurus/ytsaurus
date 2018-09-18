#pragma once

#include "cluster_tracker.h"

namespace DB {

class IDatabase;

}   // namespace DB;

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

void AttachSystemTables(
    DB::IDatabase& system,
    IClusterNodeTrackerPtr clusterNodeTracker);

}   // namespace NClickHouse
}   // namespace NYT
