#pragma once

#include "cluster_tracker.h"

namespace DB {

class IDatabase;

}   // namespace DB;

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void AttachSystemTables(
    DB::IDatabase& system,
    IClusterNodeTrackerPtr clusterNodeTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
