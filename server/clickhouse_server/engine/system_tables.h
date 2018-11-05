#pragma once

#include "cluster_tracker.h"

namespace DB {

class IDatabase;

}   // namespace DB;

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

void AttachSystemTables(
    DB::IDatabase& system,
    IClusterNodeTrackerPtr clusterNodeTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
