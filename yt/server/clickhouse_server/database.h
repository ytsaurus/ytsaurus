#pragma once

#include "cluster_tracker.h"

#include "private.h"

#include <Databases/IDatabase.h>
#include <Interpreters/Cluster.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDatabase(IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
