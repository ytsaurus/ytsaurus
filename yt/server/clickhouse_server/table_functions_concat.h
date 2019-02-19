#pragma once

#include "public.h"
#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions(IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
