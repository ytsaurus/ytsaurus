#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions(
    IStoragePtr storage,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
