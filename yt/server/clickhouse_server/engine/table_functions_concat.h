#pragma once

#include "clickhouse.h"

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/native/public.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions(
    NNative::IStoragePtr storage,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
