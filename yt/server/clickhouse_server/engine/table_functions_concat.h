#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/native/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions(
    NNative::IStoragePtr storage,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
