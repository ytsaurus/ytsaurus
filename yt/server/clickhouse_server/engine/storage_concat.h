#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/native/table_reader.h>

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageConcat(
    NNative::IStoragePtr storage,
    NNative::TTableList tables,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
