#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(
    NInterop::IStoragePtr storage,
    NInterop::TTablePtr table,
    IExecutionClusterPtr cluster);

} // namespace NClickHouse
} // namespace NYT
