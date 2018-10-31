#pragma once

#include "cluster_tracker.h"

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(
    NNative::IStoragePtr storage,
    NNative::TTablePtr table,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
