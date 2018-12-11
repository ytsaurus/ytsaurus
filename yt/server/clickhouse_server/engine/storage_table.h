#pragma once

#include "clickhouse.h"

#include "cluster_tracker.h"

//#include <Interpreters/Cluster.h>
//#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(
    NNative::IStoragePtr storage,
    NNative::TTablePtr table,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
