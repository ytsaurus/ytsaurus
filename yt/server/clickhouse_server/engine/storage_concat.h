#pragma once

#include "cluster_tracker.h"

#include <yt/server/clickhouse_server/native/table_reader.h>

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageConcat(
    NNative::IStoragePtr storage,
    NNative::TTableList tables,
    IExecutionClusterPtr cluster);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
