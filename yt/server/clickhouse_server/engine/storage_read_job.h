#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/table_schema.h>

//#include <Storages/IStorage.h>

#include <string>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

// TODO: NNative::ITablePartPtr instead of tables
DB::StoragePtr CreateStorageReadJob(
    NNative::IStoragePtr storage,
    NNative::TTableList tables,
    std::string jobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
