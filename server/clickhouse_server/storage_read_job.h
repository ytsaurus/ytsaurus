#pragma once

#include <yt/server/clickhouse_server/table.h>

#include <Storages/IStorage.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// TODO: ITablePartPtr instead of tables
DB::StoragePtr CreateStorageReadJob(
    IStoragePtr storage,
    std::vector<TTablePtr> tables,
    std::string jobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
