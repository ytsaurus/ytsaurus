#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <Storages/IStorage.h>

#include <string>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

// TODO: NInterop::ITablePartPtr instead of tables
DB::StoragePtr CreateStorageReadJob(
    NInterop::IStoragePtr storage,
    NInterop::TTableList tables,
    std::string jobSpec);

}   // namespace NClickHouse
}   // namespace NYT
