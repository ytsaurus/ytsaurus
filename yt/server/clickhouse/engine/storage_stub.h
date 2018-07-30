#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <Storages/IStorage.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageStub(NInterop::TTablePtr table);

}   // namespace NClickHouse
}   // namespace NYT
