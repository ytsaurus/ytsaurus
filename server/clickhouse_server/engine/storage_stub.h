#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Storages/IStorage.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageStub(NNative::TTablePtr table);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
