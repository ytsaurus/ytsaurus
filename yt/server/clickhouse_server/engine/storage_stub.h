#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageStub(NNative::TTablePtr table);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
