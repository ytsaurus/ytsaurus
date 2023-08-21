#pragma once

#include <yt/yt/library/clickhouse_discovery/public.h>

#include <Storages/IStorage_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemClique(
    IDiscoveryPtr discovery,
    TGuid InstanceId_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
