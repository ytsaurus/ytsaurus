#pragma once

#include <Storages/IStorage.h>

#include <yt/client/misc/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemClique(
    TDiscoveryPtr discovery,
    std::string tableName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
