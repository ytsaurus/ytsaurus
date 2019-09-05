#pragma once

#include <yt/client/misc/public.h>

#include <Storages/IStorage_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemClique(
    TDiscoveryPtr discovery,
    std::string tableName,
    TString InstanceId_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
