#pragma once

#include <yt/client/misc/public.h>

#include <Databases/IDatabase.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void AttachSystemTables(
    DB::IDatabase& system,
    TDiscoveryPtr clusterNodeTracker,
    const TString& instanceId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
