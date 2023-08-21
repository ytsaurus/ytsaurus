#pragma once

#include "public.h"

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NChaosClient::TReplicationProgress GatherReplicationProgress(const NTableServer::TTableNode* table);

void ScatterReplicationProgress(NTableServer::TTableNode* table, NChaosClient::TReplicationProgress progress);

NTableClient::TLegacyKey GetTabletReplicationProgressPivotKey(
    TTablet* tablet,
    int tabletIndex,
    std::vector<NTableClient::TLegacyOwningKey>* buffer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
