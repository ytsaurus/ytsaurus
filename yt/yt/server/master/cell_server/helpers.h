#pragma once

#include "public.h"

#include <yt/yt/server/master/cypress_server/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

int GetCellShardIndex(NHydra::TCellId cellId);

// COMPAT(danilalexeev)
TEnumIndexedArray<NCellarClient::ECellarType, bool> CheckLegacyCellMapNodeTypesOrThrow(
    const NCypressServer::ICypressManagerPtr& cypressManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
