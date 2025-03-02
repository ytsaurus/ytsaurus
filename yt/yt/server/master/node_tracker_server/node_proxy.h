#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateClusterNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TNode* node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
