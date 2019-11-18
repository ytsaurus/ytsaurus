#pragma once

#include "private.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateNetworkProjectProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TNetworkProject* networkProject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
