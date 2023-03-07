#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateMediumProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TMedium* medium);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
