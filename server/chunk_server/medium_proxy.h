#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateMediumProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TMedium* medium);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
