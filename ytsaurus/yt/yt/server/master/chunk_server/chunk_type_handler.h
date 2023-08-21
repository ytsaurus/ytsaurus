#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/type_handler.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateChunkTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
