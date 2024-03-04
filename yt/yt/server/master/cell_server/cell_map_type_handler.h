#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateCellMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NCellarClient::ECellarType cellarType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
