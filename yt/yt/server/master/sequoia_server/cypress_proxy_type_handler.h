#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateCypressProxyTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TCypressProxyObject>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
