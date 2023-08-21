#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

///////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateAccessControlObjectNamespaceTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TAccessControlObjectNamespace>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
