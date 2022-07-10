#pragma once

#include "public.h"
#include "type_handler.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateAccessControlObjectTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TAccessControlObject>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
