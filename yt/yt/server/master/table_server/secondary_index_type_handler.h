#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateSecondaryIndexTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TSecondaryIndex>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
