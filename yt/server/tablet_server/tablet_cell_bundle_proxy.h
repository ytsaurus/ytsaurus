#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateTabletCellBundleProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TTabletCellBundle* cellBundle);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

