#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateTabletProxy(
    NCellMaster::TBootstrap* bootstrap,
    TTablet* tablet);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

