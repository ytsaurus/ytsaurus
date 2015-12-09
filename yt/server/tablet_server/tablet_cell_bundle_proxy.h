#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateTabletCellBundleProxy(
    NCellMaster::TBootstrap* bootstrap,
    TTabletCellBundle* bundle);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

