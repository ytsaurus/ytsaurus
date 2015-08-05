#pragma once

#include "public.h"

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateTabletCellBundleProxy(
    NCellMaster::TBootstrap* bootstrap,
    TTabletCellBundle* bundle);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

