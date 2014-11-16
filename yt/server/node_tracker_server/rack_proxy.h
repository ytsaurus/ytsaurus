#pragma once

#include "public.h"

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NNodeTrackerServer {

///////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateRackProxy(
    NCellMaster::TBootstrap* bootstrap,
    TRack* rack);

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
