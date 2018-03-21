#pragma once

#include "public.h"

#include <yp/server/master/public.h>

#include <yt/core/rpc/public.h>

namespace NYP {
namespace NServer {
namespace NNodes {

////////////////////////////////////////////////////////////////////////////////

NYT::NRpc::IServicePtr CreateNodeTrackerService(
    NMaster::TBootstrap* bootstrap,
    TNodeTrackerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP
