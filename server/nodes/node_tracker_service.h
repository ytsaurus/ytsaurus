#pragma once

#include "public.h"

#include <yp/server/master/public.h>

#include <yt/core/rpc/public.h>

namespace NYP::NServer::NNodes {

////////////////////////////////////////////////////////////////////////////////

NYT::NRpc::IServicePtr CreateNodeTrackerService(
    NMaster::TBootstrap* bootstrap,
    TNodeTrackerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes
