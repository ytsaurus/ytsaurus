#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobTrackerService(NCellMaster::TBootstrap* boostrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
