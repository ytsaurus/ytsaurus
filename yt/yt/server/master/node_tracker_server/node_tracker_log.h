#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

void LogNodeState(
    NCellMaster::TBootstrap* bootstrap,
    TNode* node,
    ENodeState state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
