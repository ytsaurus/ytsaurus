#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_server/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

NTabletServer::IReplicatedTableTrackerHostPtr CreateReplicatedTableTrackerHost(IChaosSlotPtr slot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
