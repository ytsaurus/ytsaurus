#include "public.h"

#include <yt/yt/server/lib/cellar_agent/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NCellarAgent::ICellarOccupierProviderPtr CreateTabletSlotOccupierProvider(
    TTabletNodeConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
