#include "public.h"

#include <yt/yt/server/lib/cellar_agent/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

NCellarAgent::ICellarOccupierProviderPtr CreateChaosCellarOccupierProvider(
    TChaosNodeConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChaosNode::NYT
