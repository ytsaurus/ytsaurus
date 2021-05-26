#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

NCellarAgent::ICellarOccupierProviderPtr CreateChaosCellarOccupierProvider(
    TChaosNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChaosNode::NYT
