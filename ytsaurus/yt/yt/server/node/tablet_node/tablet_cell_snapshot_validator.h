#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NCellarAgent::ICellarOccupantPtr CreateFakeOccupant(
    NClusterNode::IBootstrapBase* bootstrap,
    TGuid cellId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
