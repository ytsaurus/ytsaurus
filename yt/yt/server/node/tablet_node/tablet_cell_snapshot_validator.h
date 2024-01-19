#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NCellarAgent::ICellarOccupantPtr CreateFakeOccupant(
    NClusterNode::IBootstrapBase* bootstrap,
    TCellId cellId,
    NApi::TClusterTag clockClusterTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
