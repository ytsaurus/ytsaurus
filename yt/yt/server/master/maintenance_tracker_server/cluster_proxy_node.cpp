#include "cluster_proxy_node.h"

namespace NYT::NMaintenanceTrackerServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

void TClusterProxyNode::Save(TSaveContext& context) const
{
    TMapNode::Save(context);
    TMaintenanceTarget::Save(context);
}

void TClusterProxyNode::Load(TLoadContext& context)
{
    // COMPAT(kvk1920):
    YT_VERIFY(context.GetVersion() >= EMasterReign::ProxyMaintenanceRequests);

    TMapNode::Load(context);
    TMaintenanceTarget::Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NMaintenanceTrackerServer
