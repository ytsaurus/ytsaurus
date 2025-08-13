#include "cluster_proxy_node.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NMaintenanceTrackerServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

void TClusterProxyNode::Save(TSaveContext& context) const
{
    TCypressMapNode::Save(context);
    TMaintenanceTarget::Save(context);
}

void TClusterProxyNode::Load(TLoadContext& context)
{
    TCypressMapNode::Load(context);
    TMaintenanceTarget::Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
