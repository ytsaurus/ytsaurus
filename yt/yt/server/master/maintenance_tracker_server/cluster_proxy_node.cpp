#include "cluster_proxy_node.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NMaintenanceTrackerServer {

using namespace NCellMaster;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

void TClusterProxyNode::Save(TSaveContext& context) const
{
    TCypressMapNode::Save(context);
    TMaintenanceTarget::Save(context);
}

void TClusterProxyNode::Load(TLoadContext& context)
{
    // COMPAT(kvk1920):
    YT_VERIFY(context.GetVersion() >= EMasterReign::ProxyMaintenanceRequests);

    TCypressMapNode::Load(context);
    TMaintenanceTarget::Load(context);

    if (context.GetVersion() < EMasterReign::RemoveStuckAttributes && TObject::Attributes_) {
        TObject::Attributes_->Remove(EInternedAttributeKey::MaintenanceRequests.Unintern());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
