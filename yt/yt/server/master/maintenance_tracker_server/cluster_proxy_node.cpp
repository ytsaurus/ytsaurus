#include "cluster_proxy_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NMaintenanceTrackerServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

void TClusterProxyNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressMapNode>();
    registrar.template BaseType<TNontemplateMaintenanceTargetBase>();
}

PHOENIX_DEFINE_TYPE(TClusterProxyNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
