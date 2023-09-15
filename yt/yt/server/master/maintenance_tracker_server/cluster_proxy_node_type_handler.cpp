#include "cluster_proxy_node_type_handler.h"

#include "cluster_proxy_node.h"
#include "cluster_proxy_node_proxy.h"

namespace NYT::NMaintenanceTrackerServer {

using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////////////////////////

class TClusterProxyNodeTypeHandler
    : public TMapNodeTypeHandlerImpl<TClusterProxyNode>
{
public:
    using TMapNodeTypeHandlerImpl::TMapNodeTypeHandlerImpl;

    EObjectType GetObjectType() const override
    {
        return EObjectType::ClusterProxyNode;
    }

    ICypressNodeProxyPtr DoGetProxy(
        TClusterProxyNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateClusterProxyNodeProxy(
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode);
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateClusterProxyNodeTypeHandler(
    NCellMaster::TBootstrap* bootstrap)
{
    return New<TClusterProxyNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

} // namespace NTY::NMaintenanceTracker
