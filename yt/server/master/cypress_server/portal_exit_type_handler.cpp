#include "portal_exit_type_handler.h"
#include "node_detail.h"
#include "portal_exit_proxy.h"
#include "portal_exit_node.h"
#include "portal_manager.h"

namespace NYT::NCypressServer {

using namespace NObjectClient;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TPortalExitTypeHandler
    : public TMapNodeTypeHandlerImpl<TPortalExitNode>
{
public:
    using TMapNodeTypeHandlerImpl::TMapNodeTypeHandlerImpl;

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::PortalExit;
    }

    virtual ETypeFlags GetFlags() const override
    {
        return ETypeFlags::ForbidInheritAclChange;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TPortalExitNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreatePortalExitProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual void DoDestroy(TPortalExitNode* node) override
    {
        TMapNodeTypeHandlerImpl::DoDestroy(node);

        if (node->IsTrunk()) {
            const auto& portalManager = Bootstrap_->GetPortalManager();
            portalManager->DestroyExitNode(node);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreatePortalExitTypeHandler(TBootstrap* bootstrap)
{
    return New<TPortalExitTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
