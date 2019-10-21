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
        return
            ETypeFlags::ForbidInheritAclChange |
            ETypeFlags::ForbidAnnotationRemoval;
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

    virtual void DoBeginCopy(
        TPortalExitNode* node,
        TBeginCopyContext* context) override
    {
        // NB: Portal exits _must_ be snapshot-wise compatible with map nodes
        // due to type erasure in TNontemplateCypressNodeTypeHandlerBase::BeginCopyCore.
        TMapNodeTypeHandlerImpl::DoBeginCopy(node, context);
    }

    virtual void DoEndCopy(
        TPortalExitNode* /*trunkNode*/,
        TEndCopyContext* /*context*/,
        ICypressNodeFactory* /*factory*/) override
    {
        // Should not actually happen.
        THROW_ERROR_EXCEPTION("Portal exits cannot be materialized during cross-cell cloning");
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreatePortalExitTypeHandler(TBootstrap* bootstrap)
{
    return New<TPortalExitTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
