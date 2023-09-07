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

    EObjectType GetObjectType() const override
    {
        return EObjectType::PortalExit;
    }

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::None;
    }

    TAcdList ListAcds(TCypressNode* node) const override
    {
        return TAcdList{&node->Acd(), &node->As<TPortalExitNode>()->DirectAcd()};
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TPortalExitNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreatePortalExitProxy(
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode);
    }

    void DoDestroy(TPortalExitNode* node) override
    {
        if (node->IsTrunk()) {
            const auto& portalManager = GetBootstrap()->GetPortalManager();
            portalManager->DestroyExitNode(node);
        }

        TMapNodeTypeHandlerImpl::DoDestroy(node);
    }

    void DoBeginCopy(
        TPortalExitNode* node,
        TBeginCopyContext* context) override
    {
        // NB: Portal exits _must_ be snapshot-wise compatible with map nodes
        // due to type erasure in TNontemplateCypressNodeTypeHandlerBase::BeginCopyCore.
        TMapNodeTypeHandlerImpl::DoBeginCopy(node, context);
    }

    void DoEndCopy(
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
