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
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TPortalExitTypeHandler
    : public TCypressMapNodeTypeHandlerImpl<TPortalExitNode>
{
public:
    using TCypressMapNodeTypeHandlerImpl::TCypressMapNodeTypeHandlerImpl;

    EObjectType GetObjectType() const override
    {
        return EObjectType::PortalExit;
    }

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::None;
    }

    TAcdList ListAcds(TCypressNode* trunkNode) const override
    {
        return {&trunkNode->Acd(), &trunkNode->As<TPortalExitNode>()->DirectAcd()};
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

        TCypressMapNodeTypeHandlerImpl::DoDestroy(node);
    }

    void DoSerializeNode(
        TPortalExitNode* node,
        TSerializeNodeContext* context) override
    {
        // NB: Portal exits _must_ be snapshot-wise compatible with map nodes
        // due to type erasure in TNontemplateCypressNodeTypeHandlerBase::SerializeNodeCore.
        TCypressMapNodeTypeHandlerImpl::DoSerializeNode(node, context);
    }

    void DoMaterializeNode(
        TPortalExitNode* /*trunkNode*/,
        TMaterializeNodeContext* /*context*/) override
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
