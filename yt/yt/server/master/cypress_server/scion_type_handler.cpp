#include "scion_type_handler.h"

#include "grafting_manager.h"
#include "node_detail.h"
#include "scion_node.h"
#include "scion_proxy.h"

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSequoiaClient;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TScionTypeHandler
    : public TSequoiaMapNodeTypeHandlerImpl<TScionNode>
{
public:
    using TSequoiaMapNodeTypeHandlerImpl::TSequoiaMapNodeTypeHandlerImpl;

    EObjectType GetObjectType() const override
    {
        return EObjectType::Scion;
    }

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::None;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TScionNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateScionProxy(
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode);
    }

    void DoDestroy(TScionNode* node) override
    {
        if (node->IsTrunk()) {
            const auto& graftingManager = GetBootstrap()->GetGraftingManager();
            graftingManager->OnScionDestroyed(node);
        }

        TSequoiaMapNodeTypeHandlerImpl::DoDestroy(node);
    }

    void DoSerializeNode(
        TScionNode* node,
        TSerializeNodeContext* context) override
    {
        // NB: Scions _must_ be snapshot-wise compatible with sequoia map nodes
        // due to type erasure in TNontemplateCypressNodeTypeHandlerBase::SerializeNodeCore.
        TSequoiaMapNodeTypeHandlerImpl::DoSerializeNode(node, context);
    }

    void DoMaterializeNode(
        TScionNode* /*trunkNode*/,
        TMaterializeNodeContext* /*context*/) override
    {
        // Should not actually happen.
        THROW_ERROR_EXCEPTION("Scions cannot be materialized during cross-cell cloning");
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateScionTypeHandler(TBootstrap* bootstrap)
{
    return New<TScionTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
