#include "rootstock_type_handler.h"

#include "rootstock_node.h"
#include "rootstock_proxy.h"
#include "grafting_manager.h"

#include <yt/yt/server/master/cypress_server/shard.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TRootstockTypeHandler
    : public TCypressNodeTypeHandlerBase<TRootstockNode>
{
private:
    using TBase = TCypressNodeTypeHandlerBase<TRootstockNode>;

public:
    using TBase::TBase;

    EObjectType GetObjectType() const override
    {
        return EObjectType::Rootstock;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TRootstockNode* rootstock,
        TTransaction* transaction) override
    {
        return CreateRootstockProxy(
            GetBootstrap(),
            &Metadata_,
            transaction,
            rootstock);
    }

    std::unique_ptr<TRootstockNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        const auto& cypressManager = GetBootstrap()->GetCypressManager();
        if (context.Shard != cypressManager->GetRootCypressShard()) {
            THROW_ERROR_EXCEPTION("Grafting can be performed only in root Cypress shard");
        }

        auto nodeHolder = TBase::DoCreate(id, context);
        auto* node = nodeHolder.get();
        node->SetOpaque(true);

        auto scionId = context.ExplicitAttributes->GetAndRemove<TNodeId>("scion_id");
        node->SetScionId(scionId);

        return nodeHolder;
    }

    void DoDestroy(TRootstockNode* node) override
    {
        if (node->IsTrunk()) {
            const auto& graftingManager = GetBootstrap()->GetGraftingManager();
            graftingManager->OnRootstockDestroyed(node);
        }

        TBase::DoDestroy(node);
    }

    void DoBranch(
        const TRootstockNode* originatingNode,
        TRootstockNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetScionId(originatingNode->GetScionId());
    }

    void DoClone(
        TRootstockNode* /*sourceNode*/,
        TRootstockNode* /*clonedTrunkNode*/,
        ICypressNodeFactory* /*factory*/,
        ENodeCloneMode /*mode*/,
        TAccount* /*account*/) override
    {
        THROW_ERROR_EXCEPTION("Cannot clone a rootstock");
    }

    bool HasBranchedChangesImpl(
        TRootstockNode* /*originatingNode*/,
        TRootstockNode* /*branchedNode*/) override
    {
        // Cannot be branched.
        return false;
    }

    void DoBeginCopy(
        TRootstockNode* /*node*/,
        TBeginCopyContext* /*context*/) override
    {
        THROW_ERROR_EXCEPTION("Cross-cell copying of rootstocks is not supported");
    }

    void DoEndCopy(
        TRootstockNode* /*trunkNode*/,
        TEndCopyContext* /*context*/,
        ICypressNodeFactory* /*factory*/) override
    {
        THROW_ERROR_EXCEPTION("Cross-cell copying of rootstocks is not supported");
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateRootstockTypeHandler(TBootstrap* bootstrap)
{
    return New<TRootstockTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
