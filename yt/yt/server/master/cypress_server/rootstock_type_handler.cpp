#include "rootstock_type_handler.h"

#include "cypress_manager.h"
#include "rootstock_node.h"
#include "rootstock_proxy.h"
#include "grafting_manager.h"

#include <yt/yt/server/master/cypress_server/shard.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_manager_detail.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

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
        if (!NTransactionSupervisor::IsInTransactionAction()) {
            // COMPAT(h0pless): Remove once all issues with rootstock creation will be ironed out.
            YT_LOG_ALERT("An attempt to create a rootstock was made outside of transaction action, request was redirected to Sequoia (RootstockId: %v)",
                id);

            THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::RequestInvolvesSequoia,
                "Cannot create rootstock outside of transaction action");
        }

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
        IAttributeDictionary* /*inheritedAttributes*/,
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

    void DoSerializeNode(
        TRootstockNode* /*node*/,
        TSerializeNodeContext* /*context*/) override
    {
        THROW_ERROR_EXCEPTION("Cross-cell copying of rootstocks is not supported");
    }

    void DoMaterializeNode(
        TRootstockNode* /*trunkNode*/,
        TMaterializeNodeContext* /*context*/) override
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
