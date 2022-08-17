#include "portal_entrance_type_handler.h"
#include "portal_entrance_node.h"
#include "portal_entrance_proxy.h"
#include "portal_manager.h"

#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/cypress_server/proto/portal_manager.pb.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

class TPortalEntranceTypeHandler
    : public TCypressNodeTypeHandlerBase<TPortalEntranceNode>
{
private:
    using TBase = TCypressNodeTypeHandlerBase<TPortalEntranceNode>;

public:
    using TBase::TBase;

    EObjectType GetObjectType() const override
    {
        return EObjectType::PortalEntrance;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TPortalEntranceNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreatePortalEntranceProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    std::unique_ptr<TPortalEntranceNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        auto exitCellTag = context.ExplicitAttributes->GetAndRemove<TCellTag>("exit_cell_tag");

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (multicellManager->IsSecondaryMaster()) {
            THROW_ERROR_EXCEPTION("Portal entrance cannot be placed on the secondary cell");
        }

        if (exitCellTag == multicellManager->GetPrimaryCellTag()) {
            THROW_ERROR_EXCEPTION("Portal exit cannot be placed on the primary cell");
        }

        if (!multicellManager->IsRegisteredMasterCell(exitCellTag)) {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v", exitCellTag);
        }

        if (None(multicellManager->GetMasterCellRoles(exitCellTag) & EMasterCellRoles::CypressNodeHost)) {
            THROW_ERROR_EXCEPTION("Cell with tag %v cannot host Cypress nodes", exitCellTag);
        }

        if (context.Transaction && context.Transaction->GetParent()) {
            THROW_ERROR_EXCEPTION("Portal creation under nested transaction is forbidden");
        }

        auto nodeHolder = TBase::DoCreate(id, context);
        auto* node = nodeHolder.get();
        node->SetOpaque(true);

        node->SetExitCellTag(exitCellTag);

        return nodeHolder;
    }

    void DoDestroy(TPortalEntranceNode* node) override
    {
        if (node->IsTrunk()) {
            const auto& portalManager = Bootstrap_->GetPortalManager();
            portalManager->DestroyEntranceNode(node);
        }

        TBase::DoDestroy(node);
    }

    void DoBranch(
        const TPortalEntranceNode* originatingNode,
        TPortalEntranceNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetExitCellTag(originatingNode->GetExitCellTag());
    }

    void DoClone(
        TPortalEntranceNode* /*sourceNode*/,
        TPortalEntranceNode* /*clonedTrunkNode*/,
        ICypressNodeFactory* /*factory*/,
        ENodeCloneMode /*mode*/,
        TAccount* /*account*/) override
    {
        THROW_ERROR_EXCEPTION("Cannot clone a portal");
    }

    bool HasBranchedChangesImpl(
        TPortalEntranceNode* /*originatingNode*/,
        TPortalEntranceNode* /*branchedNode*/) override
    {
        // Cannot be branched.
        return false;
    }

    void DoBeginCopy(
        TPortalEntranceNode* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);

        using NYT::Save;
        auto exitId = MakePortalExitNodeId(node->GetId(), node->GetExitCellTag());
        Save(*context, exitId);
        context->RegisterPortalRootId(exitId);
    }

    void DoEndCopy(
        TPortalEntranceNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(trunkNode, context, factory);

        // TODO(babenko): cross-cell copying of portals
        THROW_ERROR_EXCEPTION("Cross-cell copying of portals is not supported");
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreatePortalEntranceTypeHandler(TBootstrap* bootstrap)
{
    return New<TPortalEntranceTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
