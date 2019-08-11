#include "portal_entrance_type_handler.h"
#include "portal_entrance_node.h"
#include "portal_entrance_proxy.h"
#include "portal_manager.h"

#include <yt/server/master/cell_master/multicell_manager.h>

#include <yt/server/master/cypress_server/proto/portal_manager.pb.h>

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

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::PortalEntrance;
    }

    virtual ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

    virtual ETypeFlags GetFlags() const override
    {
        return
            TBase::GetFlags() |
            ETypeFlags::ForbidInheritAclChange; // |
            //ETypeFlags::ForbidLocking; XXX(babenko)
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TPortalEntranceNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreatePortalEntranceProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TPortalEntranceNode> DoCreate(
        const TVersionedNodeId& id,
        const TCreateNodeContext& context) override
    {
        if (context.Transaction) {
            THROW_ERROR_EXCEPTION("Portals cannot be created in a transaction");
        }

        auto exitCellTag  = context.ExplicitAttributes->GetAndRemove<TCellTag>("exit_cell_tag");

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsRegisteredMasterCell(exitCellTag)) {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v", exitCellTag);
        }
        // XXX(babenko): validate cell role

        auto nodeHolder = TBase::DoCreate(id, context);
        auto* node = nodeHolder.get();

        node->SetExitCellTag(exitCellTag);

        return nodeHolder;
    }

    virtual void DoDestroy(TPortalEntranceNode* node) override
    {
        TBase::DoDestroy(node);

        const auto& portalManager = Bootstrap_->GetPortalManager();
        portalManager->DestroyEntranceNode(node);
    }

    virtual void DoBranch(
        const TPortalEntranceNode* /*originatingNode*/,
        TPortalEntranceNode* /*branchedNode*/,
        const TLockRequest& /*lockRequest*/) override
    {
        YT_ABORT();
    }

    virtual void DoMerge(
        TPortalEntranceNode* /*originatingNode*/,
        TPortalEntranceNode* /*branched*/Node) override
    {
        YT_ABORT();
    }

    virtual void DoClone(
        TPortalEntranceNode* /*sourceNode*/,
        TPortalEntranceNode* /*clonedNode*/,
        ICypressNodeFactory* /*factory*/,
        ENodeCloneMode /*mode*/,
        TAccount* /*account*/) override
    {
        THROW_ERROR_EXCEPTION("Cannot clone a portal");
    }

    virtual bool HasBranchedChangesImpl(
        TPortalEntranceNode* /*originatingNode*/,
        TPortalEntranceNode* /*branchedNode*/) override
    {
        // Cannot be branched.
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreatePortalEntranceTypeHandler(TBootstrap* bootstrap)
{
    return New<TPortalEntranceTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
