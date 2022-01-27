#include "chaos_replicated_table_node_type_handler.h"
#include "chaos_replicated_table_node.h"
#include "chaos_replicated_table_node_proxy.h"
#include "private.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChaosServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableTypeHandler
    : public TCypressNodeTypeHandlerBase<TChaosReplicatedTableNode>
{
public:
    using TCypressNodeTypeHandlerBase::TCypressNodeTypeHandlerBase;

    EObjectType GetObjectType() const override
    {
        return EObjectType::ChaosReplicatedTable;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TChaosReplicatedTableNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateChaosReplicatedTableNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    std::unique_ptr<TChaosReplicatedTableNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        auto replicationCardId = context.ExplicitAttributes->GetAndRemove<TReplicationCardId>("replication_card_id");
        if (TypeFromId(replicationCardId) != EObjectType::ReplicationCard) {
            THROW_ERROR_EXCEPTION("Malformed replication card id");
        }

        auto ownsReplicationCard = context.ExplicitAttributes->GetAndRemove<bool>("owns_replication_card", true);

        auto implHolder = TCypressNodeTypeHandlerBase::DoCreate(id, context);
        implHolder->SetReplicationCardId(replicationCardId);
        implHolder->SetOwnsReplicationCard(ownsReplicationCard);

        return implHolder;
    }

    void PostReplicationCardRemovalRequest(TChaosReplicatedTableNode* node)
    {
        auto cellTag = CellTagFromId(node->GetReplicationCardId());

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* chaosCell = cellManager->FindCellByCellTag(cellTag);
        if (!IsObjectAlive(chaosCell)) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "No chaos cell hosting replication card is known (ReplicationCardId: %v, CellTag: %v)",
                node->GetReplicationCardId(),
                cellTag);
            return;
        }

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->FindMailbox(chaosCell->GetId());
        if (!mailbox) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "No mailbox exists for chaos cell (ReplicationCardId: %v, ChaosCellId: %v)",
                node->GetReplicationCardId(),
                chaosCell->GetId());
            return;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Sending replication card removal request to chaos cell (ReplicationCardId: %v, ChaosCellId: %v)",
            node->GetReplicationCardId(),
            chaosCell->GetId());

        NChaosClient::NProto::TReqRemoveReplicationCard request;
        ToProto(request.mutable_replication_card_id(), node->GetReplicationCardId());
        hiveManager->PostMessage(mailbox, request);
    }

    void DoDestroy(TChaosReplicatedTableNode* node) override
    {
        if (node->IsTrunk() && node->GetOwnsReplicationCard()) {
            PostReplicationCardRemovalRequest(node);
        }

        TCypressNodeTypeHandlerBase::DoDestroy(node);
    }

    void DoBranch(
        const TChaosReplicatedTableNode* originatingNode,
        TChaosReplicatedTableNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TCypressNodeTypeHandlerBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetReplicationCardId(originatingNode->GetReplicationCardId());
        branchedNode->SetOwnsReplicationCard(originatingNode->GetOwnsReplicationCard());
    }

    void DoClone(
        TChaosReplicatedTableNode* sourceNode,
        TChaosReplicatedTableNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TCypressNodeTypeHandlerBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

        clonedTrunkNode->SetReplicationCardId(sourceNode->GetReplicationCardId());
        clonedTrunkNode->SetOwnsReplicationCard(sourceNode->GetOwnsReplicationCard());
    }

    void DoBeginCopy(
        TChaosReplicatedTableNode* node,
        TBeginCopyContext* context) override
    {
        TCypressNodeTypeHandlerBase::DoBeginCopy(node, context);

        using NYT::Save;
        Save(*context, node->GetReplicationCardId());
        Save(*context, node->GetOwnsReplicationCard());
    }

    void DoEndCopy(
        TChaosReplicatedTableNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TCypressNodeTypeHandlerBase::DoEndCopy(trunkNode, context, factory);

        using NYT::Load;
        trunkNode->SetReplicationCardId(Load<TReplicationCardId>(*context));
        trunkNode->SetOwnsReplicationCard(Load<bool>(*context));
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateChaosReplicatedTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TChaosReplicatedTableTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
