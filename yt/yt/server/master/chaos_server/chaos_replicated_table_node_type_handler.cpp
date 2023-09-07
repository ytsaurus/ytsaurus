#include "chaos_replicated_table_node_type_handler.h"

#include "chaos_cell_bundle.h"
#include "chaos_replicated_table_node.h"
#include "chaos_replicated_table_node_proxy.h"
#include "chaos_manager.h"
#include "private.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/table_server/schemaful_node_type_handler.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableTypeHandler
    : public TSchemafulNodeTypeHandlerBase<TChaosReplicatedTableNode>
{
private:
    using TBase = TSchemafulNodeTypeHandlerBase<TChaosReplicatedTableNode>;

public:
    explicit TChaosReplicatedTableTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    {
        // NB: Due to virtual inheritance bootstrap has to be explicitly initialized.
        SetBootstrap(bootstrap);
    }

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
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode);
    }

    std::unique_ptr<TChaosReplicatedTableNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        auto combinedAttributes = OverlayAttributeDictionaries(context.ExplicitAttributes, context.InheritedAttributes);

        auto optionalChaosCellBundleName = combinedAttributes->FindAndRemove<TString>("chaos_cell_bundle");
        if (!optionalChaosCellBundleName) {
            THROW_ERROR_EXCEPTION("\"chaos_cell_bundle\" is neither specified nor inherited");
        }

        const auto& chaosManager = GetBootstrap()->GetChaosManager();
        auto* chaosCellBundle = chaosManager->GetChaosCellBundleByNameOrThrow(*optionalChaosCellBundleName, /*activeLifeStageOnly*/ true);

        auto replicationCardId = combinedAttributes->GetAndRemove<TReplicationCardId>("replication_card_id", {});
        if (replicationCardId && TypeFromId(replicationCardId) != EObjectType::ReplicationCard) {
            THROW_ERROR_EXCEPTION("Malformed replication card id");
        }

        auto ownsReplicationCard = combinedAttributes->GetAndRemove<bool>("owns_replication_card", true);

        auto tableSchema = combinedAttributes->FindAndRemove<TTableSchemaPtr>("schema");
        auto schemaId = combinedAttributes->GetAndRemove<TObjectId>("schema_id", NullObjectId);

        const auto& tableManager = this->GetBootstrap()->GetTableManager();
        // NB: Chaos replicated table is always native.
        auto* effectiveTableSchema = tableManager->ProcessSchemaFromAttributes(
            tableSchema,
            schemaId,
            /*dynamic*/ true,
            /*chaos*/ true,
            /*nodeId*/ id);

        auto nodeHolder = TBase::DoCreate(id, context);
        auto* node = nodeHolder.get();

        try {
            node->SetReplicationCardId(replicationCardId);
            node->SetOwnsReplicationCard(ownsReplicationCard);
            chaosManager->SetChaosCellBundle(node, chaosCellBundle);

            if (effectiveTableSchema) {
                tableManager->GetOrCreateNativeMasterTableSchema(*effectiveTableSchema, node);
            } else {
                auto* emptyTableSchema = tableManager->GetEmptyMasterTableSchema();
                tableManager->SetTableSchema(node, emptyTableSchema);
            }

            // NB: Schema mode is always strong in chaos replicated tables.
            node->SetSchemaMode(ETableSchemaMode::Strong);

            if (node->IsTrackedConsumerObject()) {
                chaosManager->RegisterConsumer(node);
            }
            if (node->IsTrackedQueueObject()) {
                chaosManager->RegisterQueue(node);
            }

            return nodeHolder;
        } catch (const std::exception&) {
            this->Zombify(node);
            this->Destroy(node);
            throw;
        }
    }

    bool IsSupportedInheritableAttribute(const TString& key) const override
    {
        static const THashSet<TString> SupportedInheritableAttributes{
            "chaos_cell_bundle"
        };

        if (SupportedInheritableAttributes.contains(key)) {
            return true;
        }

        return TBase::IsSupportedInheritableAttribute(key);
    }

    void PostReplicationCardRemovalRequest(TChaosReplicatedTableNode* node)
    {
        auto cellTag = CellTagFromId(node->GetReplicationCardId());

        const auto& cellManager = GetBootstrap()->GetTamedCellManager();
        auto* chaosCell = cellManager->FindCellByCellTag(cellTag);
        if (!IsObjectAlive(chaosCell)) {
            YT_LOG_WARNING("No chaos cell hosting replication card is known (ReplicationCardId: %v, CellTag: %v)",
                node->GetReplicationCardId(),
                cellTag);
            return;
        }

        const auto& hiveManager = GetBootstrap()->GetHiveManager();
        auto* mailbox = hiveManager->FindMailbox(chaosCell->GetId());
        if (!mailbox) {
            YT_LOG_WARNING("No mailbox exists for chaos cell (ReplicationCardId: %v, ChaosCellId: %v)",
                node->GetReplicationCardId(),
                chaosCell->GetId());
            return;
        }

        YT_LOG_DEBUG("Sending replication card removal request to chaos cell (TableId: %v, ReplicationCardId: %v, ChaosCellId: %v)",
            node->GetId(),
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

        TBase::DoDestroy(node);
    }

    void DoZombify(TChaosReplicatedTableNode* node) override
    {
        const auto& chaosManager = GetBootstrap()->GetChaosManager();
        if (node->IsTrackedConsumerObject()) {
            chaosManager->UnregisterConsumer(node);
        }
        if (node->IsTrackedQueueObject()) {
            chaosManager->UnregisterQueue(node);
        }

        TBase::DoZombify(node);
    }

    void DoBranch(
        const TChaosReplicatedTableNode* originatingNode,
        TChaosReplicatedTableNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

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
        TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

        const auto& chaosManager = GetBootstrap()->GetChaosManager();
        chaosManager->SetChaosCellBundle(clonedTrunkNode, sourceNode->ChaosCellBundle().Get());

        clonedTrunkNode->SetReplicationCardId(sourceNode->GetReplicationCardId());
        // NB: Cannot share ownership.
        clonedTrunkNode->SetOwnsReplicationCard(false);

        if (clonedTrunkNode->IsTrackedQueueObject()) {
            chaosManager->RegisterQueue(clonedTrunkNode);
        }
    }

    void DoBeginCopy(
        TChaosReplicatedTableNode* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);

        using NYT::Save;
        Save(*context, node->ChaosCellBundle());
        Save(*context, node->GetReplicationCardId());
        Save(*context, node->GetOwnsReplicationCard());
    }

    void DoEndCopy(
        TChaosReplicatedTableNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(trunkNode, context, factory);

        using NYT::Load;

        auto* chaosCellBundle = Load<TChaosCellBundle*>(*context);
        const auto& chaosManager = GetBootstrap()->GetChaosManager();
        chaosManager->SetChaosCellBundle(trunkNode, chaosCellBundle);

        trunkNode->SetReplicationCardId(Load<TReplicationCardId>(*context));
        trunkNode->SetOwnsReplicationCard(Load<bool>(*context));

        if (trunkNode->IsTrackedQueueObject()) {
            chaosManager->RegisterQueue(trunkNode);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateChaosReplicatedTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TChaosReplicatedTableTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
