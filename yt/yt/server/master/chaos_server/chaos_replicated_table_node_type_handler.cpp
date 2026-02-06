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

#include <yt/yt/server/lib/misc/interned_attributes.h>

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
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChaosServerLogger;

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

        auto chaosCellBundleName = combinedAttributes->FindAndRemove<std::string>(EInternedAttributeKey::ChaosCellBundle.Unintern());
        if (!chaosCellBundleName) {
            THROW_ERROR_EXCEPTION("%Qv is neither specified nor inherited", EInternedAttributeKey::ChaosCellBundle.Unintern());
        }

        const auto& chaosManager = GetBootstrap()->GetChaosManager();
        auto* chaosCellBundle = chaosManager->GetChaosCellBundleByNameOrThrow(*chaosCellBundleName, /*activeLifeStageOnly*/ true);

        auto replicationCardId = combinedAttributes->GetAndRemove<TReplicationCardId>(EInternedAttributeKey::ReplicationCardId.Unintern(), {});
        if (replicationCardId && TypeFromId(replicationCardId) != EObjectType::ReplicationCard) {
            THROW_ERROR_EXCEPTION("Malformed replication card id")
                << TErrorAttribute("replication_card_id", replicationCardId);
        }

        auto ownsReplicationCard = combinedAttributes->GetAndRemove<bool>("owns_replication_card", true);
        auto constrainedSchema = combinedAttributes->FindAndRemove<TConstrainedTableSchema>("schema");
        if (constrainedSchema && !constrainedSchema->ColumnToConstraint().empty()) {
            THROW_ERROR_EXCEPTION("Cannot specify constraints in \"schema\" option, use \"constrained_schema\" instead")
                << TErrorAttribute("constraints", constrainedSchema->ColumnToConstraint());
        }
        auto tableSchema = constrainedSchema ? New<TCompactTableSchema>(constrainedSchema->TableSchema()) : nullptr;
        auto schemaId = combinedAttributes->GetAndRemove<TObjectId>("schema_id", NullObjectId);
        constrainedSchema = combinedAttributes->FindAndRemove<TConstrainedTableSchema>("constrained_schema");
        if (constrainedSchema) {
            THROW_ERROR_EXCEPTION("Attribute \"constrained_schema\" is not supported for chaos replicated tables")
                << TErrorAttribute("constrained_schema", constrainedSchema);
        }
        auto constraints = combinedAttributes->FindAndRemove<TColumnNameToConstraintMap>("constraints");
        if (constraints) {
            THROW_ERROR_EXCEPTION("Attribute \"constraints\" is not supported for chaos replicated tables")
                << TErrorAttribute("constraints", constraints);
        }

        const auto& tableManager = this->GetBootstrap()->GetTableManager();
        // NB: Chaos replicated table is always native.
        auto effectiveTableSchema = tableManager->ProcessSchemaFromAttributes(
            tableSchema,
            schemaId,
            /*tableSchemaFromConstrainedSchema*/ nullptr,
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
                tableManager->GetOrCreateNativeMasterTableSchema(effectiveTableSchema, node);
            } else {
                auto* emptyTableSchema = tableManager->GetEmptyMasterTableSchema();
                tableManager->SetTableSchema(node, emptyTableSchema);
            }

            // NB: Schema mode is always strong in chaos replicated tables.
            node->SetSchemaMode(ETableSchemaMode::Strong);

            if (node->IsTrackedQueueConsumerObject()) {
                chaosManager->RegisterQueueConsumer(node);
            }
            if (node->IsTrackedQueueProducerObject()) {
                chaosManager->RegisterQueueProducer(node);
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

    bool IsSupportedInheritableAttribute(const std::string& key) const override
    {
        static const THashSet<std::string> SupportedInheritableAttributes{
            EInternedAttributeKey::ChaosCellBundle.Unintern(),
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
        auto mailbox = hiveManager->FindMailbox(chaosCell->GetId());
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
        if (node->IsTrackedQueueConsumerObject()) {
            chaosManager->UnregisterQueueConsumer(node);
        }
        if (node->IsTrackedQueueProducerObject()) {
            chaosManager->UnregisterQueueProducer(node);
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
        IAttributeDictionary* inheritedAttributes,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

        const auto& chaosManager = GetBootstrap()->GetChaosManager();
        chaosManager->SetChaosCellBundle(clonedTrunkNode, sourceNode->GetTrunkNode()->ChaosCellBundle().Get());

        clonedTrunkNode->SetReplicationCardId(sourceNode->GetReplicationCardId());
        // NB: Cannot share ownership.
        clonedTrunkNode->SetOwnsReplicationCard(false);

        if (clonedTrunkNode->IsTrackedQueueObject()) {
            chaosManager->RegisterQueue(clonedTrunkNode);
        }
    }

    void DoSerializeNode(
        TChaosReplicatedTableNode* node,
        TSerializeNodeContext* context) override
    {
        TBase::DoSerializeNode(node, context);

        using NYT::Save;
        const auto& chaosCellBundle = node->GetTrunkNode()->ChaosCellBundle();
        Save(*context, chaosCellBundle);
        Save(*context, node->GetReplicationCardId());
        Save(*context, node->GetOwnsReplicationCard());
    }

    void DoMaterializeNode(
        TChaosReplicatedTableNode* trunkNode,
        TMaterializeNodeContext* context) override
    {
        TBase::DoMaterializeNode(trunkNode, context);

        using NYT::Load;

        auto chaosCellBundle = Load<TChaosCellBundleRawPtr>(*context);
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
