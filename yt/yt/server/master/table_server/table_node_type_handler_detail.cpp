#include "table_node_type_handler_detail.h"

#include "mount_config_attributes.h"
#include "private.h"
#include "replicated_table_node_proxy.h"
#include "replicated_table_node.h"
#include "secondary_index.h"
#include "table_manager.h"
#include "table_node_proxy.h"
#include "table_node.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_owner_type_handler.h>
#include <yt/yt/server/master/chunk_server/config.h>

#include <yt/yt/server/master/cypress_server/config.h>
#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/helpers.h>

#include <yt/yt/server/master/tablet_server/chaos_helpers.h>
#include <yt/yt/server/master/tablet_server/hunk_storage_node.h>
#include <yt/yt/server/master/tablet_server/mount_config_storage.h>
#include <yt/yt/server/master/tablet_server/tablet.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/lib/tablet_server/config.h>
#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NCypressServer::NProto;
using namespace NHiveServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

template<class TImpl>
TTableNodeTypeHandlerBase<TImpl>::TTableNodeTypeHandlerBase(TBootstrap* bootstrap)
    : TSchemafulNodeTypeHandler(bootstrap)
    , TTabletOwnerTypeHandler(bootstrap)
{
    // NB: Due to virtual inheritance bootstrap has to be explicitly initialized.
    this->SetBootstrap(bootstrap);
}

template <class TImpl>
bool TTableNodeTypeHandlerBase<TImpl>::HasBranchedChangesImpl(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    if (TTabletOwnerTypeHandler::HasBranchedChangesImpl(originatingNode, branchedNode))  {
        return true;
    }

    return
        branchedNode->GetOptimizeFor() != originatingNode->GetOptimizeFor() ||
        branchedNode->GetHunkErasureCodec() != originatingNode->GetHunkErasureCodec() ||
        // One may consider supporting unlocking unmounted dynamic tables.
        // However, it isn't immediately obvious why that should be useful and
        // allowing to unlock something always requires careful consideration.
        branchedNode->IsDynamic();
}

template <class TImpl>
std::unique_ptr<TImpl> TTableNodeTypeHandlerBase<TImpl>::DoCreate(
    TVersionedNodeId id,
    const TCreateNodeContext& context)
{
    const auto& cypressManagerConfig = this->GetBootstrap()->GetDynamicConfig()->CypressManager;
    const auto& chunkManagerConfig = this->GetBootstrap()->GetConfigManager()->GetConfig()->ChunkManager;

    if (auto compressionCodecValue = context.ExplicitAttributes->FindYson("compression_codec")) {
        ValidateCompressionCodec(
            compressionCodecValue,
            chunkManagerConfig->ForbiddenCompressionCodecs,
            chunkManagerConfig->ForbiddenCompressionCodecNameToAlias);
    }

    if (auto eraseCodecValue = context.ExplicitAttributes->FindYson("erasure_codec")) {
        ValidateErasureCodec(eraseCodecValue, chunkManagerConfig->ForbiddenErasureCodecs);
    }

    auto combinedAttributes = OverlayAttributeDictionaries(context.ExplicitAttributes, context.InheritedAttributes);
    bool dynamic = combinedAttributes->GetAndRemove<bool>("dynamic", false);
    auto optionalTabletCellBundleName = combinedAttributes->FindAndRemove<std::string>("tablet_cell_bundle");
    bool optimizeForIsExplicit = context.ExplicitAttributes->Contains("optimize_for");
    auto optimizeFor = combinedAttributes->GetAndRemove<EOptimizeFor>("optimize_for", dynamic ? EOptimizeFor::Scan : EOptimizeFor::Lookup);
    auto optionalChunkFormat = combinedAttributes->FindAndRemove<EChunkFormat>("chunk_format");
    auto hunkErasureCodec = combinedAttributes->GetAndRemove<NErasure::ECodec>("hunk_erasure_codec", NErasure::ECodec::None);
    auto replicationFactor = combinedAttributes->GetAndRemove("replication_factor", cypressManagerConfig->DefaultTableReplicationFactor);
    auto compressionCodec = combinedAttributes->GetAndRemove<NCompression::ECodec>("compression_codec", NCompression::ECodec::Lz4);
    auto erasureCodec = combinedAttributes->GetAndRemove<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
    auto enableStripedErasure = combinedAttributes->GetAndRemove<bool>("use_striped_erasure", false);
    auto replicationProgress = combinedAttributes->FindAndRemove<TReplicationProgress>("replication_progress");
    auto trimmedRowCounts = combinedAttributes->GetAndRemove<std::vector<i64>>("trimmed_row_counts", {});
    auto hunkStorageId = combinedAttributes->FindAndRemove<TObjectId>("hunk_storage_id");

    ValidateReplicationFactor(replicationFactor);

    if (optionalChunkFormat) {
        ValidateTableChunkFormat(*optionalChunkFormat);
        if (optimizeForIsExplicit) {
            ValidateTableChunkFormatAndOptimizeFor(*optionalChunkFormat, optimizeFor);
        }
        optimizeFor = OptimizeForFromFormat(*optionalChunkFormat);
    }

    auto type = TypeFromId(id.ObjectId);
    bool replicated = type == EObjectType::ReplicatedTable;
    bool log = IsLogTableType(type);

    if (log && !dynamic) {
        THROW_ERROR_EXCEPTION("Table of type %Qlv must be dynamic",
            type);
    }

    auto tableSchema = combinedAttributes->FindAndRemove<TTableSchemaPtr>("schema");
    auto schemaId = combinedAttributes->GetAndRemove<TObjectId>("schema_id", NullObjectId);
    auto schemaMode = combinedAttributes->GetAndRemove<ETableSchemaMode>("schema_mode", ETableSchemaMode::Weak);

    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    const auto* effectiveTableSchema = tableManager->ProcessSchemaFromAttributes(
        tableSchema,
        schemaId,
        dynamic,
        /*chaos*/ false,
        id);

    auto optionalTabletCount = combinedAttributes->FindAndRemove<int>("tablet_count");
    auto optionalPivotKeys = combinedAttributes->FindAndRemove<std::vector<TLegacyOwningKey>>("pivot_keys");
    if (optionalTabletCount && optionalPivotKeys) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"tablet_count\" and \"pivot_keys\"");
    }
    auto upstreamReplicaId = combinedAttributes->GetAndRemove<TTableReplicaId>("upstream_replica_id", TTableReplicaId());
    if (upstreamReplicaId) {
        if (!dynamic) {
            THROW_ERROR_EXCEPTION("Upstream replica can only be set for dynamic tables");
        }
        if (replicated) {
            THROW_ERROR_EXCEPTION("Upstream replica cannot be set for replicated tables");
        }
    }

    if (replicationProgress && !IsChaosTableReplicaType(TypeFromId(upstreamReplicaId))) {
        THROW_ERROR_EXCEPTION("Replication progress can only be initialized for tables bound for chaos replication");
    }

    if (!trimmedRowCounts.empty()) {
        if (!dynamic) {
            THROW_ERROR_EXCEPTION("\"trimmed_row_counts\" can only be provided for dynamic tables");
        }
        if (log) {
            THROW_ERROR_EXCEPTION("\"trimmed_row_counts\" cannot not be provided for table of type %Qlv",
                type);
        }
        if (effectiveTableSchema->IsSorted()) {
            THROW_ERROR_EXCEPTION("\"trimmed_row_counts\" can only be set for ordered tables");
        }
        if (!optionalTabletCount) {
            THROW_ERROR_EXCEPTION("\"trimmed_row_counts\" cannot be specified without \"tablet_count\"");
        }
        if (ssize(trimmedRowCounts) != *optionalTabletCount) {
            THROW_ERROR_EXCEPTION("\"trimmed_row_counts\" has invalid size: expected %v or %v, got %v",
                0,
                *optionalTabletCount,
                ssize(trimmedRowCounts));
        }
    }

    const auto& tabletManager = this->GetBootstrap()->GetTabletManager();
    auto* tabletCellBundle = optionalTabletCellBundleName
        ? tabletManager->GetTabletCellBundleByNameOrThrow(*optionalTabletCellBundleName, true /*activeLifeStageOnly*/)
        : tabletManager->GetDefaultTabletCellBundle();

    InternalizeMountConfigAttributes(combinedAttributes.Get());

    auto nodeHolder = this->DoCreateImpl(
        id,
        context,
        replicationFactor,
        compressionCodec,
        erasureCodec,
        enableStripedErasure);
    auto* node = nodeHolder.get();

    try {
        node->SetOptimizeFor(optimizeFor);
        if (optionalChunkFormat) {
            node->SetChunkFormat(*optionalChunkFormat);
        }
        node->SetHunkErasureCodec(hunkErasureCodec);

        if (node->IsPhysicallyLog()) {
            // NB: This setting may be not visible in attributes but crucial for replication
            // to work properly.
            node->SetCommitOrdering(NTransactionClient::ECommitOrdering::Strong);
        }

        if (node->IsNative()) {
            if (effectiveTableSchema) {
                tableManager->GetOrCreateNativeMasterTableSchema(*effectiveTableSchema, node);
            } else {
                auto* emptySchema = tableManager->GetEmptyMasterTableSchema();
                tableManager->SetTableSchema(node, emptySchema);
            }
        } else {
            YT_VERIFY(schemaId);
            YT_VERIFY(!tableSchema);

            auto* schemaById = tableManager->GetMasterTableSchema(schemaId);
            tableManager->SetTableSchema(node, schemaById);
        }

        if ((node->IsNative() && effectiveTableSchema) || schemaMode == ETableSchemaMode::Strong) {
            node->SetSchemaMode(ETableSchemaMode::Strong);
        }

        // NB: Dynamic table should have a bundle during creation for accounting to work properly.
        tabletManager->SetTabletCellBundle(node, tabletCellBundle);

        if (dynamic) {
            if (node->IsNative()) {
                tabletManager->ValidateMakeTableDynamic(node);
            }

            tabletManager->MakeTableDynamic(
                node,
                trimmedRowCounts.empty() ? 0 : trimmedRowCounts[0]);

            std::vector<i64> adjustedTrimmedRowCounts;
            if (!trimmedRowCounts.empty()) {
                adjustedTrimmedRowCounts = std::move(trimmedRowCounts);
                adjustedTrimmedRowCounts.erase(adjustedTrimmedRowCounts.begin());
            }

            if (node->IsTrackedQueueObject()) {
                tableManager->RegisterQueue(node);
            }

            if (node->IsTrackedQueueConsumerObject()) {
                tableManager->RegisterQueueConsumer(node);
            }

            if (node->IsTrackedQueueProducerObject()) {
                tableManager->RegisterQueueProducer(node);
            }

            if (node->IsNative()) {
                if (optionalTabletCount) {
                    tabletManager->PrepareReshard(
                        node,
                        /*firstTabletIndex*/ 0,
                        /*lastTabletIndex*/ 0,
                        *optionalTabletCount,
                        /*pivotKeys*/ {},
                        adjustedTrimmedRowCounts,
                        /*create*/ true);
                } else if (optionalPivotKeys) {
                    tabletManager->PrepareReshard(
                        node,
                        /*firstTabletIndex*/ 0,
                        /*lastTabletIndex*/ 0,
                        optionalPivotKeys->size(),
                        *optionalPivotKeys,
                        adjustedTrimmedRowCounts,
                        /*create*/ true);
                }
            }

            if (!node->IsExternal()) {
                if (optionalTabletCount) {
                    tabletManager->Reshard(
                        node,
                        /*firstTabletIndex*/ 0,
                        /*lastTabletIndex*/ 0,
                        *optionalTabletCount,
                        /*pivotKeys*/ {},
                        adjustedTrimmedRowCounts);
                } else if (optionalPivotKeys) {
                    tabletManager->Reshard(node,
                        /*firstTabletIndex*/ 0,
                        /*lastTabletIndex*/ 0,
                        optionalPivotKeys->size(),
                        *optionalPivotKeys,
                        adjustedTrimmedRowCounts);
                }
            }

            node->SetUpstreamReplicaId(upstreamReplicaId);

            if (replicationProgress) {
                ScatterReplicationProgress(node, *replicationProgress);
            }
        }

        if (hunkStorageId) {
            const auto& objectManager = this->GetBootstrap()->GetObjectManager();
            auto* object = objectManager->GetObjectOrThrow(*hunkStorageId);
            node->ValidateAndSetHunkStorage(object);
        }
    } catch (const std::exception&) {
        this->Zombify(node);
        this->Destroy(node);
        throw;
    }

    if (replicated && !id.IsBranched() && !node->IsExternal()) {
        tabletManager->GetReplicatedTableCreatedSignal()->Fire(TReplicatedTableData{
            .Id = id.ObjectId,
            .Options = New<TReplicatedTableOptions>(),
        });
    }

    return nodeHolder;
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoDestroy(TImpl* table)
{
    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    if (table->IsTrackedQueueObject()) {
        tableManager->UnregisterQueue(table);
    }

    if (table->IsTrackedQueueConsumerObject()) {
        tableManager->UnregisterQueueConsumer(table);
    }

    if (table->IsTrackedQueueProducerObject()) {
        tableManager->UnregisterQueueProducer(table);
    }

    TTabletOwnerTypeHandler::DoDestroy(table);
    TSchemafulNodeTypeHandler::DoDestroy(table);

    // COMPAT(akozhikhov): We do this as we move reset from destroy to zombify
    // to guarantee that reset will be called.
    const auto& configManager = this->GetBootstrap()->GetConfigManager();
    const auto& config = configManager->GetConfig()->ObjectManager;
    if (config->ResetHunkStorageInTableDestroy) {
        table->ResetHunkStorage();
    }
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoZombify(TImpl* table)
{
    TTabletOwnerTypeHandler::DoZombify(table);
    TSchemafulNodeTypeHandler::DoZombify(table);

    const auto& objectManager = this->GetBootstrap()->GetObjectManager();

    // Since the deletion and attribute alteration is replicated to the external cell,
    // we must only unref it once - on the native cell.
    if (table->IsNative()) {
        if (auto secondaryIndex = table->GetIndexTo()) {
            secondaryIndex->SetIndexTableId({});
            int refCounter = objectManager->UnrefObject(secondaryIndex);

            YT_LOG_ALERT_IF(refCounter > 0,
                "Failed to drop secondary index upon index table removal (IndexTableId: %v, IndexId: %v, IndexRefCounter: %v)",
                table->GetId(),
                secondaryIndex->GetId(),
                refCounter);

            table->SetIndexTo(nullptr);
        }

        for (auto secondaryIndex : GetValuesSortedByKey(table->SecondaryIndices())) {
            secondaryIndex->SetTableId({});
            int refCounter = objectManager->UnrefObject(secondaryIndex);

            YT_LOG_ALERT_IF(refCounter > 0,
                "Failed to drop secondary index upon table removal (TableId: %v, IndexId: %v, IndexRefCounter: %v)",
                table->GetId(),
                secondaryIndex->GetId(),
                refCounter);
        }

        table->MutableSecondaryIndices().clear();
    }

    if (!table->IsExternal() &&
        table->IsTrunk() &&
        TypeFromId(table->GetId()) == EObjectType::ReplicatedTable)
    {
        this->GetBootstrap()->GetTabletManager()->GetReplicatedTableDestroyedSignal()->Fire(table->GetId());
    }

    table->ResetHunkStorage();
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const TLockRequest& lockRequest)
{
    TTabletOwnerTypeHandler::DoBranch(originatingNode, branchedNode, lockRequest);
    TSchemafulNodeTypeHandler::DoBranch(originatingNode, branchedNode, lockRequest);

    branchedNode->SetHunkStorage(originatingNode->GetHunkStorage());
    branchedNode->SetOptimizeFor(originatingNode->GetOptimizeFor());
    branchedNode->SetHunkErasureCodec(originatingNode->GetHunkErasureCodec());
    branchedNode->SetProfilingMode(originatingNode->GetProfilingMode());
    branchedNode->SetProfilingTag(originatingNode->GetProfilingTag());
    branchedNode->SetUpstreamReplicaId(originatingNode->GetUpstreamReplicaId());
    if (auto* originatingMountConfig = originatingNode->FindMountConfigStorage()) {
        *branchedNode->GetMutableMountConfigStorage() = *originatingMountConfig;
    }

    // Save current retained and unflushed timestamps in locked node.
    branchedNode->SetRetainedTimestamp(originatingNode->GetCurrentRetainedTimestamp());
    branchedNode->SetUnflushedTimestamp(originatingNode->GetCurrentUnflushedTimestamp(lockRequest.Timestamp));
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    bool isQueueObjectBefore = originatingNode->IsTrackedQueueObject();

    originatingNode->SetHunkStorage(branchedNode->GetHunkStorage());
    branchedNode->ResetHunkStorage();

    originatingNode->MergeOptimizeFor(branchedNode);
    originatingNode->MergeHunkErasureCodec(branchedNode);
    originatingNode->SetProfilingMode(branchedNode->GetProfilingMode());
    originatingNode->SetProfilingTag(branchedNode->GetProfilingTag());
    originatingNode->SetUpstreamReplicaId(branchedNode->GetUpstreamReplicaId());
    if (auto* branchedMountConfig = branchedNode->FindMountConfigStorage()) {
        *originatingNode->GetMutableMountConfigStorage() = *branchedMountConfig;
    }

    TTabletOwnerTypeHandler::DoMerge(originatingNode, branchedNode);
    TSchemafulNodeTypeHandler::DoMerge(originatingNode, branchedNode);

    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    bool isQueueObjectAfter = originatingNode->IsTrackedQueueObject();
    if (isQueueObjectAfter != isQueueObjectBefore) {
        if (isQueueObjectAfter) {
            tableManager->RegisterQueue(originatingNode);
        } else {
            tableManager->UnregisterQueue(originatingNode);
        }
    }
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    NYTree::IAttributeDictionary* inheritedAttributes,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    if (!factory->ShouldAllowSecondaryIndexAbandonment() && !IsHiveMutation()) {
        if (sourceNode->GetIndexTo()) {
            THROW_ERROR_EXCEPTION("Cannot copy table %v because it acts as an index to table %v, consider specifying "
                "\"allow_secondary_index_abandonment\" option",
                sourceNode->GetId(),
                sourceNode->GetIndexTo()->GetTableId());
        } else if (!sourceNode->SecondaryIndices().empty()) {
            THROW_ERROR_EXCEPTION("Cannot copy table %v because it has secondary indices, consider specifying "
                "\"allow_secondary_index_abandonment\" option",
                sourceNode->GetId());
        }
    }

    // Order is important: table schema might be used in TChunkOwnerTypeHandler::DoClone.
    TSchemafulNodeTypeHandler::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);
    TTabletOwnerTypeHandler::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

    clonedTrunkNode->SetHunkStorage(sourceNode->GetHunkStorage());

    clonedTrunkNode->SetOptimizeFor(sourceNode->GetOptimizeFor());
    if (auto optionalChunkFormat = sourceNode->TryGetChunkFormat()) {
        clonedTrunkNode->SetChunkFormat(*optionalChunkFormat);
    }
    clonedTrunkNode->SetHunkErasureCodec(sourceNode->GetHunkErasureCodec());

    auto* trunkSourceNode = sourceNode->GetTrunkNode();
    if (trunkSourceNode->HasCustomDynamicTableAttributes()) {
        clonedTrunkNode->InitializeCustomDynamicTableAttributes();
        clonedTrunkNode->GetCustomDynamicTableAttributes()->CopyFrom(
            trunkSourceNode->GetCustomDynamicTableAttributes());
    }

    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    if (clonedTrunkNode->IsTrackedQueueObject()) {
        tableManager->RegisterQueue(clonedTrunkNode);
    }
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoSerializeNode(
    TImpl* node,
    TSerializeNodeContext* context)
{
    TTabletOwnerTypeHandler::DoSerializeNode(node, context);
    TSchemafulNodeTypeHandler::DoSerializeNode(node, context);

    if (node->IsDynamic()) {
        // This is just a precaution. Copying dynamic tables should be fine: yes,
        // there's a potential for a race between externalization request arriving
        // to an external cell via Hive and a mount request arriving there via RPC
        // (as part of 2PC). This race, however, is prevented by both mounting and
        // externalization taking exclusive lock beforehand.
        const auto& configManager = this->GetBootstrap()->GetConfigManager();
        const auto& config = configManager->GetConfig()->CypressManager;
        if (!config->AllowCrossShardDynamicTableCopying) {
            THROW_ERROR_EXCEPTION("Dynamic tables do not support cross-cell copying");
        }

        if (!node->GetTrunkNode()->SecondaryIndices().empty() || node->GetTrunkNode()->GetIndexTo()) {
            THROW_ERROR_EXCEPTION("Cannot cross-cell copy neither a table with a secondary index nor an index table itself");
        }
    }

    if (auto* hunkStorage = node->GetHunkStorage()) {
        THROW_ERROR_EXCEPTION("Cannot cross-cell copy a table with hunk storage node %v",
            hunkStorage->GetId());
    }

    using NYT::Save;

    Save(*context, node->GetOptimizeFor());
    Save(*context, node->TryGetChunkFormat());
    Save(*context, node->GetHunkErasureCodec());

    auto* trunkNode = node->GetTrunkNode();
    Save(*context, trunkNode->HasCustomDynamicTableAttributes());
    if (trunkNode->HasCustomDynamicTableAttributes()) {
        trunkNode->GetCustomDynamicTableAttributes()->SerializeNode(context);
    }
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoMaterializeNode(
    TImpl* node,
    TMaterializeNodeContext* context)
{
    TTabletOwnerTypeHandler::DoMaterializeNode(node, context);
    TSchemafulNodeTypeHandler::DoMaterializeNode(node, context);

    // TODO(babenko): support copying dynamic tables

    using NYT::Load;

    node->SetOptimizeFor(Load<EOptimizeFor>(*context));
    if (auto optionalChunkFormat = Load<std::optional<EChunkFormat>>(*context)) {
        node->SetChunkFormat(*optionalChunkFormat);
    }
    node->SetHunkErasureCodec(Load<NErasure::ECodec>(*context));

    if (Load<bool>(*context)) {
        node->InitializeCustomDynamicTableAttributes();
        node->GetCustomDynamicTableAttributes()->MaterializeNode(context);
    }

    // TODO(achulkov2): Add corresponding test once copying dynamic tables is supported. Please ping me :)
    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    if (node->IsTrackedQueueObject()) {
        tableManager->RegisterQueue(node);
    }
}

template<class TImpl>
bool TTableNodeTypeHandlerBase<TImpl>::IsSupportedInheritableAttribute(const std::string& key) const
{
    static const THashSet<TString> SupportedInheritableAttributes{
        EInternedAttributeKey::Atomicity.Unintern(),
        EInternedAttributeKey::CommitOrdering.Unintern(),
        EInternedAttributeKey::OptimizeFor.Unintern(),
        EInternedAttributeKey::HunkErasureCodec.Unintern(),
        EInternedAttributeKey::ProfilingMode.Unintern(),
        EInternedAttributeKey::ProfilingTag.Unintern(),
    };

    if (SupportedInheritableAttributes.contains(key)) {
        return true;
    }

    return TTabletOwnerTypeHandler::IsSupportedInheritableAttribute(key);
}

template <class TImpl>
std::optional<std::vector<std::string>> TTableNodeTypeHandlerBase<TImpl>::DoListColumns(TImpl* node) const
{
    const auto& schema = *node->GetSchema()->AsTableSchema();

    std::vector<std::string> result;
    result.reserve(schema.Columns().size());
    for (const auto& column : schema.Columns()) {
        result.push_back(column.Name());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TTableNodeTypeHandler::TTableNodeTypeHandler(TBootstrap* bootstrap)
    : TBase(bootstrap)
{
    // NB: Due to virtual inheritance bootstrap has to be explicitly initialized.
    SetBootstrap(bootstrap);
}

EObjectType TTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::Table;
}

ICypressNodeProxyPtr TTableNodeTypeHandler::DoGetProxy(
    TTableNode* trunkNode,
    TTransaction* transaction)
{
    return CreateTableNodeProxy(
        GetBootstrap(),
        &Metadata_,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

TReplicationLogTableNodeTypeHandler::TReplicationLogTableNodeTypeHandler(TBootstrap* bootstrap)
    : TTableNodeTypeHandler(bootstrap)
{
    // NB: Due to virtual inheritance bootstrap has to be explicitly initialized.
    SetBootstrap(bootstrap);
}

EObjectType TReplicationLogTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::ReplicationLogTable;
}

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableNodeTypeHandler::TReplicatedTableNodeTypeHandler(TBootstrap* bootstrap)
    : TBase(bootstrap)
{
    // NB: Due to virtual inheritance bootstrap has to be explicitly initialized.
    SetBootstrap(bootstrap);
}

EObjectType TReplicatedTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::ReplicatedTable;
}

bool TReplicatedTableNodeTypeHandler::HasBranchedChangesImpl(
    TReplicatedTableNode* /*originatingNode*/,
    TReplicatedTableNode* /*branchedNode*/)
{
    // Forbid explicitly unlocking replicated tables.
    return true;
}

ICypressNodeProxyPtr TReplicatedTableNodeTypeHandler::DoGetProxy(
    TReplicatedTableNode* trunkNode,
    TTransaction* transaction)
{
    return CreateReplicatedTableNodeProxy(
        GetBootstrap(),
        &Metadata_,
        transaction,
        trunkNode);
}

void TReplicatedTableNodeTypeHandler::DoBranch(
    const TReplicatedTableNode* originatingNode,
    TReplicatedTableNode* branchedNode,
    const NCypressServer::TLockRequest& lockRequest)
{
    // NB: Replica set is got from trunk node (see #TReplicatedTableNode::Replicas).
    branchedNode->SetReplicatedTableOptions(CloneYsonStruct(originatingNode->GetReplicatedTableOptions()));

    TBase::DoBranch(originatingNode, branchedNode, lockRequest);
}

void TReplicatedTableNodeTypeHandler::DoClone(
    TReplicatedTableNode* sourceNode,
    TReplicatedTableNode* clonedTrunkNode,
    IAttributeDictionary* inheritedAttributes,
    NCypressServer::ICypressNodeFactory* factory,
    NCypressServer::ENodeCloneMode mode,
    NSecurityServer::TAccount* account)
{
    auto id = clonedTrunkNode->GetVersionedId();
    if (!id.IsBranched() && !clonedTrunkNode->IsExternal()) {
        const auto& tabletManager = this->GetBootstrap()->GetTabletManager();
        tabletManager->GetReplicatedTableCreatedSignal()->Fire(TReplicatedTableData{
            .Id = id.ObjectId,
            .Options = New<TReplicatedTableOptions>(),
        });
    }

    TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

    // NB: Replica set is intentionally not copied but recreated by tablet manager.
    clonedTrunkNode->SetReplicatedTableOptions(CloneYsonStruct(sourceNode->GetReplicatedTableOptions()));

    // TODO(akozhikhov): Change order of DoClone to call this signal once.
    if (!id.IsBranched() && !clonedTrunkNode->IsExternal()) {
        const auto& tabletManager = this->GetBootstrap()->GetTabletManager();
        tabletManager->GetReplicatedTableCreatedSignal()->Fire(TReplicatedTableData{
            .Id = id.ObjectId,
            .Options = clonedTrunkNode->GetReplicatedTableOptions(),
        });
    }
}

void TReplicatedTableNodeTypeHandler::DoSerializeNode(
    TReplicatedTableNode* /*node*/,
    TSerializeNodeContext* /*context*/)
{
    // TODO(babenko): support cross-cell copy for replicated tables
    THROW_ERROR_EXCEPTION("Replicated tables do not support cross-cell copying");
}

void TReplicatedTableNodeTypeHandler::DoMaterializeNode(
    TReplicatedTableNode* /*node*/,
    TMaterializeNodeContext* /*context*/)
{
    // TODO(babenko): support cross-cell copy for replicated tables
    THROW_ERROR_EXCEPTION("Replicated tables do not support cross-cell copying");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
