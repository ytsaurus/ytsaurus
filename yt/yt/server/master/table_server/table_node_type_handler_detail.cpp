#include "table_manager.h"
#include "table_node_type_handler_detail.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "schemaful_node_helpers.h"
#include "replicated_table_node.h"
#include "replicated_table_node_proxy.h"
#include "master_table_schema.h"
#include "private.h"

#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/helpers.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_owner_type_handler.h>
#include <yt/yt/server/master/chunk_server/config.h>

#include <yt/yt/server/master/cypress_server/config.h>

#include <yt/yt/server/master/tablet_server/tablet.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsCompressionCodecValidationSuppressed()
{
    return IsSubordinateMutation();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
bool TTableNodeTypeHandlerBase<TImpl>::HasBranchedChangesImpl(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode))  {
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
    const auto& dynamicConfig = this->Bootstrap_->GetConfigManager()->GetConfig();
    const auto& cypressManagerConfig = this->Bootstrap_->GetConfig()->CypressManager;
    const auto& chunkManagerConfig = this->Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;

    if (!IsCompressionCodecValidationSuppressed()) {
        if (auto compressionCodecValue = context.ExplicitAttributes->FindYson("compression_codec")) {
            ValidateCompressionCodec(
                compressionCodecValue,
                chunkManagerConfig->DeprecatedCodecIds,
                chunkManagerConfig->DeprecatedCodecNameToAlias);
        }
    }

    auto combinedAttributes = OverlayAttributeDictionaries(context.ExplicitAttributes, context.InheritedAttributes);
    auto optionalTabletCellBundleName = combinedAttributes->FindAndRemove<TString>("tablet_cell_bundle");
    auto optimizeFor = combinedAttributes->GetAndRemove<EOptimizeFor>("optimize_for", EOptimizeFor::Lookup);
    auto hunkErasureCodec = combinedAttributes->GetAndRemove<NErasure::ECodec>("hunk_erasure_codec", NErasure::ECodec::None);
    auto replicationFactor = combinedAttributes->GetAndRemove("replication_factor", cypressManagerConfig->DefaultTableReplicationFactor);
    auto compressionCodec = combinedAttributes->GetAndRemove<NCompression::ECodec>("compression_codec", NCompression::ECodec::Lz4);
    auto erasureCodec = combinedAttributes->GetAndRemove<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

    ValidateReplicationFactor(replicationFactor);

    bool dynamic = combinedAttributes->GetAndRemove<bool>("dynamic", false);
    auto type = TypeFromId(id.ObjectId);
    bool replicated = type == EObjectType::ReplicatedTable;
    bool log = IsLogTableType(type);

    if (log && !dynamic) {
        THROW_ERROR_EXCEPTION("Table of type %Qlv must be dynamic",
            type);
    }

    const auto& tableManager = this->Bootstrap_->GetTableManager();
    auto [effectiveTableSchema, tableSchema] = ProcessSchemafulNodeAttributes(
        combinedAttributes,
        dynamic,
        dynamicConfig,
        tableManager);

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

    const auto& tabletManager = this->Bootstrap_->GetTabletManager();
    auto* tabletCellBundle = optionalTabletCellBundleName
        ? tabletManager->GetTabletCellBundleByNameOrThrow(*optionalTabletCellBundleName, true /*activeLifeStageOnly*/)
        : tabletManager->GetDefaultTabletCellBundle();

    auto nodeHolder = this->DoCreateImpl(
        id,
        context,
        replicationFactor,
        compressionCodec,
        erasureCodec);
    auto* node = nodeHolder.get();

    try {
        node->SetOptimizeFor(optimizeFor);
        node->SetHunkErasureCodec(hunkErasureCodec);

        if (node->IsPhysicallyLog()) {
            // NB: This setting may be not visible in attributes but crucial for replication
            // to work properly.
            node->SetCommitOrdering(NTransactionClient::ECommitOrdering::Strong);
        }

        if (effectiveTableSchema) {
            tableManager->GetOrCreateMasterTableSchema(*effectiveTableSchema, node);
        } else {
            auto* emptySchema = tableManager->GetEmptyMasterTableSchema();
            tableManager->SetTableSchema(node, emptySchema);
        }

        if (effectiveTableSchema) {
            node->SetSchemaMode(ETableSchemaMode::Strong);
        }

        // NB: Dynamic table should have a bundle during creation for accounting to work properly.
        tabletManager->SetTabletCellBundle(node, tabletCellBundle);

        if (dynamic) {
            if (node->IsNative()) {
                tabletManager->ValidateMakeTableDynamic(node);
            }

            tabletManager->MakeTableDynamic(node);

            if (node->IsQueueObject()) {
                tableManager->RegisterQueue(node);
            }

            if (node->IsConsumerObject()) {
                tableManager->RegisterConsumer(node);
            }

            if (node->IsNative()) {
                if (optionalTabletCount) {
                    tabletManager->PrepareReshardTable(node, 0, 0, *optionalTabletCount, {}, true);
                } else if (optionalPivotKeys) {
                    tabletManager->PrepareReshardTable(node, 0, 0, optionalPivotKeys->size(), *optionalPivotKeys, true);
                }
            }

            if (!node->IsExternal()) {
                if (optionalTabletCount) {
                    tabletManager->ReshardTable(node, 0, 0, *optionalTabletCount, {});
                } else if (optionalPivotKeys) {
                    tabletManager->ReshardTable(node, 0, 0, optionalPivotKeys->size(), *optionalPivotKeys);
                }
            }

            node->SetUpstreamReplicaId(upstreamReplicaId);
        }
    } catch (const std::exception&) {
        this->Destroy(node);
        throw;
    }

    return nodeHolder;
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoDestroy(TImpl* table)
{
    const auto& tableManager = this->Bootstrap_->GetTableManager();

    if (table->IsQueueObject()) {
        tableManager->UnregisterQueue(table);
    }

    if (table->IsConsumerObject()) {
        tableManager->UnregisterConsumer(table);
    }

    if (table->IsTrunk()) {
        const auto& tabletManager = this->Bootstrap_->GetTabletManager();
        tabletManager->DestroyTable(table);
    }

    tableManager->ResetTableSchema(table);

    TBase::DoDestroy(table);
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const TLockRequest& lockRequest)
{
    const auto& tableManager = this->Bootstrap_->GetTableManager();
    tableManager->SetTableSchema(branchedNode, originatingNode->GetSchema());

    branchedNode->SetSchemaMode(originatingNode->GetSchemaMode());
    branchedNode->SetOptimizeFor(originatingNode->GetOptimizeFor());
    branchedNode->SetHunkErasureCodec(originatingNode->GetHunkErasureCodec());
    branchedNode->SetProfilingMode(originatingNode->GetProfilingMode());
    branchedNode->SetProfilingTag(originatingNode->GetProfilingTag());

    // Save current retained and unflushed timestamps in locked node.
    branchedNode->SetRetainedTimestamp(originatingNode->GetCurrentRetainedTimestamp());
    branchedNode->SetUnflushedTimestamp(originatingNode->GetCurrentUnflushedTimestamp(lockRequest.Timestamp));

    TBase::DoBranch(originatingNode, branchedNode, lockRequest);
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    const auto& tableManager = this->Bootstrap_->GetTableManager();

    bool isQueueObjectBefore = originatingNode->IsQueueObject();

    tableManager->SetTableSchema(originatingNode, branchedNode->GetSchema());
    tableManager->ResetTableSchema(branchedNode);

    originatingNode->SetSchemaMode(branchedNode->GetSchemaMode());
    originatingNode->MergeOptimizeFor(branchedNode);
    originatingNode->MergeHunkErasureCodec(branchedNode);
    originatingNode->SetProfilingMode(branchedNode->GetProfilingMode());
    originatingNode->SetProfilingTag(branchedNode->GetProfilingTag());

    TBase::DoMerge(originatingNode, branchedNode);

    bool isQueueObjectAfter = originatingNode->IsQueueObject();
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
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    const auto& tabletManager = this->Bootstrap_->GetTabletManager();
    tabletManager->ValidateCloneTable(
        sourceNode,
        mode,
        account);

    TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

    // NB: Dynamic table should have a bundle during creation for accounting to work properly.
    auto* trunkSourceNode = sourceNode->GetTrunkNode();
    tabletManager->SetTabletCellBundle(clonedTrunkNode, trunkSourceNode->TabletCellBundle().Get());

    if (sourceNode->IsDynamic()) {
        tabletManager->CloneTable(
            sourceNode,
            clonedTrunkNode,
            mode);
    }

    const auto& tableManager = this->Bootstrap_->GetTableManager();
    tableManager->SetTableSchema(clonedTrunkNode, sourceNode->GetSchema());

    clonedTrunkNode->SetSchemaMode(sourceNode->GetSchemaMode());
    clonedTrunkNode->SetOptimizeFor(sourceNode->GetOptimizeFor());

    if (trunkSourceNode->HasCustomDynamicTableAttributes()) {
        clonedTrunkNode->InitializeCustomDynamicTableAttributes();
        clonedTrunkNode->GetCustomDynamicTableAttributes()->CopyFrom(
            trunkSourceNode->GetCustomDynamicTableAttributes());
    }

    if (clonedTrunkNode->IsQueueObject()) {
        tableManager->RegisterQueue(clonedTrunkNode);
    }
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoBeginCopy(
    TImpl* node,
    TBeginCopyContext* context)
{
    TBase::DoBeginCopy(node, context);

    const auto& tabletManager = this->Bootstrap_->GetTabletManager();
    tabletManager->ValidateBeginCopyTable(node, context->GetMode());

    // TODO(babenko): support copying dynamic tables
    if (node->IsDynamic()) {
        THROW_ERROR_EXCEPTION("Dynamic tables do not support cross-cell copying");
    }

    using NYT::Save;
    auto* trunkNode = node->GetTrunkNode();
    Save(*context, trunkNode->TabletCellBundle());

    Save(*context, node->GetSchema());

    Save(*context, node->GetSchemaMode());
    Save(*context, node->GetOptimizeFor());
    Save(*context, node->GetHunkErasureCodec());

    Save(*context, trunkNode->HasCustomDynamicTableAttributes());
    if (trunkNode->HasCustomDynamicTableAttributes()) {
        trunkNode->GetCustomDynamicTableAttributes()->BeginCopy(context);
    }
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoEndCopy(
    TImpl* node,
    TEndCopyContext* context,
    ICypressNodeFactory* factory)
{
    TBase::DoEndCopy(node, context, factory);

    const auto& tabletManager = this->Bootstrap_->GetTabletManager();
    // TODO(babenko): support copying dynamic tables

    using NYT::Load;

    if (auto* bundle = Load<TTabletCellBundle*>(*context)) {
        const auto& objectManager = this->Bootstrap_->GetObjectManager();
        objectManager->ValidateObjectLifeStage(bundle);
        tabletManager->SetTabletCellBundle(node, bundle);
    }

    const auto& tableManager = this->Bootstrap_->GetTableManager();
    auto* schema = Load<TMasterTableSchema*>(*context);
    tableManager->SetTableSchema(node, schema);

    node->SetSchemaMode(Load<ETableSchemaMode>(*context));
    node->SetOptimizeFor(Load<EOptimizeFor>(*context));
    node->SetHunkErasureCodec(Load<NErasure::ECodec>(*context));

    if (Load<bool>(*context)) {
        node->InitializeCustomDynamicTableAttributes();
        node->GetCustomDynamicTableAttributes()->EndCopy(context);
    }

    // TODO(achulkov2): Add corresponding test once copying dynamic tables is supported. Please ping me :)
    if (node->IsQueueObject()) {
        tableManager->RegisterQueue(node);
    }
}

template<class TImpl>
bool TTableNodeTypeHandlerBase<TImpl>::IsSupportedInheritableAttribute(const TString& key) const
{
    static const THashSet<TString> SupportedInheritableAttributes{
        "atomicity",
        "commit_ordering",
        "in_memory_mode",
        "optimize_for",
        "hunk_erasure_codec",
        "tablet_cell_bundle",
        "profiling_mode",
        "profiling_tag"
    };

    if (SupportedInheritableAttributes.contains(key)) {
        return true;
    }

    return TBase::IsSupportedInheritableAttribute(key);
}

template <class TImpl>
std::optional<std::vector<TString>> TTableNodeTypeHandlerBase<TImpl>::DoListColumns(TImpl* node) const
{
    const auto& schema = *node->GetSchema()->AsTableSchema();

    std::vector<TString> result;
    result.reserve(schema.Columns().size());
    for (const auto& column : schema.Columns()) {
        result.push_back(column.Name());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

EObjectType TTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::Table;
}

ICypressNodeProxyPtr TTableNodeTypeHandler::DoGetProxy(
    TTableNode* trunkNode,
    TTransaction* transaction)
{
    return CreateTableNodeProxy(
        Bootstrap_,
        &Metadata_,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

EObjectType TReplicationLogTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::ReplicationLogTable;
}

////////////////////////////////////////////////////////////////////////////////

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
        Bootstrap_,
        &Metadata_,
        transaction,
        trunkNode);
}

void TReplicatedTableNodeTypeHandler::DoBeginCopy(
    TReplicatedTableNode* /*node*/,
    TBeginCopyContext* /*context*/)
{
    // TODO(babenko): support cross-cell copy for replicated tables
    THROW_ERROR_EXCEPTION("Replicated tables do not support cross-cell copying");
}

void TReplicatedTableNodeTypeHandler::DoEndCopy(
    TReplicatedTableNode* /*node*/,
    TEndCopyContext* /*context*/,
    ICypressNodeFactory* /*factory*/)
{
    // TODO(babenko): support cross-cell copy for replicated tables
    THROW_ERROR_EXCEPTION("Replicated tables do not support cross-cell copying");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

