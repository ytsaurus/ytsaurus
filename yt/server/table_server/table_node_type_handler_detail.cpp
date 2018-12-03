#include "table_node_type_handler_detail.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "replicated_table_node.h"
#include "replicated_table_node_proxy.h"
#include "shared_table_schema.h"
#include "private.h"

#include <yt/ytlib/table_client/schema.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/chunk_owner_type_handler.h>

#include <yt/server/tablet_server/tablet.h>
#include <yt/server/tablet_server/tablet_manager.h>

namespace NYT::NTableServer {

using namespace NTableClient;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TTableNodeTypeHandlerBase<TImpl>::TTableNodeTypeHandlerBase(TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

template <class TImpl>
bool TTableNodeTypeHandlerBase<TImpl>::HasBranchedChangesImpl(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode))  {
        return true;
    }

    if (branchedNode->IsDynamic()) {
        YCHECK(originatingNode->IsDynamic());
        // One may consider supporting unlocking unmounted dynamic tables.
        // However, it isn't immediately obvious why that should be useful and
        // allowing to unlock something always requires careful consideration.
        return true;
    }

    return false;
}

template <class TImpl>
std::unique_ptr<TImpl> TTableNodeTypeHandlerBase<TImpl>::DoCreate(
    const TVersionedNodeId& id,
    TCellTag cellTag,
    TTransaction* transaction,
    IAttributeDictionary* inheritedAttributes,
    IAttributeDictionary* explicitAttributes,
    TAccount* account)
{
    const auto& config = this->Bootstrap_->GetConfig()->CypressManager;

    auto combinedAttributes = OverlayAttributeDictionaries(explicitAttributes, inheritedAttributes);
    auto optionalTabletCellBundleName = combinedAttributes.FindAndRemove<TString>("tablet_cell_bundle");
    auto optimizeFor = combinedAttributes.GetAndRemove<EOptimizeFor>("optimize_for", EOptimizeFor::Lookup);
    auto replicationFactor = combinedAttributes.GetAndRemove("replication_factor", config->DefaultTableReplicationFactor);
    auto compressionCodec = combinedAttributes.GetAndRemove<NCompression::ECodec>("compression_codec", NCompression::ECodec::Lz4);
    auto erasureCodec = combinedAttributes.GetAndRemove<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

    ValidateReplicationFactor(replicationFactor);

    bool dynamic = combinedAttributes.GetAndRemove<bool>("dynamic", false);
    bool replicated = TypeFromId(id.ObjectId) == EObjectType::ReplicatedTable;

    if (replicated && !dynamic) {
        THROW_ERROR_EXCEPTION("Replicated table must be dynamic");
    }

    auto optionalSchema = combinedAttributes.FindAndRemove<TTableSchema>("schema");

    if (dynamic && !optionalSchema) {
        THROW_ERROR_EXCEPTION("\"schema\" is mandatory for dynamic tables");
    }

    if (replicated) {
        if (!dynamic) {
            THROW_ERROR_EXCEPTION("Replicated table must be dynamic");
        }
    }

    if (optionalSchema) {
        // NB: Sorted dynamic tables contain unique keys, set this for user.
        if (dynamic && optionalSchema->IsSorted() && !optionalSchema->GetUniqueKeys()) {
             optionalSchema = optionalSchema->ToUniqueKeys();
        }

        ValidateTableSchemaUpdate(TTableSchema(), *optionalSchema, dynamic, true);
    }

    auto optionalTabletCount = combinedAttributes.FindAndRemove<int>("tablet_count");
    auto optionalPivotKeys = combinedAttributes.FindAndRemove<std::vector<TOwningKey>>("pivot_keys");
    if (optionalTabletCount && optionalPivotKeys) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"tablet_count\" and \"pivot_keys\"");
    }
    auto upstreamReplicaId = combinedAttributes.GetAndRemove<TTableReplicaId>("upstream_replica_id", TTableReplicaId());
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
        ? tabletManager->GetTabletCellBundleByNameOrThrow(*optionalTabletCellBundleName)
        : tabletManager->GetDefaultTabletCellBundle();

    auto nodeHolder = this->DoCreateImpl(
        id,
        cellTag,
        transaction,
        inheritedAttributes,
        explicitAttributes,
        account,
        replicationFactor,
        compressionCodec,
        erasureCodec);
    auto* node = nodeHolder.get();

    try {
        node->SetOptimizeFor(optimizeFor);

        if (node->IsReplicated()) {
            // NB: This setting is not visible in attributes but crucial for replication
            // to work properly.
            node->SetCommitOrdering(NTransactionClient::ECommitOrdering::Strong);
        }

        if (optionalSchema) {
            const auto& registry = this->Bootstrap_->GetCypressManager()->GetSharedTableSchemaRegistry();
            auto sharedSchema = registry->GetSchema(std::move(*optionalSchema));
            node->SharedTableSchema() = sharedSchema;
            node->SetSchemaMode(ETableSchemaMode::Strong);
        }

        if (dynamic) {
            if (!node->IsForeign()) {
                tabletManager->ValidateMakeTableDynamic(node);
            }

            tabletManager->MakeTableDynamic(node);

            if (!node->IsForeign()) {
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

        tabletManager->SetTabletCellBundle(node, tabletCellBundle);
    } catch (const std::exception&) {
        DoDestroy(node);
        throw;
    }

    return nodeHolder;
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoDestroy(TImpl* table)
{
    TBase::DoDestroy(table);

    if (table->IsTrunk()) {
        const auto& tabletManager = this->Bootstrap_->GetTabletManager();
        tabletManager->DestroyTable(table);
    }
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const TLockRequest& lockRequest)
{
    branchedNode->SharedTableSchema() = originatingNode->SharedTableSchema();
    branchedNode->SetSchemaMode(originatingNode->GetSchemaMode());
    branchedNode->SetOptimizeFor(originatingNode->GetOptimizeFor());

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
    originatingNode->SharedTableSchema() = branchedNode->SharedTableSchema();
    originatingNode->SetSchemaMode(branchedNode->GetSchemaMode());
    originatingNode->MergeOptimizeFor(originatingNode, branchedNode);

    TBase::DoMerge(originatingNode, branchedNode);
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    const auto& tabletManager = this->Bootstrap_->GetTabletManager();

    tabletManager->ValidateCloneTable(
        sourceNode,
        clonedNode,
        factory->GetTransaction(),
        mode,
        account);

    TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

    if (sourceNode->IsDynamic()) {
        tabletManager->CloneTable(
            sourceNode,
            clonedNode,
            mode);
    }

    clonedNode->SharedTableSchema() = sourceNode->SharedTableSchema();
    clonedNode->SetSchemaMode(sourceNode->GetSchemaMode());
    clonedNode->SetOptimizeFor(sourceNode->GetOptimizeFor());

    auto* trunkSourceNode = sourceNode->GetTrunkNode();
    clonedNode->SetDynamic(trunkSourceNode->IsDynamic());
    clonedNode->SetAtomicity(trunkSourceNode->GetAtomicity());
    clonedNode->SetCommitOrdering(trunkSourceNode->GetCommitOrdering());
    clonedNode->SetInMemoryMode(trunkSourceNode->GetInMemoryMode());
    clonedNode->SetUpstreamReplicaId(trunkSourceNode->GetUpstreamReplicaId());
    clonedNode->SetLastCommitTimestamp(trunkSourceNode->GetLastCommitTimestamp());
    clonedNode->SetEnableTabletBalancer(trunkSourceNode->GetEnableTabletBalancer());
    clonedNode->SetMinTabletSize(trunkSourceNode->GetMinTabletSize());
    clonedNode->SetMaxTabletSize(trunkSourceNode->GetMaxTabletSize());
    clonedNode->SetDesiredTabletSize(trunkSourceNode->GetDesiredTabletSize());
    clonedNode->SetDesiredTabletCount(trunkSourceNode->GetDesiredTabletCount());

    tabletManager->SetTabletCellBundle(clonedNode, trunkSourceNode->GetTabletCellBundle());
}

template <class TImpl>
bool TTableNodeTypeHandlerBase<TImpl>::IsExternalizable() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TTableNodeTypeHandler::TTableNodeTypeHandler(TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

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

TReplicatedTableNodeTypeHandler::TReplicatedTableNodeTypeHandler(TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

EObjectType TReplicatedTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::ReplicatedTable;
}

bool TReplicatedTableNodeTypeHandler::HasBranchedChangesImpl(
    TReplicatedTableNode* originatingNode,
    TReplicatedTableNode* branchedNode)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

