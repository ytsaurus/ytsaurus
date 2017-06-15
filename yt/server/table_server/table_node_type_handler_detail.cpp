#include "table_node_type_handler_detail.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "replicated_table_node.h"
#include "replicated_table_node_proxy.h"
#include "private.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/chunk_owner_type_handler.h>

#include <yt/server/tablet_server/tablet.h>
#include <yt/server/tablet_server/tablet_manager.h>

namespace NYT {
namespace NTableServer {

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

using NTabletNode::EInMemoryMode;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TTableNodeTypeHandlerBase<TImpl>::TTableNodeTypeHandlerBase(TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

template <class TImpl>
std::unique_ptr<TImpl> TTableNodeTypeHandlerBase<TImpl>::DoCreate(
    const TVersionedNodeId& id,
    TCellTag cellTag,
    TTransaction* transaction,
    IAttributeDictionary* attributes,
    TAccount* account,
    bool enableAccounting)
{
    if (!attributes->Contains("compression_codec")) {
        attributes->Set("compression_codec", NCompression::ECodec::Lz4);
    }

    if (!attributes->Contains("optimize_for")) {
        attributes->Set("optimize_for", EOptimizeFor::Lookup);
    }

    if (!attributes->Contains("tablet_cell_bundle")) {
        attributes->Set("tablet_cell_bundle", DefaultTabletCellBundleName);
    }

    bool dynamic = attributes->GetAndRemove<bool>("dynamic", false);
    bool replicated = TypeFromId(id.ObjectId) == EObjectType::ReplicatedTable;

    if (replicated && !dynamic) {
        THROW_ERROR_EXCEPTION("Replicated table must be dynamic");
    }

    auto maybeSchema = attributes->FindAndRemove<TTableSchema>("schema");

    if (dynamic && !maybeSchema) {
        THROW_ERROR_EXCEPTION("\"schema\" is mandatory for dynamic tables");
    }

    if (replicated) {
        if (!dynamic) {
            THROW_ERROR_EXCEPTION("Replicated table must be dynamic");
        }
        if (!maybeSchema->IsSorted()) {
            THROW_ERROR_EXCEPTION("Replicated table must be sorted");
        }
    }

    if (maybeSchema) {
        // NB: Sorted dynamic tables contain unique keys, set this for user.
        if (dynamic && maybeSchema->IsSorted() && !maybeSchema->GetUniqueKeys()) {
             maybeSchema = maybeSchema->ToUniqueKeys();
        }

        ValidateTableSchemaUpdate(TTableSchema(), *maybeSchema, dynamic, true);
    }

    auto maybeTabletCount = attributes->FindAndRemove<int>("tablet_count");
    auto maybePivotKeys = attributes->FindAndRemove<std::vector<TOwningKey>>("pivot_keys");
    if (maybeTabletCount && maybePivotKeys) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"tablet_count\" and \"pivot_keys\"");
    }
    auto upstreamReplicaId = attributes->GetAndRemove<TTableReplicaId>("upstream_replica_id", TTableReplicaId());
    if (upstreamReplicaId) {
        if (!dynamic) {
            THROW_ERROR_EXCEPTION("Upstream replica can only be set for dynamic tables");
        }
        if (!maybeSchema->IsSorted()) {
            THROW_ERROR_EXCEPTION("Upstream replica can only be set for sorted tables");
        }
        if (replicated) {
            THROW_ERROR_EXCEPTION("Upstream replica cannot be set for replicated tables");
        }
    }

    TBase::InitializeAttributes(attributes);

    auto nodeHolder = TBase::DoCreate(
        id,
        cellTag,
        transaction,
        attributes,
        account,
        enableAccounting);
    auto* node = nodeHolder.get();

    try {
        if (node->IsReplicated()) {
            // NB: This setting is not visible in attributes but crucial for replication
            // to work properly.
            node->SetCommitOrdering(NTransactionClient::ECommitOrdering::Strong);
        }

        if (maybeSchema) {
            node->TableSchema() = *maybeSchema;
            node->SetSchemaMode(ETableSchemaMode::Strong);
        }

        if (dynamic) {
            const auto& tabletManager = this->Bootstrap_->GetTabletManager();
            tabletManager->MakeTableDynamic(node);

            if (maybeTabletCount) {
                tabletManager->ReshardTable(node, 0, 0, *maybeTabletCount, {});
            } else if (maybePivotKeys) {
                tabletManager->ReshardTable(node, 0, 0, maybePivotKeys->size(), *maybePivotKeys);
            }

            node->SetUpstreamReplicaId(upstreamReplicaId);
        }
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
    ELockMode mode)
{
    branchedNode->TableSchema() = originatingNode->TableSchema();
    branchedNode->SetSchemaMode(originatingNode->GetSchemaMode());
    branchedNode->SetRetainedTimestamp(originatingNode->GetCurrentRetainedTimestamp());
    branchedNode->SetUnflushedTimestamp(originatingNode->GetCurrentUnflushedTimestamp());
    branchedNode->SetUpstreamReplicaId(originatingNode->GetUpstreamReplicaId());

    TBase::DoBranch(originatingNode, branchedNode, mode);
}

template <class TImpl>
void TTableNodeTypeHandlerBase<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    originatingNode->TableSchema() = branchedNode->TableSchema();
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
    const auto& securityManager = this->Bootstrap_->GetSecurityManager();
    securityManager->ValidateResourceUsageIncrease(
        account,
        TClusterResources().SetTabletCount(sourceNode->GetTrunkNode()->Tablets().size()));

    const auto& tabletManager = this->Bootstrap_->GetTabletManager();

    TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

    if (sourceNode->IsDynamic()) {
        tabletManager->CloneTable(
            sourceNode,
            clonedNode,
            factory->GetTransaction(),
            mode);

        clonedNode->SetLastCommitTimestamp(sourceNode->GetLastCommitTimestamp());
    }

    clonedNode->TableSchema() = sourceNode->TableSchema();
    clonedNode->SetSchemaMode(sourceNode->GetSchemaMode());
    clonedNode->SetAtomicity(sourceNode->GetAtomicity());
    clonedNode->SetRetainedTimestamp(sourceNode->GetRetainedTimestamp());
    clonedNode->SetUnflushedTimestamp(sourceNode->GetUnflushedTimestamp());
    clonedNode->SetUpstreamReplicaId(sourceNode->GetUpstreamReplicaId());
    clonedNode->SetOptimizeFor(sourceNode->GetOptimizeFor());

    auto* trunkSourceNode = sourceNode->GetTrunkNode();
    tabletManager->SetTabletCellBundle(clonedNode, trunkSourceNode->GetTabletCellBundle());
}

template <class TImpl>
int TTableNodeTypeHandlerBase<TImpl>::GetDefaultReplicationFactor() const
{
    return this->Bootstrap_->GetConfig()->CypressManager->DefaultTableReplicationFactor;
}

template <class TImpl>
TClusterResources TTableNodeTypeHandlerBase<TImpl>::GetTabletResourceUsage(
    const TCypressNodeBase* node)
{
    int tabletCount = 0;
    i64 memorySize = 0;

    if (node->IsTrunk()) {
        const auto* table = node->As<TImpl>();
        tabletCount = table->Tablets().size();
        for (const auto* tablet : table->Tablets()) {
            if (tablet->GetState() != ETabletState::Unmounted) {
                memorySize += tablet->GetTabletStaticMemorySize();
            }
        }
    }

    return TClusterResources()
        .SetTabletCount(tabletCount)
        .SetTabletStaticMemory(memorySize);
}

template <class TImpl>
TClusterResources TTableNodeTypeHandlerBase<TImpl>::GetTotalResourceUsage(
    const TCypressNodeBase* node)
{
    return TBase::GetTotalResourceUsage(node) + GetTabletResourceUsage(node);
}

template <class TImpl>
TClusterResources TTableNodeTypeHandlerBase<TImpl>::GetAccountingResourceUsage(
    const TCypressNodeBase* node)
{
    return TBase::GetAccountingResourceUsage(node) + GetTabletResourceUsage(node);
}

////////////////////////////////////////////////////////////////////////////////

TTableNodeTypeHandler::TTableNodeTypeHandler(TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

EObjectType TTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::Table;
}

bool TTableNodeTypeHandler::IsExternalizable() const
{
    return true;
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

} // namespace NTableServer
} // namespace NYT

