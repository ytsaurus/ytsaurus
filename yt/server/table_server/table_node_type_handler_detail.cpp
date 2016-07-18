#include "table_node_type_handler_detail.h"
#include "table_node_proxy.h"
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

#include <yt/ytlib/chunk_client/schema.h>

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
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NTabletServer;

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

std::unique_ptr<TTableNode> TTableNodeTypeHandler::DoCreate(
    const TVersionedNodeId& id,
    TCellTag cellTag,
    TTransaction* transaction,
    IAttributeDictionary* attributes)
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

    auto maybeSchema = attributes->FindAndRemove<TTableSchema>("schema");

    if (maybeSchema) {
        // NB: Sorted dynamic tables contain unique keys, set this for user.
        if (dynamic && maybeSchema->IsSorted() && !maybeSchema->GetUniqueKeys()) {
             maybeSchema = maybeSchema->ToUniqueKeys();
        }

        ValidateTableSchemaUpdate(TTableSchema(), *maybeSchema, dynamic, true);
    }

    if (dynamic && !maybeSchema) {
        THROW_ERROR_EXCEPTION("\"schema\" is mandatory for dynamic tables");
    }

    TBase::InitializeAttributes(attributes);

    auto nodeHolder = TChunkOwnerTypeHandler::DoCreate(
        id,
        cellTag,
        transaction,
        attributes);
    auto* node = nodeHolder.get();

    try {
        if (maybeSchema) {
            node->TableSchema() = *maybeSchema;
            node->SetSchemaMode(ESchemaMode::Strong);
        }

        if (dynamic) {
            auto tabletManager = Bootstrap_->GetTabletManager();
            tabletManager->MakeTableDynamic(node);
        }
    } catch (...) {
        DoDestroy(node);
        throw;
    }

    return nodeHolder;
}

void TTableNodeTypeHandler::DoDestroy(TTableNode* table)
{
    TBase::DoDestroy(table);

    if (table->IsTrunk()) {
        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->DestroyTable(table);
    }
}

void TTableNodeTypeHandler::DoBranch(
    const TTableNode* originatingNode,
    TTableNode* branchedNode,
    ELockMode mode)
{
    branchedNode->TableSchema() = originatingNode->TableSchema();
    branchedNode->SetSchemaMode(originatingNode->GetSchemaMode());

    TBase::DoBranch(originatingNode, branchedNode, mode);
}

void TTableNodeTypeHandler::DoMerge(
    TTableNode* originatingNode,
    TTableNode* branchedNode)
{
    originatingNode->TableSchema() = branchedNode->TableSchema();
    originatingNode->SetSchemaMode(branchedNode->GetSchemaMode());

    TBase::DoMerge(originatingNode, branchedNode);
}

void TTableNodeTypeHandler::DoClone(
    TTableNode* sourceNode,
    TTableNode* clonedNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode)
{
    auto tabletManager = Bootstrap_->GetTabletManager();

    TBase::DoClone(sourceNode, clonedNode, factory, mode);

    if (sourceNode->IsDynamic()) {
        auto data = tabletManager->BeginCloneTable(sourceNode, clonedNode, mode);
        factory->RegisterCommitHandler([sourceNode, clonedNode, tabletManager, data] () {
            tabletManager->CommitCloneTable(sourceNode, clonedNode, data);
        });
        factory->RegisterRollbackHandler([sourceNode, clonedNode, tabletManager, data] () {
            tabletManager->RollbackCloneTable(sourceNode, clonedNode, data);
        });
    }

    clonedNode->TableSchema() = sourceNode->TableSchema();
    clonedNode->SetSchemaMode(sourceNode->GetSchemaMode());
    clonedNode->SetAtomicity(sourceNode->GetAtomicity());
    clonedNode->SetLastCommitTimestamp(sourceNode->GetLastCommitTimestamp());

    auto* trunkSourceNode = sourceNode->GetTrunkNode();
    tabletManager->SetTabletCellBundle(clonedNode, trunkSourceNode->GetTabletCellBundle());
}

int TTableNodeTypeHandler::GetDefaultReplicationFactor() const
{
    return Bootstrap_->GetConfig()->CypressManager->DefaultTableReplicationFactor;
}

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableNodeTypeHandler::TReplicatedTableNodeTypeHandler(TBootstrap* bootstrap)
    : TTableNodeTypeHandler(bootstrap)
{ }

EObjectType TReplicatedTableNodeTypeHandler::GetObjectType() const
{
    return EObjectType::ReplicatedTable;
}

ICypressNodeProxyPtr TReplicatedTableNodeTypeHandler::DoGetProxy(
    TTableNode* trunkNode,
    TTransaction* transaction)
{
    return CreateReplicatedTableNodeProxy(
        Bootstrap_,
        &Metadata_,
        transaction,
        trunkNode);
}

std::unique_ptr<TTableNode> TReplicatedTableNodeTypeHandler::DoCreate(
    const TVersionedNodeId& id,
    TCellTag cellTag,
    TTransaction* transaction,
    IAttributeDictionary* attributes)
{
    return TTableNodeTypeHandler::DoCreate(
        id,
        cellTag,
        transaction,
        attributes);
}

void TReplicatedTableNodeTypeHandler::DoDestroy(TTableNode* table)
{
    TTableNodeTypeHandler::DoDestroy(table);

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

