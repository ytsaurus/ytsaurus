#include "schemaful_node_type_handler.h"

#include <yt/yt/server/master/chaos_server/chaos_replicated_table_node.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/master/table_server/replicated_table_node.h>
#include <yt/yt/server/master/table_server/table_manager.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NChaosServer;
using namespace NCypressServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TSchemafulNodeTypeHandlerBase<TImpl>::TSchemafulNodeTypeHandlerBase(TBootstrap* bootstrap)
    : TBase(bootstrap)
{
    // NB: Due to virtual inheritance bootstrap has to be explicitly initialized.
    this->SetBootstrap(bootstrap);
}

template <class TImpl>
TMasterTableSchema* TSchemafulNodeTypeHandlerBase<TImpl>::DoFindSchema(TImpl* schemafulNode) const
{
    auto* node = static_cast<TSchemafulNode*>(schemafulNode);
    return node->GetSchema();
}

template <class TImpl>
void TSchemafulNodeTypeHandlerBase<TImpl>::DoZombify(TImpl* schemafulNode)
{
    TBase::DoZombify(schemafulNode);
}

template <class TImpl>
void TSchemafulNodeTypeHandlerBase<TImpl>::DoDestroy(TImpl* schemafulNode)
{
    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    tableManager->ResetTableSchema(schemafulNode);

    TBase::DoDestroy(schemafulNode);
}

template <class TImpl>
void TSchemafulNodeTypeHandlerBase<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const NCypressServer::TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    tableManager->SetTableSchema(branchedNode, originatingNode->GetSchema());
    branchedNode->SetSchemaMode(originatingNode->GetSchemaMode());
}

template <class TImpl>
void TSchemafulNodeTypeHandlerBase<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    tableManager->SetTableSchema(originatingNode, branchedNode->GetSchema());
    originatingNode->SetSchemaMode(branchedNode->GetSchemaMode());
    tableManager->ResetTableSchema(branchedNode);

    TBase::DoMerge(originatingNode, branchedNode);
}

template <class TImpl>
void TSchemafulNodeTypeHandlerBase<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    NCypressServer::ICypressNodeFactory* factory,
    NCypressServer::ENodeCloneMode mode,
    NSecurityServer::TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    tableManager->SetTableSchema(clonedTrunkNode, sourceNode->GetSchema());
    clonedTrunkNode->SetSchemaMode(sourceNode->GetSchemaMode());
}

template <class TImpl>
void TSchemafulNodeTypeHandlerBase<TImpl>::DoBeginCopy(
    TImpl* schemafulNode,
    NCypressServer::TBeginCopyContext* context)
{
    TBase::DoBeginCopy(schemafulNode, context);

    Save(*context, schemafulNode->GetSchema());
    Save(*context, schemafulNode->GetSchemaMode());
}

template <class TImpl>
void TSchemafulNodeTypeHandlerBase<TImpl>::DoEndCopy(
    TImpl* schemafulNode,
    NCypressServer::TEndCopyContext* context,
    NCypressServer::ICypressNodeFactory* factory)
{
    TBase::DoEndCopy(schemafulNode, context, factory);

    const auto& tableManager = this->GetBootstrap()->GetTableManager();
    auto* schema = Load<TMasterTableSchema*>(*context);
    tableManager->SetTableSchema(schemafulNode, schema);
    schemafulNode->SetSchemaMode(Load<ETableSchemaMode>(*context));

}

////////////////////////////////////////////////////////////////////////////////

template class TSchemafulNodeTypeHandlerBase<TChaosReplicatedTableNode>;
template class TSchemafulNodeTypeHandlerBase<TReplicatedTableNode>;
template class TSchemafulNodeTypeHandlerBase<TTableNode>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
