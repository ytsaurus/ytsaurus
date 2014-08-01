#ifndef CHUNK_OWNER_TYPE_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_owner_type_handler.h"
#endif
#undef CHUNK_OWNER_TYPE_HANDLER_INL_H_

#include <core/erasure/public.h>

#include <server/cypress_server/node.h>
#include <server/cypress_server/node_detail.h>
#include <server/cypress_server/cypress_manager.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/hydra_facade.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkOwner>
TChunkOwnerTypeHandler<TChunkOwner>::TChunkOwnerTypeHandler(NCellMaster::TBootstrap* bootstrap)
    : TBase(bootstrap)
    , Logger(ChunkServerLogger)
{ }

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::SetDefaultAttributes(
    NYTree::IAttributeDictionary* attributes,
    NTransactionServer::TTransaction* transaction)
{
    TBase::SetDefaultAttributes(attributes, transaction);

    if (!attributes->Contains("replication_factor")) {
        attributes->Set("replication_factor", NChunkClient::DefaultReplicationFactor);
    }

    if (!attributes->Contains("erasure_codec")) {
        attributes->SetYson(
            "erasure_codec",
            NYTree::ConvertToYsonString(NErasure::ECodec(NErasure::ECodec::None)));
    }
}

template <class TChunkOwner>
NYTree::ENodeType TChunkOwnerTypeHandler<TChunkOwner>::GetNodeType()
{
    return NYTree::ENodeType::Entity;
}

template <class TChunkOwner>
std::unique_ptr<TChunkOwner> TChunkOwnerTypeHandler<TChunkOwner>::DoCreate(
    const NCypressServer::TVersionedNodeId& id,
    NTransactionServer::TTransaction* transaction,
    NCypressServer::INodeTypeHandler::TReqCreate* request,
    NCypressServer::INodeTypeHandler::TRspCreate* response)
{
    auto chunkManager = this->Bootstrap->GetChunkManager();
    auto objectManager = this->Bootstrap->GetObjectManager();

    auto node = TBase::DoCreate(id, transaction, request, response);

    // Create an empty chunk list and reference it from the node.
    auto* chunkList = chunkManager->CreateChunkList();
    node->SetChunkList(chunkList);
    YCHECK(chunkList->OwningNodes().insert(node.get()).second);
    objectManager->RefObject(chunkList);

    return node;
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoDestroy(TChunkOwner* node)
{
    TBase::DoDestroy(node);

    auto objectManager = TBase::Bootstrap->GetObjectManager();

    auto* chunkList = node->GetChunkList();
    YCHECK(chunkList->OwningNodes().erase(node) == 1);
    objectManager->UnrefObject(chunkList);
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoBranch(
    const TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    TBase::DoBranch(originatingNode, branchedNode);

    auto objectManager = TBase::Bootstrap->GetObjectManager();

    auto* chunkList = originatingNode->GetChunkList();

    branchedNode->SetChunkList(chunkList);
    objectManager->RefObject(branchedNode->GetChunkList());
    YCHECK(branchedNode->GetChunkList()->OwningNodes().insert(branchedNode).second);

    branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());
    branchedNode->SetVital(originatingNode->GetVital());

    LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Chunk owner node branched (BranchedNodeId: %v, ChunkListId: %v, ReplicationFactor: %v)",
        ~ToString(branchedNode->GetId()),
        ~ToString(originatingNode->GetChunkList()->GetId()),
        originatingNode->GetReplicationFactor());
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoMerge(
    TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    auto originatingChunkListId = originatingNode->GetChunkList()->GetId();
    auto branchedChunkListId = branchedNode->GetChunkList()->GetId();

    auto originatingUpdateMode = originatingNode->GetUpdateMode();
    auto branchedUpdateMode = branchedNode->GetUpdateMode();

    MergeChunkLists(originatingNode, branchedNode);

    LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Chunk owner node merged (OriginatingNodeId: %v, OriginatingChunkListId: %v, OriginatingUpdateMode: %v, OriginatingReplicationFactor: %v, "
        "BranchedNodeId: %v, BranchedChunkListId: %v, BranchedUpdateMode: %v, BranchedReplicationFactor: %v, "
        "NewOriginatingChunkListId: %v, NewOriginatingUpdateMode: %v)",
        ~ToString(originatingNode->GetVersionedId()),
        ~ToString(originatingChunkListId),
        ~ToString(originatingUpdateMode),
        originatingNode->GetReplicationFactor(),
        ~ToString(branchedNode->GetVersionedId()),
        ~ToString(branchedChunkListId),
        ~ToString(branchedUpdateMode),
        branchedNode->GetReplicationFactor(),
        ~ToString(originatingNode->GetChunkList()->GetId()),
        ~ToString(originatingNode->GetUpdateMode()));
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::MergeChunkLists(
    TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    auto hydraManager = TBase::Bootstrap->GetHydraFacade()->GetHydraManager();
    auto chunkManager = TBase::Bootstrap->GetChunkManager();
    auto objectManager = TBase::Bootstrap->GetObjectManager();

    auto* originatingChunkList = originatingNode->GetChunkList();
    auto* branchedChunkList = branchedNode->GetChunkList();

    auto originatingMode = originatingNode->GetUpdateMode();
    auto branchedMode = branchedNode->GetUpdateMode();

    YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);

    // Check if we have anything to do at all.
    if (branchedMode == NChunkClient::EUpdateMode::None) {
        objectManager->UnrefObject(branchedChunkList);
        return;
    }

    bool isTopmostCommit = !originatingNode->GetTransaction();
    bool hasPropertiesChanged =
        originatingNode->GetReplicationFactor() != branchedNode->GetReplicationFactor() ||
        originatingNode->GetVital() != branchedNode->GetVital();
    bool isPropertiesUpdateNeeded = isTopmostCommit && hasPropertiesChanged && hydraManager->IsLeader();

    if (branchedMode == NChunkClient::EUpdateMode::Overwrite) {
        YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
        YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
        originatingNode->SetChunkList(branchedChunkList);

        if (isPropertiesUpdateNeeded) {
            chunkManager->ScheduleChunkPropertiesUpdate(branchedChunkList);
        }

        objectManager->UnrefObject(originatingChunkList);
    } else {
        YCHECK(branchedMode == NChunkClient::EUpdateMode::Append);
        YCHECK(branchedChunkList->Children().size() == 2);
        auto deltaRef = branchedChunkList->Children()[1];

        auto* newOriginatingChunkList = chunkManager->CreateChunkList();

        YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
        YCHECK(newOriginatingChunkList->OwningNodes().insert(originatingNode).second);

        originatingNode->SetChunkList(newOriginatingChunkList);
        objectManager->RefObject(newOriginatingChunkList);

        if (originatingMode == NChunkClient::EUpdateMode::Append) {
            YCHECK(!isTopmostCommit); // No need to update properties.
            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList->Children()[0]);
            auto* newDeltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newOriginatingChunkList, newDeltaChunkList);
            chunkManager->AttachToChunkList(newDeltaChunkList, originatingChunkList->Children()[1]);
            chunkManager->AttachToChunkList(newDeltaChunkList, deltaRef);
        } else {
            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList);
            chunkManager->AttachToChunkList(newOriginatingChunkList, deltaRef);

            if (isPropertiesUpdateNeeded) {
                chunkManager->ScheduleChunkPropertiesUpdate(deltaRef);
            }
        }

        objectManager->UnrefObject(originatingChunkList);
        objectManager->UnrefObject(branchedChunkList);
    }

    if (isTopmostCommit) {
        // Originating mode must remain None.
        // Rebalance when the topmost transaction commits.
        chunkManager->RebalanceChunkTree(originatingNode->GetChunkList());
    } else {
        // Set proper originating mode.
        originatingNode->SetUpdateMode(
            originatingMode == NChunkClient::EUpdateMode::Overwrite || branchedMode == NChunkClient::EUpdateMode::Overwrite
            ? NChunkClient::EUpdateMode::Overwrite
            : NChunkClient::EUpdateMode::Append);
    }
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoClone(
    TChunkOwner* sourceNode,
    TChunkOwner* clonedNode,
    NCypressServer::ICypressNodeFactoryPtr factory)
{
    TBase::DoClone(sourceNode, clonedNode, factory);

    auto objectManager = TBase::Bootstrap->GetObjectManager();

    auto* chunkList = sourceNode->GetChunkList();
    YCHECK(!clonedNode->GetChunkList());
    clonedNode->SetChunkList(chunkList);
    clonedNode->SetReplicationFactor(sourceNode->GetReplicationFactor());
    clonedNode->SetVital(sourceNode->GetVital());
    objectManager->RefObject(chunkList);
    YCHECK(chunkList->OwningNodes().insert(clonedNode).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
