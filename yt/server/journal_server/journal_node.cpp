#include "stdafx.h"
#include "journal_node.h"
#include "journal_node_proxy.h"
#include "private.h"

#include <server/chunk_server/chunk_owner_type_handler.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialize.h>

namespace NYT {
namespace NJournalServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JournalServerLogger;

////////////////////////////////////////////////////////////////////////////////

TJournalNode::TJournalNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
    , ReadQuorum_(0)
    , WriteQuorum_(0)
{ }

void TJournalNode::Save(NCellMaster::TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, ReadQuorum_);
    Save(context, WriteQuorum_);
}

void TJournalNode::Load(NCellMaster::TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;
    Load(context, ReadQuorum_);
    Load(context, WriteQuorum_);
}

TChunk* TJournalNode::GetTrailingChunk() const
{
    if (!ChunkList_) {
        return nullptr;
    }

    if (ChunkList_->Children().empty()) {
        return nullptr;
    }

    return ChunkList_->Children().back()->AsChunk();
}

bool TJournalNode::IsSealed() const
{
    auto* chunk = GetTrailingChunk();
    return !chunk || chunk->IsSealed();
}

TJournalNode* TJournalNode::GetTrunkNode()
{
    return static_cast<TJournalNode*>(TrunkNode_);
}

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TJournalNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TJournalNode> TBase;

    explicit TJournalNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::Journal;
    }

    virtual bool IsExternalizable() override
    {
        return true;
    }

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

    virtual TClusterResources GetIncrementalResourceUsage(const TCypressNodeBase* node) override
    {
        const auto* journalNode = static_cast<const TJournalNode*>(node);
        return
            TBase::GetIncrementalResourceUsage(node) +
            GetDiskUsage(journalNode->GetChunkList(), journalNode->GetReplicationFactor());
    }

    virtual TClusterResources GetTotalResourceUsage(const TCypressNodeBase* node) override
    {
        return GetIncrementalResourceUsage(node);
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TJournalNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateJournalNodeProxy(
            this,
            Bootstrap_,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TJournalNode> DoCreate(
        const TVersionedNodeId& id,
        TCellTag cellTag,
        IAttributeDictionary* attributes,
        INodeTypeHandler::TReqCreate* request,
        INodeTypeHandler::TRspCreate* response) override
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        auto objectManager = Bootstrap_->GetObjectManager();

        int replicationFactor = attributes->Find<int>("replication_factor").Get(DefaultReplicationFactor);
        attributes->Remove("replication_factor");

        int readQuorum = attributes->Find<int>("read_quorum").Get(DefaultReadQuorum);
        attributes->Remove("read_quorum");

        int writeQuorum = attributes->Find<int>("write_quorum").Get(DefaultWriteQuorum);
        attributes->Remove("write_quorum");

        if (readQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (writeQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (readQuorum + writeQuorum < replicationFactor + 1) {
            THROW_ERROR_EXCEPTION("Read/write quorums are not safe: read_quorum + write_quorum < replication_factor + 1");
        }

        auto nodeHolder = TBase::DoCreate(id, cellTag, attributes, request, response);
        auto* node = nodeHolder.get();

        node->SetReplicationFactor(replicationFactor);
        node->SetReadQuorum(readQuorum);
        node->SetWriteQuorum(writeQuorum);

        if (!node->IsExternal()) {
            // Create an empty chunk list and reference it from the node.
            auto* chunkList = chunkManager->CreateChunkList();
            node->SetChunkList(chunkList);
            YCHECK(chunkList->OwningNodes().insert(node).second);
            objectManager->RefObject(chunkList);
        }

        return nodeHolder;
    }

    virtual void DoDestroy(TJournalNode* node) override
    {
        TBase::DoDestroy(node);

        auto* chunkList = node->GetChunkList();
        if (chunkList) {
            YCHECK(chunkList->OwningNodes().erase(node) == 1);
            auto objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(chunkList);
        }

        if (IsLeader() && !node->IsTrunk() && !node->IsExternal()) {
            ScheduleSeal(node);
        }
    }

    virtual void DoBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        TBase::DoBranch(originatingNode, branchedNode);

        if (!originatingNode->IsExternal()) {
            auto* chunkList = originatingNode->GetChunkList();

            branchedNode->SetChunkList(chunkList);
            YCHECK(branchedNode->GetChunkList()->OwningNodes().insert(branchedNode).second);

            auto objectManager = Bootstrap_->GetObjectManager();
            objectManager->RefObject(branchedNode->GetChunkList());
        }

        branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());
        branchedNode->SetReadQuorum(originatingNode->GetReadQuorum());
        branchedNode->SetWriteQuorum(originatingNode->GetWriteQuorum());
        branchedNode->SetVital(originatingNode->GetVital());

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Journal node branched (BranchedNodeId: %v, ChunkListId: %v, ReplicationFactor: %v, ReadQuorum: %v, WriteQuorum: %v)",
            branchedNode->GetId(),
            GetObjectId(originatingNode->GetChunkList()),
            originatingNode->GetReplicationFactor(),
            originatingNode->GetReadQuorum(),
            originatingNode->GetWriteQuorum());
    }

    virtual void DoMerge(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        auto* originatingChunkList = originatingNode->GetChunkList();
        auto originatingChunkListId = GetObjectId(originatingChunkList);

        auto* branchedChunkList = branchedNode->GetChunkList();
        auto branchedChunkListId = GetObjectId(branchedChunkList);

        if (!originatingNode->IsExternal()) {
            YCHECK(originatingChunkList == branchedChunkList);
            YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);

            auto objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(branchedChunkList);

            if (IsLeader()) {
                ScheduleSeal(originatingNode);
            }
        }

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Journal node merged (OriginatingNodeId: %v, OriginatingChunkListId: %v, BranchedNodeId: %v, "
            "BranchedChunkListId: %v)",
            originatingNode->GetVersionedId(),
            originatingChunkListId,
            branchedNode->GetVersionedId(),
            branchedChunkListId);
    }

    virtual void DoClone(
        TJournalNode* sourceNode,
        TJournalNode* clonedNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override
    {
        switch (mode) {
            case ENodeCloneMode::Copy:
                THROW_ERROR_EXCEPTION("Journals cannot be copied");
                break;

            case ENodeCloneMode::Move:
                // Moving a journal is OK.
                break;

            default:
                YUNREACHABLE();
        }

        if (!sourceNode->IsExternal()) {
            auto objectManager = TBase::Bootstrap_->GetObjectManager();
            auto* chunkList = sourceNode->GetChunkList();
            YCHECK(!clonedNode->GetChunkList());
            clonedNode->SetChunkList(chunkList);
            objectManager->RefObject(chunkList);
            YCHECK(chunkList->OwningNodes().insert(clonedNode).second);
        }

        clonedNode->SetReadQuorum(sourceNode->GetReadQuorum());
        clonedNode->SetWriteQuorum(sourceNode->GetWriteQuorum());

        TBase::DoClone(sourceNode, clonedNode, factory, mode);
    }


    void ScheduleSeal(TJournalNode* journal)
    {
        if (journal->IsSealed())
            return;

        auto chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->MaybeScheduleChunkSeal(journal->GetTrailingChunk());
    }

};

INodeTypeHandlerPtr CreateJournalTypeHandler(TBootstrap* bootstrap)
{
    return New<TJournalNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT

