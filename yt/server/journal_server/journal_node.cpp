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

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

    virtual void SetDefaultAttributes(
        IAttributeDictionary* attributes,
        TTransaction* transaction) override
    {
        TBase::SetDefaultAttributes(attributes, transaction);

        if (!attributes->Contains("replication_factor")) {
            attributes->Set("replication_factor", DefaultReplicationFactor);
        }

        if (!attributes->Contains("read_quorum")) {
            attributes->Set("read_quorum", DefaultReadQuorum);
        }

        if (!attributes->Contains("write_quorum")) {
            attributes->Set("write_quorum", DefaultWriteQuorum);
        }
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TJournalNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateJournalNodeProxy(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TJournalNode> DoCreate(
        const TVersionedNodeId& id,
        TTransaction* transaction,
        INodeTypeHandler::TReqCreate* request,
        INodeTypeHandler::TRspCreate* response) override
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

    virtual void DoValidateCreated(TJournalNode* node) override
    {
        TBase::DoValidateCreated(node);

        int replicationFactor = node->GetReplicationFactor();
        int readQuorum = node->GetReadQuorum();
        int writeQuorum = node->GetWriteQuorum();

        if (readQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (writeQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (readQuorum + writeQuorum < replicationFactor + 1) {
            THROW_ERROR_EXCEPTION("Read/write quorums are not safe: read_quorum + write_quorum < replication_factor + 1");
        }
    }

    virtual void DoDestroy(TJournalNode* node) override
    {
        TBase::DoDestroy(node);

        auto* chunkList = node->GetChunkList();
        YCHECK(chunkList->OwningNodes().erase(node) == 1);

        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->UnrefObject(chunkList);

        if (IsLeader() && !node->IsTrunk()) {
            ScheduleSeal(node);
        }
    }

    virtual void DoBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        TBase::DoBranch(originatingNode, branchedNode);

        auto* chunkList = originatingNode->GetChunkList();

        branchedNode->SetChunkList(chunkList);
        YCHECK(branchedNode->GetChunkList()->OwningNodes().insert(branchedNode).second);

        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->RefObject(branchedNode->GetChunkList());

        branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());
        branchedNode->SetReadQuorum(originatingNode->GetReadQuorum());
        branchedNode->SetWriteQuorum(originatingNode->GetWriteQuorum());
        branchedNode->SetVital(originatingNode->GetVital());

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Journal node branched (BranchedNodeId: %v, ChunkListId: %v, ReplicationFactor: %v, ReadQuorum: %v, WriteQuorum: %v)",
            ~ToString(branchedNode->GetId()),
            ~ToString(originatingNode->GetChunkList()->GetId()),
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
        auto* branchedChunkList = branchedNode->GetChunkList();
        YCHECK(originatingChunkList == branchedChunkList);
        YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);

        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->UnrefObject(branchedChunkList);

        if (IsLeader()) {
            ScheduleSeal(originatingNode);
        }

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Journal node merged (OriginatingNodeId: %v, BranchedNodeId: %v)",
            ~ToString(originatingNode->GetVersionedId()),
            ~ToString(branchedNode->GetVersionedId()));
    }

    virtual void DoClone(
        TJournalNode* /*sourceNode*/,
        TJournalNode* /*clonedNode*/,
        ICypressNodeFactoryPtr /*factory*/) override
    {
        THROW_ERROR_EXCEPTION("Journals cannot be cloned");
    }


    void ScheduleSeal(TJournalNode* journal)
    {
        if (journal->IsSealed())
            return;

        auto chunkManager = Bootstrap->GetChunkManager();
        chunkManager->ScheduleChunkSeal(journal->GetTrailingChunk());
    }

};

INodeTypeHandlerPtr CreateJournalTypeHandler(TBootstrap* bootstrap)
{
    return New<TJournalNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT

