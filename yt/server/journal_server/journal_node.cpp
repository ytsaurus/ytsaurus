#include "stdafx.h"
#include "journal_node.h"
#include "journal_node_proxy.h"
#include "private.h"

#include <server/chunk_server/chunk_owner_type_handler.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialize.h>
#include <server/cell_master/multicell_manager.h>

#include <ytlib/journal_client/journal_ypath_proxy.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NJournalServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NJournalClient;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TJournalNode::TJournalNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
    , ReadQuorum_(0)
    , WriteQuorum_(0)
    , Sealed_(true)
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
    // COMPAT(babenko)
    if (context.GetVersion() >= 200) {
        Load(context, Sealed_);
    }
}

void TJournalNode::BeginUpload(NChunkClient::EUpdateMode mode)
{
    TChunkOwnerBase::BeginUpload(mode);

    GetTrunkNode()->Sealed_ = false;
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

TJournalNode* TJournalNode::GetTrunkNode()
{
    return static_cast<TJournalNode*>(TrunkNode_);
}

const TJournalNode* TJournalNode::GetTrunkNode() const
{
    return static_cast<const TJournalNode*>(TrunkNode_);
}

bool TJournalNode::GetSealed() const
{
    return GetTrunkNode()->Sealed_;
}

void TJournalNode::SetSealed(bool value)
{
    YCHECK(IsTrunk());
    Sealed_ = value;
}

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeTypeHandler
    : public TChunkOwnerTypeHandler<TJournalNode>
{
public:
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

    virtual TClusterResources GetTotalResourceUsage(const TCypressNodeBase* node) override
    {
        return TBase::GetTotalResourceUsage(node->GetTrunkNode());
    }

    virtual TClusterResources GetAccountingResourceUsage(const TCypressNodeBase* node) override
    {
        return TBase::GetAccountingResourceUsage(node->GetTrunkNode());
    }

protected:
    typedef TChunkOwnerTypeHandler<TJournalNode> TBase;

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
        TTransaction* transaction,
        IAttributeDictionary* attributes) override
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        auto objectManager = Bootstrap_->GetObjectManager();

        // NB: Don't call TBase::InitializeAttributes; take care of all attributes here.

        int replicationFactor = attributes->Get<int>("replication_factor", DefaultReplicationFactor);
        attributes->Remove("replication_factor");

        int readQuorum = attributes->Get<int>("read_quorum", DefaultReadQuorum);
        attributes->Remove("read_quorum");

        int writeQuorum = attributes->Get<int>("write_quorum", DefaultWriteQuorum);
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

        auto nodeHolder = TBase::DoCreate(
            id,
            cellTag,
            transaction,
            attributes);
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

    virtual void DoBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        // NB: Don't call TBase::DoBranch.

        branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());
        branchedNode->SetReadQuorum(originatingNode->GetReadQuorum());
        branchedNode->SetWriteQuorum(originatingNode->GetWriteQuorum());
        branchedNode->SetVital(originatingNode->GetVital());

        auto* chunkList = originatingNode->GetChunkList();
        auto chunkListId = GetObjectId(chunkList);

        if (!originatingNode->IsExternal()) {
            branchedNode->SetChunkList(chunkList);
            YCHECK(chunkList->OwningNodes().insert(branchedNode).second);

            auto objectManager = Bootstrap_->GetObjectManager();
            objectManager->RefObject(chunkList);
        }

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Journal node branched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v, "
            "ReplicationFactor: %v, ReadQuorum: %v, WriteQuorum: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            chunkListId,
            originatingNode->GetReplicationFactor(),
            originatingNode->GetReadQuorum(),
            originatingNode->GetWriteQuorum());
    }

    virtual void DoMerge(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        // NB: Don't call TBase::DoMerge.

        YCHECK(originatingNode->GetChunkList() == branchedNode->GetChunkList());
        auto* chunkList = originatingNode->GetChunkList();
        auto chunkListId = GetObjectId(chunkList);

        if (!originatingNode->IsExternal()) {
            YCHECK(chunkList->OwningNodes().erase(branchedNode) == 1);

            auto objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(chunkList);
        }

        HandleTransactionFinished(originatingNode, branchedNode);

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Journal node merged (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            chunkListId);
    }

    virtual void DoUnbranch(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        // NB: Don't call TBase::DoUnbranch.

        YCHECK(originatingNode->GetChunkList() == branchedNode->GetChunkList());
        auto* chunkList = originatingNode->GetChunkList();
        auto chunkListId = GetObjectId(chunkList);

        HandleTransactionFinished(originatingNode, branchedNode);

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Journal node unbranched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            chunkListId);
    }

    virtual void DoClone(
        TJournalNode* sourceNode,
        TJournalNode* clonedNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override
    {
        if (mode == ENodeCloneMode::Copy) {
            THROW_ERROR_EXCEPTION("Journals cannot be copied");
        }

        if (!sourceNode->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }

        clonedNode->SetReadQuorum(sourceNode->GetReadQuorum());
        clonedNode->SetWriteQuorum(sourceNode->GetWriteQuorum());

        TBase::DoClone(sourceNode, clonedNode, factory, mode);
    }

    void HandleTransactionFinished(TJournalNode* originatingNode, TJournalNode* branchedNode)
    {
        if (branchedNode->GetUpdateMode() != EUpdateMode::Append)
            return;

        auto* trunkNode = branchedNode->GetTrunkNode();
        if (!trunkNode->IsExternal()) {
            auto* trailingChunk = trunkNode->GetTrailingChunk();
            if (trailingChunk && !trailingChunk->IsSealed()) {
                LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Waiting for the trailing journal chunk to become sealed (NodeId: %v, ChunkId: %v)",
                    trunkNode->GetId(),
                    trailingChunk->GetId());
                if (IsLeader()) {
                    auto chunkManager = Bootstrap_->GetChunkManager();
                    chunkManager->MaybeScheduleChunkSeal(trailingChunk);
                }
            } else {
                auto cypressManager = Bootstrap_->GetCypressManager();
                cypressManager->SealJournal(trunkNode, nullptr);
            }
        }
    }

};

INodeTypeHandlerPtr CreateJournalTypeHandler(TBootstrap* bootstrap)
{
    return New<TJournalNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT

