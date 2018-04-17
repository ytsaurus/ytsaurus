#include "journal_node.h"
#include "private.h"
#include "journal_node_proxy.h"
#include "journal_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/chunk_owner_type_handler.h>

#include <yt/ytlib/journal_client/journal_ypath_proxy.h>

#include <yt/ytlib/object_client/helpers.h>

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
{ }

void TJournalNode::Save(NCellMaster::TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, ReadQuorum_);
    Save(context, WriteQuorum_);
    Save(context, Sealed_);
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
    return TrunkNode_->As<TJournalNode>();
}

const TJournalNode* TJournalNode::GetTrunkNode() const
{
    return TrunkNode_->As<TJournalNode>();
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

TClusterResources TJournalNode::GetDeltaResourceUsage() const
{
    auto* trunkNode = GetTrunkNode();
    if (trunkNode == this) {
        return TBase::GetDeltaResourceUsage();
    } else {
        return trunkNode->GetDeltaResourceUsage(); // Recurse once.
    }
}

TClusterResources TJournalNode::GetTotalResourceUsage() const
{
    auto* trunkNode = GetTrunkNode();
    if (trunkNode == this) {
        return TBase::GetTotalResourceUsage();
    } else {
        return trunkNode->GetTotalResourceUsage(); // Recurse once.
    }
}

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeTypeHandler
    : public TChunkOwnerTypeHandler<TJournalNode>
{
public:
    explicit TJournalNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::Journal;
    }

    virtual bool IsExternalizable() const override
    {
        return true;
    }

    virtual ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

protected:
    typedef TChunkOwnerTypeHandler<TJournalNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TJournalNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateJournalNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TJournalNode> DoCreate(
        const TVersionedNodeId& id,
        TCellTag cellTag,
        TTransaction* transaction,
        IAttributeDictionary* inheritedAttributes,
        IAttributeDictionary* explicitAttributes,
        NSecurityServer::TAccount* account) override
    {
        const auto& config = Bootstrap_->GetConfig()->CypressManager;

        auto combinedAttributes = OverlayAttributeDictionaries(explicitAttributes, inheritedAttributes);
        auto replicationFactor = combinedAttributes.GetAndRemove<int>("replication_factor", config->DefaultJournalReplicationFactor);
        auto readQuorum = combinedAttributes.GetAndRemove<int>("read_quorum", config->DefaultJournalReadQuorum);
        auto writeQuorum = combinedAttributes.GetAndRemove<int>("write_quorum", config->DefaultJournalWriteQuorum);

        ValidateReplicationFactor(replicationFactor);
        if (readQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (writeQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (readQuorum + writeQuorum < replicationFactor + 1) {
            THROW_ERROR_EXCEPTION("Read/write quorums are not safe: read_quorum + write_quorum < replication_factor + 1");
        }

        auto nodeHolder = DoCreateImpl(
            id,
            cellTag,
            transaction,
            inheritedAttributes,
            explicitAttributes,
            account,
            replicationFactor,
            NCompression::ECodec::None,
            NErasure::ECodec::None);
        auto* node = nodeHolder.get();

        node->SetReadQuorum(readQuorum);
        node->SetWriteQuorum(writeQuorum);

        return nodeHolder;
    }

    virtual void DoBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        // NB: Don't call TBase::DoBranch.

        branchedNode->SetPrimaryMediumIndex(originatingNode->GetPrimaryMediumIndex());
        branchedNode->Replication() = originatingNode->Replication();
        branchedNode->SetReadQuorum(originatingNode->GetReadQuorum());
        branchedNode->SetWriteQuorum(originatingNode->GetWriteQuorum());

        if (!originatingNode->IsExternal()) {
            auto* chunkList = originatingNode->GetChunkList();
            branchedNode->SetChunkList(chunkList);

            chunkList->AddOwningNode(branchedNode);

            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->RefObject(chunkList);
        }
    }

    virtual void DoLogBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto* primaryMedium = chunkManager->GetMediumByIndex(originatingNode->GetPrimaryMediumIndex());
        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Node branched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v, "
            "PrimaryMedium: %v, Replication: %v, ReadQuorum: %v, WriteQuorum: %v, Mode: %v, LockTimestamp: %llx)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            GetObjectId(originatingNode->GetChunkList()),
            primaryMedium->GetName(),
            originatingNode->Replication(),
            originatingNode->GetReadQuorum(),
            originatingNode->GetWriteQuorum(),
            lockRequest.Mode,
            lockRequest.Timestamp);
    }

    virtual void DoMerge(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        // NB: Don't call TBase::DoMerge.

        YCHECK(originatingNode->GetChunkList() == branchedNode->GetChunkList());
        auto* chunkList = originatingNode->GetChunkList();

        if (!originatingNode->IsExternal()) {
            chunkList->RemoveOwningNode(branchedNode);

            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(chunkList);
        }

        HandleTransactionFinished(originatingNode, branchedNode);
    }

    virtual void DoLogMerge(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Node merged (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            GetObjectId(originatingNode->GetChunkList()));
    }

    virtual void DoUnbranch(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        // NB: Don't call TBase::DoUnbranch.

        YCHECK(originatingNode->GetChunkList() == branchedNode->GetChunkList());

        HandleTransactionFinished(originatingNode, branchedNode);
    }

    virtual void DoLogUnbranch(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Node unbranched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            GetObjectId(originatingNode->GetChunkList()));
    }

    virtual void DoClone(
        TJournalNode* sourceNode,
        TJournalNode* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        if (mode == ENodeCloneMode::Copy) {
            THROW_ERROR_EXCEPTION("Journals cannot be copied");
        }

        if (!sourceNode->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }

        clonedNode->SetReadQuorum(sourceNode->GetReadQuorum());
        clonedNode->SetWriteQuorum(sourceNode->GetWriteQuorum());

        TBase::DoClone(sourceNode, clonedNode, factory, mode, account);
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
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                chunkManager->ScheduleChunkSeal(trailingChunk);
            } else {
                const auto& journalManager = Bootstrap_->GetJournalManager();
                journalManager->SealJournal(trunkNode, nullptr);
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

