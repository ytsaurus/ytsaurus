#include "journal_node_type_handler.h"
#include "journal_node.h"
#include "journal_node_proxy.h"
#include "journal_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_owner_type_handler.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/medium.h>
#include <yt/yt/server/master/chunk_server/helpers.h>

#include <yt/yt/server/master/cypress_server/config.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

namespace NYT::NJournalServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeTypeHandler
    : public TChunkOwnerTypeHandler<TJournalNode>
{
private:
    using TBase = TChunkOwnerTypeHandler<TJournalNode>;

public:
    using TBase::TBase;

    EObjectType GetObjectType() const override
    {
        return EObjectType::Journal;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

    bool HasBranchedChangesImpl(
        TJournalNode* /*originatingNode*/,
        TJournalNode* /*branchedNode*/) override
    {
        // Forbid explicitly unlocking journal nodes.
        return true;
    }

protected:
    ICypressNodeProxyPtr DoGetProxy(
        TJournalNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateJournalNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    std::unique_ptr<TJournalNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        const auto& config = Bootstrap_->GetConfig()->CypressManager;
        if (context.InheritedAttributes) {
            context.InheritedAttributes->Remove("compression_codec");
        }
        auto combinedAttributes = OverlayAttributeDictionaries(context.ExplicitAttributes, context.InheritedAttributes);
        auto erasureCodec = combinedAttributes->GetAndRemove<NErasure::ECodec>("erasure_codec", config->DefaultJournalErasureCodec);
        auto replicationFactor = combinedAttributes->GetAndRemove<int>("replication_factor", config->DefaultJournalReplicationFactor);
        auto readQuorum = combinedAttributes->GetAndRemove<int>("read_quorum", config->DefaultJournalReadQuorum);
        auto writeQuorum = combinedAttributes->GetAndRemove<int>("write_quorum", config->DefaultJournalWriteQuorum);

        NJournalClient::ValidateJournalAttributes(
            erasureCodec,
            replicationFactor,
            readQuorum,
            writeQuorum);

        auto nodeHolder = DoCreateImpl(
            id,
            context,
            replicationFactor,
            NCompression::ECodec::None,
            erasureCodec,
            /*enableStripedErasure*/ false,
            EChunkListKind::JournalRoot);
        auto* node = nodeHolder.get();

        node->SetReadQuorum(readQuorum);
        node->SetWriteQuorum(writeQuorum);

        return nodeHolder;
    }

    void DoBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode,
        const TLockRequest& /*lockRequest*/) override
    {
        // NB: Don't call TBase::DoBranch.

        branchedNode->SetPrimaryMediumIndex(originatingNode->GetPrimaryMediumIndex());
        branchedNode->Replication() = originatingNode->Replication();
        branchedNode->SetErasureCodec(originatingNode->GetErasureCodec());
        branchedNode->SetReadQuorum(originatingNode->GetReadQuorum());
        branchedNode->SetWriteQuorum(originatingNode->GetWriteQuorum());

        if (!originatingNode->IsExternal()) {
            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                if (auto* chunkList = originatingNode->GetChunkList(contentType)) {
                    branchedNode->SetChunkList(contentType, chunkList);
                    chunkList->AddOwningNode(branchedNode);
                }
            }
        }
    }

    void DoLogBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto* primaryMedium = chunkManager->GetMediumByIndex(originatingNode->GetPrimaryMediumIndex());
        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Node branched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v, "
            "PrimaryMedium: %v, Replication: %v, ErasureCodec: %v, ReadQuorum: %v, WriteQuorum: %v, "
            "Mode: %v, LockTimestamp: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            GetObjectId(originatingNode->GetChunkList()),
            primaryMedium->GetName(),
            originatingNode->Replication(),
            originatingNode->GetErasureCodec(),
            originatingNode->GetReadQuorum(),
            originatingNode->GetWriteQuorum(),
            lockRequest.Mode,
            lockRequest.Timestamp);
    }

    void DoMerge(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        // NB: Don't call TBase::DoMerge.

        if (!originatingNode->IsExternal()) {
            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                YT_VERIFY(originatingNode->GetChunkList(contentType) == branchedNode->GetChunkList(contentType));

                if (auto* chunkList = originatingNode->GetChunkList(contentType)) {
                    chunkList->RemoveOwningNode(branchedNode);
                }
            }
        }

        HandleTransactionFinished(branchedNode);
    }

    void DoLogMerge(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Node merged (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            GetObjectId(originatingNode->GetChunkList()));
    }

    void DoUnbranch(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        // NB: Don't call TBase::DoUnbranch.

        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            YT_VERIFY(originatingNode->GetChunkList(contentType) == branchedNode->GetChunkList(contentType));
        }

        HandleTransactionFinished(branchedNode);
    }

    void DoLogUnbranch(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Node unbranched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            GetObjectId(originatingNode->GetChunkList()));
    }

    void DoClone(
        TJournalNode* sourceNode,
        TJournalNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        if (!sourceNode->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }

        clonedTrunkNode->SetErasureCodec(sourceNode->GetErasureCodec());
        clonedTrunkNode->SetReadQuorum(sourceNode->GetReadQuorum());
        clonedTrunkNode->SetWriteQuorum(sourceNode->GetWriteQuorum());

        TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);
    }

    void DoBeginCopy(
        TJournalNode* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);

        // TODO(babenko): support journals cross-cell copying
        THROW_ERROR_EXCEPTION("Cross-cell copying of journal is not supported");
    }

    void DoEndCopy(
        TJournalNode* node,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(node, context, factory);

        // TODO(babenko): support journals cross-cell copying
        THROW_ERROR_EXCEPTION("Cross-cell copying of journal is not supported");
    }

    void HandleTransactionFinished(TJournalNode* branchedNode)
    {
        if (branchedNode->GetUpdateMode() != EUpdateMode::Append) {
            return;
        }

        auto* trunkNode = branchedNode->GetTrunkNode();
        if (trunkNode->IsExternal()) {
            return;
        }

        auto* chunkList = trunkNode->GetChunkList();
        if (auto* unsealedChunk = chunkList ? FindFirstUnsealedChild(chunkList)->As<TChunk>() : nullptr) {
            YT_LOG_DEBUG_IF(
                IsMutationLoggingEnabled(),
                "Waiting for journal chunk to become sealed (NodeId: %v, ChunkId: %v)",
                trunkNode->GetId(),
                unsealedChunk->GetId());
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->ScheduleChunkSeal(unsealedChunk);
        } else {
            const auto& journalManager = Bootstrap_->GetJournalManager();
            journalManager->SealJournal(trunkNode, nullptr);
        }
    }
};

INodeTypeHandlerPtr CreateJournalTypeHandler(TBootstrap* bootstrap)
{
    return New<TJournalNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer

