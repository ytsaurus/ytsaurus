#include "journal_node_proxy.h"
#include "private.h"
#include "journal_node.h"
#include "journal_manager.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_owner_node_proxy.h>

#include <yt/yt/server/master/chunk_server/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/journal_client/helpers.h>
#include <yt/yt/ytlib/journal_client/proto/journal_ypath.pb.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NJournalServer {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NJournalClient;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

//! Computes the quorum row count for a given journal.
/*!
 *  Quorum row count |Q| obeys the following:
 *  1) safety: if row with index |i| was ever committed then |i < Q|;
 *  2) soundness: it is possible to read (possibly applying erasure repair)
 *     the first |Q| rows in the journal.
 */
class TJournalQuorumRowCountSession
    : public TRefCounted
{
public:
    TJournalQuorumRowCountSession(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , JournalRpcTimeout_(Bootstrap_->GetDynamicConfig()->ChunkManager->JournalRpcTimeout)
    { }

    TFuture<i64> Run(TJournalNode* node) {
        YT_VERIFY(!node->GetHunkChunkList());

        const auto* chunkList = node->GetChunkList();
        if (!chunkList) {
            return MakeFuture<i64>(0);
        }

        if (chunkList->Children().empty()) {
            return MakeFuture<i64>(0);
        }

        SealedRowCount_ = chunkList->Statistics().RowCount;

        auto* firstUnsealedChild = FindFirstUnsealedChild(chunkList);
        if (!firstUnsealedChild) {
            return MakeFuture(SealedRowCount_);
        }

        auto* firstUnsealedChunk = firstUnsealedChild->As<TChunk>();
        if (firstUnsealedChunk->GetOverlayed()) {
            auto firstUnsealedChunkIndex = GetChildIndex(chunkList, firstUnsealedChunk);
            std::vector<TEphemeralObjectPtr<TChunk>> chunks;
            // NB: We can not pass reference to chunks to replica fetcher.
            // TODO(grphil): Avoid copy here.
            std::vector<TEphemeralObjectPtr<TChunk>> chunksToFetchReplicas;
            for (int index = firstUnsealedChunkIndex; index < std::ssize(chunkList->Children()); ++index) {
                auto* chunk = chunkList->Children()[index]->As<TChunk>();
                chunks.emplace_back(chunk);
                chunksToFetchReplicas.emplace_back(chunk);
            }

            const auto& chunkReplicaFetcher = Bootstrap_->GetChunkManager()->GetChunkReplicaFetcher();
            return chunkReplicaFetcher->GetChunkReplicasAsync(std::move(chunksToFetchReplicas))
                .Apply(BIND([
                    chunks = std::move(chunks),
                    chunkReplicaFetcher,
                    this,
                    this_ = MakeStrong(this)
                ] (const THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>> replicasOrErrors) {
                    for (const auto& chunk : chunks) {
                        if (!IsObjectAlive(chunk)) {
                            THROW_ERROR_EXCEPTION("Chunk %v died while replicas were fetched for it", chunk->GetId());
                        }
                        const auto& replicasOrError = GetOrCrash(replicasOrErrors, chunk->GetId()).ValueOrThrow();
                        auto replicas = chunkReplicaFetcher->FilterAliveReplicas(replicasOrError);

                        ChunkDescriptors_.push_back(TChunkDescriptor{
                            .ChunkId = chunk->GetId(),
                            .CodecId = chunk->GetErasureCodec(),
                            .ReadQuorum = chunk->GetReadQuorum(),
                            .ReplicaLagLimit = chunk->GetReplicaLagLimit(),
                            .ReplicaDescriptors = GetChunkReplicaDescriptors(chunk.Get(), replicas)
                        });
                    }
                }).AsyncViaGuarded(
                        Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ChunkManager),
                        TError("Error fetching chunk replicas")))
                .Apply(BIND(&TJournalQuorumRowCountSession::ComputeQuorumRowCountFromChunks, MakeStrong(this))
                    .AsyncViaGuarded(
                        NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                        TError("Error computing quorum row count")));
        } else {
            YT_VERIFY(chunkList->Children().back() == firstUnsealedChunk);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            TEphemeralObjectPtr<TChunk> chunkPtr(firstUnsealedChunk);

            const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();
            return chunkReplicaFetcher->GetChunkReplicasAsync(chunkPtr.Clone())
                .Apply(BIND([
                    chunkPtr = std::move(chunkPtr),
                    chunkManager,
                    chunkReplicaFetcher,
                    sealedRowCount = SealedRowCount_
                ] (const TErrorOr<std::vector<TSequoiaChunkReplica>>& replicasOrError) {
                    if (!IsObjectAlive(chunkPtr)) {
                        THROW_ERROR_EXCEPTION("Chunk is dead");
                    }
                    auto replicas = chunkReplicaFetcher->FilterAliveReplicas(replicasOrError.ValueOrThrow());
                    auto info = WaitFor(chunkManager->GetChunkQuorumInfo(
                        chunkPtr.Get(),
                        GetChunkReplicaDescriptors(chunkPtr.Get(), replicas))).ValueOrThrow();

                    return sealedRowCount + info.RowCount;
                }).AsyncViaGuarded(
                    Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ChunkManager),
                    TError("Error fetching Sequoia replicas")));
        }
    }

private:
    TBootstrap* const Bootstrap_;
    const TDuration JournalRpcTimeout_;

    i64 SealedRowCount_ = -1;

    struct TChunkDescriptor
    {
        TChunkId ChunkId;
        NErasure::ECodec CodecId;
        int ReadQuorum;
        i64 ReplicaLagLimit;
        std::vector<TChunkReplicaDescriptor> ReplicaDescriptors;
    };

    std::vector<TChunkDescriptor> ChunkDescriptors_;

    i64 ComputeQuorumRowCountFromChunks()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        for (int chunkIndex = std::ssize(ChunkDescriptors_) - 1; chunkIndex >= 0; --chunkIndex) {
            auto chunkRowCount = TryGetChunkRowCount(chunkIndex);
            if (chunkRowCount) {
                return *chunkRowCount;
            }
        }
        // If no chunk has first overlayed row index, we return initially computed quorum row count.
        return SealedRowCount_;
    }

    std::optional<i64> TryGetChunkRowCount(int chunkIndex)
    {
        YT_VERIFY(chunkIndex >= 0);

        const auto& chunkDescriptor = ChunkDescriptors_[chunkIndex];
        auto info = WaitFor(ComputeQuorumInfo(
            chunkDescriptor.ChunkId,
            true,
            chunkDescriptor.CodecId,
            chunkDescriptor.ReadQuorum,
            chunkDescriptor.ReplicaLagLimit,
            chunkDescriptor.ReplicaDescriptors,
            JournalRpcTimeout_,
            Bootstrap_->GetNodeChannelFactory()))
                .ValueOrThrow();
        if (info.FirstOverlayedRowIndex) {
            return std::make_optional(*info.FirstOverlayedRowIndex + info.RowCount);
        }
        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeProxy
    : public TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TJournalNode>
{
public:
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

private:
    using TBase = TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TJournalNode>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisImpl();
        auto isExternal = node->IsExternal();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReadQuorum)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WriteQuorum)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QuorumRowCount)
            .SetExternal(isExternal)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::Sealed);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        auto* node = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ReadQuorum:
                BuildYsonFluently(consumer)
                    .Value(node->GetReadQuorum());
                return true;

            case EInternedAttributeKey::WriteQuorum:
                BuildYsonFluently(consumer)
                    .Value(node->GetWriteQuorum());
                return true;

            case EInternedAttributeKey::Sealed:
                BuildYsonFluently(consumer)
                    .Value(node->GetSealed());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* node = GetThisImpl();
        auto isExternal = node->IsExternal();

        switch (key) {
            case EInternedAttributeKey::QuorumRowCount: {
                if (isExternal) {
                    break;
                }

                return New<TJournalQuorumRowCountSession>(Bootstrap_)->Run(node).Apply(
                    BIND([] (i64 count) {
                        return ConvertToYsonString(count);
                    }));
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    void ValidateBeginUpload() override
    {
        TBase::ValidateBeginUpload();

        const auto* journal = GetThisImpl();
        if (!journal->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }
    }

    void ValidateStorageParametersUpdate() override
    {
        TBase::ValidateStorageParametersUpdate();
        THROW_ERROR_EXCEPTION("Changing storage settings for journal nodes is forbidden");
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(UpdateStatistics);
        DISPATCH_YPATH_SERVICE_METHOD(Seal);
        DISPATCH_YPATH_SERVICE_METHOD(Truncate);
        return TBase::DoInvoke(context);
    }

    void ValidateReadLimit(const NChunkClient::NProto::TReadLimit& readLimit) const override
    {
        if (readLimit.has_legacy_key() || readLimit.has_key_bound_prefix()) {
            THROW_ERROR_EXCEPTION("Key selectors are not supported for journals");
        }
        if (readLimit.has_offset()) {
            THROW_ERROR_EXCEPTION("Offset selectors are not supported for journals");
        }
        if (readLimit.has_chunk_index()) {
            THROW_ERROR_EXCEPTION("Chunk selectors are not supported for journals");
        }
        if (readLimit.has_tablet_index()) {
            THROW_ERROR_EXCEPTION("Tablet selectors are not supported for journals");
        }
    }

    DECLARE_YPATH_SERVICE_METHOD(NJournalClient::NProto, UpdateStatistics)
    {
        Y_UNUSED(response);

        DeclareMutating();

        context->SetRequestInfo("Statistics: %v", request->statistics());

        auto* journal = GetThisImpl();
        YT_VERIFY(journal->IsTrunk());

        const auto& journalManager = Bootstrap_->GetJournalManager();
        journalManager->UpdateStatistics(journal->GetTrunkNode(), FromProto<TChunkOwnerDataStatistics>(request->statistics()));

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NJournalClient::NProto, Seal)
    {
        Y_UNUSED(response);

        DeclareMutating();

        ValidateNoTransaction();

        context->SetRequestInfo();

        auto* journal = GetThisImpl();
        const auto& journalManager = Bootstrap_->GetJournalManager();
        journalManager->SealJournal(journal, FromProto<TChunkOwnerDataStatistics>(request->statistics()));

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NJournalClient::NProto, Truncate)
    {
        Y_UNUSED(response);

        DeclareMutating();

        ValidateNoTransaction();

        context->SetRequestInfo("RowCount: %v", request->row_count());

        auto* journal = LockThisImpl();
        const auto& journalManager = Bootstrap_->GetJournalManager();
        journalManager->TruncateJournal(journal, request->row_count());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateJournalNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TJournalNode* trunkNode)
{
    return New<TJournalNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer
