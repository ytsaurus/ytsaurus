#include "journal_node_proxy.h"
#include "private.h"
#include "journal_node.h"
#include "journal_manager.h"

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
using namespace NJournalClient;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionServer;
using namespace NCellMaster;

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
    TJournalQuorumRowCountSession(TJournalNode* node, TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        const auto* chunkList = node->GetChunkList();
        if (!chunkList) {
            Promise_.Set(0);
            return;
        }

        if (chunkList->Children().empty()) {
            Promise_.Set(0);
            return;
        }

        SealedRowCount_ = chunkList->Statistics().RowCount;

        auto* firstUnsealedChunk = FindFirstUnsealedChild(chunkList)->As<TChunk>();
        if (!firstUnsealedChunk) {
            Promise_.Set(SealedRowCount_);
            return;
        }

        if (firstUnsealedChunk->GetOverlayed()) {
            auto firstUnsealedChunkIndex = GetChildIndex(chunkList, firstUnsealedChunk);
            for (int index = firstUnsealedChunkIndex; index < static_cast<int>(chunkList->Children().size()); ++index) {
                auto* chunk = chunkList->Children()[index]->As<TChunk>();
                ChunkDescriptors_.push_back(TChunkDescriptor{
                    .ChunkId = chunk->GetId(),
                    .CodecId = chunk->GetErasureCodec(),
                    .ReadQuorum = chunk->GetReadQuorum(),
                    .ReplicaLagLimit = chunk->GetReplicaLagLimit(),
                    .ReplicaDescriptors = GetChunkReplicaDescriptors(chunk)
                });
            }
            CurrentChunkIndex_ = static_cast<int>(ChunkDescriptors_.size()) - 1;
            RequestChunkInfo();
        } else {
            YT_VERIFY(chunkList->Children().back() == firstUnsealedChunk);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            Promise_.SetFrom(chunkManager->GetChunkQuorumInfo(firstUnsealedChunk).Apply(
                BIND([sealedRowCount = SealedRowCount_] (const TChunkQuorumInfo& info) {
                    YT_VERIFY(!info.FirstOverlayedRowIndex);
                    return sealedRowCount + info.RowCount;
                })));
        }
    }

    TFuture<i64> Run()
    {
        return Promise_.ToFuture();
    }

private:
    TBootstrap* const Bootstrap_;

    i64 SealedRowCount_ = -1;

    const TPromise<i64> Promise_ = NewPromise<i64>();

    struct TChunkDescriptor
    {
        TChunkId ChunkId;
        NErasure::ECodec CodecId;
        int ReadQuorum;
        i64 ReplicaLagLimit;
        std::vector<TChunkReplicaDescriptor> ReplicaDescriptors;
    };

    std::vector<TChunkDescriptor> ChunkDescriptors_;
    int CurrentChunkIndex_ = -1;


    void RequestChunkInfo()
    {
        if (CurrentChunkIndex_ < 0) {
            Promise_.Set(SealedRowCount_);
            return;
        }

        const auto& chunkDescriptor = ChunkDescriptors_[CurrentChunkIndex_];
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        YT_UNUSED_FUTURE(chunkManager->GetChunkQuorumInfo(
            chunkDescriptor.ChunkId,
            true,
            chunkDescriptor.CodecId,
            chunkDescriptor.ReadQuorum,
            chunkDescriptor.ReplicaLagLimit,
            chunkDescriptor.ReplicaDescriptors)
            .Apply(
                BIND(&TJournalQuorumRowCountSession::OnChunkInfoReceived, MakeStrong(this))
                    .Via(GetCurrentInvoker())));
    }

    void OnChunkInfoReceived(const TErrorOr<TChunkQuorumInfo>& infoOrError)
    {
        if (!infoOrError.IsOK()) {
            Promise_.Set(infoOrError);
            return;
        }

        const auto& info = infoOrError.Value();
        if (info.FirstOverlayedRowIndex) {
            Promise_.Set(*info.FirstOverlayedRowIndex + info.RowCount);
            return;
        }

        --CurrentChunkIndex_;
        RequestChunkInfo();
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

                return New<TJournalQuorumRowCountSession>(node, Bootstrap_)->Run().Apply(
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
        journalManager->UpdateStatistics(journal->GetTrunkNode(), &request->statistics());

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
        journalManager->SealJournal(journal, &request->statistics());

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
