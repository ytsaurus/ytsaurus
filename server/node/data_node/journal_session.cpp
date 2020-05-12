#include "journal_session.h"
#include "chunk_store.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "location.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/lib/hydra/changelog.h>

#include <yt/ytlib/chunk_client/proto/chunk_info.pb.h>

namespace NYT::NDataNode {

using namespace NHydra;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TJournalSession::DoStart()
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);
    
    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
    auto asyncChangelog = dispatcher->CreateChangelog(
        Location_,
        GetChunkId(),
        Options_.EnableMultiplexing,
        Options_.WorkloadDescriptor);

    return asyncChangelog.Apply(BIND([=, this_ = MakeStrong(this)] (const IChangelogPtr& changelog) {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        Changelog_ = changelog;
        Chunk_ = New<TJournalChunk>(
            Bootstrap_,
            Location_,
            TChunkDescriptor(GetChunkId()));
        Chunk_->SetActive(true);
        ChunkUpdateGuard_ = TChunkUpdateGuard::Acquire(Chunk_);

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->RegisterNewChunk(Chunk_);
    }).AsyncVia(SessionInvoker_));
}

void TJournalSession::DoCancel(const TError& /*error*/)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    OnFinished();
}

TFuture<TChunkInfo> TJournalSession::DoFinish(
    const TRefCountedChunkMetaPtr& /*chunkMeta*/,
    std::optional<int> blockCount)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    auto result = Changelog_->Close();

    if (blockCount) {
        if (*blockCount != Changelog_->GetRecordCount()) {
            THROW_ERROR_EXCEPTION("Block count mismatch in journal session %v: expected %v, got %v",
                SessionId_,
                Changelog_->GetRecordCount(),
                *blockCount);
        }
        result = result.Apply(BIND(&TJournalChunk::Seal, Chunk_)
            .AsyncVia(SessionInvoker_));
    }

    return result.Apply(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
        OnFinished();

        error.ThrowOnError();

        TChunkInfo info;
        info.set_disk_space(Chunk_->GetCachedDataSize());
        info.set_sealed(Chunk_->IsSealed());
        return info;
    }).AsyncVia(SessionInvoker_));
}

TFuture<void> TJournalSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    bool /*enableCaching*/)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    int recordCount = Changelog_->GetRecordCount();

    if (startBlockIndex > recordCount) {
        THROW_ERROR_EXCEPTION("Missing blocks %v:%v-%v",
            GetId(),
            recordCount,
            startBlockIndex - 1);
    }

    if (startBlockIndex < recordCount) {
        YT_LOG_DEBUG("Skipped duplicate blocks (BlockIds: %v:%v-%v)",
            GetId(),
            startBlockIndex,
            recordCount - 1);
    }

    std::vector<TSharedRef> records;
    records.reserve(blocks.size() - recordCount + startBlockIndex);
    for (int index = recordCount - startBlockIndex;
         index < static_cast<int>(blocks.size());
         ++index)
    {
        records.push_back(blocks[index].Data);
    }

    if (!records.empty()) {
        LastAppendResult_ = Changelog_->Append(records);
    }

    Chunk_->UpdateCachedParams(Changelog_);

    return VoidFuture;
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TJournalSession::DoSendBlocks(
    int /*startBlockIndex*/,
    int /*blockCount*/,
    const TNodeDescriptor& /*target*/)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    THROW_ERROR_EXCEPTION("Sending blocks is not supported for journal chunks");
}

TFuture<void> TJournalSession::DoFlushBlocks(int blockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    int recordCount = Changelog_->GetRecordCount();

    if (blockIndex > recordCount) {
        THROW_ERROR_EXCEPTION("Missing blocks %v:%v-%v",
            GetId(),
            recordCount - 1,
            blockIndex);
    }

    return LastAppendResult_;
}

void TJournalSession::OnFinished()
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (Changelog_) {
        Chunk_->UpdateCachedParams(Changelog_);
    }
    
    Chunk_->SetActive(false);

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->UpdateExistingChunk(Chunk_);

    Finished_.Fire(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
