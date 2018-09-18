#include "journal_session.h"
#include "chunk_store.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/hydra/changelog.h>

#include <yt/ytlib/chunk_client/chunk_info.pb.h>

namespace NYT {
namespace NDataNode {

using namespace NHydra;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TChunkInfo TJournalSession::GetChunkInfo() const
{
    TChunkInfo info;
    info.set_disk_space(Chunk_->GetDataSize());
    info.set_sealed(Chunk_->IsSealed());
    return info;
}

TFuture<void> TJournalSession::DoStart()
{
    Chunk_ = New<TJournalChunk>(
        Bootstrap_,
        Location_,
        TChunkDescriptor(GetChunkId()));
    Chunk_->SetActive(true);

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->RegisterNewChunk(Chunk_);

    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
    auto asyncChangelog = dispatcher->CreateChangelog(
        Location_,
        GetChunkId(),
        Options_.EnableMultiplexing,
        Options_.WorkloadDescriptor);

    return asyncChangelog.Apply(BIND([=, this_ = MakeStrong(this)] (const IChangelogPtr& changelog) {
        if (Chunk_->IsRemoveScheduled()) {
            THROW_ERROR_EXCEPTION("Chunk %v is scheduled for removal",
                GetId());
        }
        Chunk_->AttachChangelog(changelog);
    }).AsyncVia(Bootstrap_->GetControlInvoker()));
}

void TJournalSession::DoCancel(const TError& /*error*/)
{
    OnFinished();
}

TFuture<IChunkPtr> TJournalSession::DoFinish(
    const TChunkMeta* /*chunkMeta*/,
    const TNullable<int>& blockCount)
{
    auto changelog = Chunk_->GetAttachedChangelog();
    auto result = changelog->Close();

    if (blockCount) {
        if (*blockCount != changelog->GetRecordCount()) {
            THROW_ERROR_EXCEPTION("Block count mismatch in journal session %v: expected %v, got %v",
                SessionId_,
                changelog->GetRecordCount(),
                *blockCount);
        }
        result = result.Apply(BIND(&TJournalChunk::Seal, Chunk_)
            .AsyncVia(Bootstrap_->GetControlInvoker()));
    }

    return result.Apply(BIND([=, this_ = MakeStrong(this)] (const TError& error) -> IChunkPtr {
        OnFinished();
        error.ThrowOnError();
        return IChunkPtr(Chunk_);
    }).AsyncVia(Bootstrap_->GetControlInvoker()));
}

TFuture<void> TJournalSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    bool /*enableCaching*/)
{
    auto changelog = Chunk_->GetAttachedChangelog();
    int recordCount = changelog->GetRecordCount();

    if (startBlockIndex > recordCount) {
        THROW_ERROR_EXCEPTION("Missing blocks %v:%v-%v",
            GetId(),
            recordCount,
            startBlockIndex - 1);
    }

    if (startBlockIndex < recordCount) {
        LOG_DEBUG("Skipped duplicate blocks %v:%v-%v",
            GetId(),
            startBlockIndex,
            recordCount - 1);
    }

    TFuture<void> lastAppendResult;
    for (int index = recordCount - startBlockIndex;
         index < static_cast<int>(blocks.size());
         ++index)
    {
        lastAppendResult = changelog->Append(blocks[index].Data);
    }

    if (lastAppendResult) {
        LastAppendResult_ = lastAppendResult.ToUncancelable();
    }

    return VoidFuture;
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TJournalSession::DoSendBlocks(
    int /*startBlockIndex*/,
    int /*blockCount*/,
    const TNodeDescriptor& /*target*/)
{
    THROW_ERROR_EXCEPTION("Sending blocks is not supported for journal chunks");
}

TFuture<void> TJournalSession::DoFlushBlocks(int blockIndex)
{
    auto changelog = Chunk_->GetAttachedChangelog();
    int recordCount = changelog->GetRecordCount();

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
    Chunk_->DetachChangelog();
    Chunk_->SetActive(false);

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->UpdateExistingChunk(Chunk_);

    Finished_.Fire(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
