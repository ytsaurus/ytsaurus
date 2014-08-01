#include "stdafx.h"
#include "journal_session.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "chunk_store.h"

#include <server/hydra/changelog.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NHydra;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TJournalSession::TJournalSession(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap,
    const TChunkId& chunkId,
    const TSessionOptions& options,
    TLocationPtr location)
    : TSessionBase(
        config,
        bootstrap,
        chunkId,
        options,
        location)
    , LastAppendResult_(OKFuture)
{ }

TChunkInfo TJournalSession::GetChunkInfo() const
{
    UpdateCachedParams();

    TChunkInfo info;
    info.set_disk_space(CachedDataSize_);
    info.set_sealed(CachedSealed_);
    return info;
}

void TJournalSession::UpdateCachedParams() const
{
    if (Changelog_) {
        CachedRowCount_ = Changelog_->GetRecordCount();
        CachedDataSize_ = Changelog_->GetDataSize();
        CachedSealed_ = Changelog_->IsSealed();
    }
}

void TJournalSession::DoStart()
{
    Chunk_ = New<TJournalChunk>(
        Bootstrap_,
        Location_,
        TChunkDescriptor(ChunkId_));

    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    Changelog_ = dispatcher->CreateChangelog(Chunk_, Options_.OptimizeForLatency);

    Chunk_->AttachChangelog(Changelog_);
    Chunk_->SetActive(true);

    auto chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->RegisterNewChunk(Chunk_);
}

void TJournalSession::DoCancel()
{
    UpdateCachedParams();

    Chunk_->DetachChangelog();
    Chunk_->SetActive(false);
    
    auto chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->UpdateExistingChunk(Chunk_);

    Finished_.Fire(TError());
}

TFuture<TErrorOr<IChunkPtr>> TJournalSession::DoFinish(
    const TChunkMeta& /*chunkMeta*/,
    const TNullable<int>& blockCount)
{
    auto sealResult = OKFuture;
    if (blockCount) {
        if (*blockCount != Changelog_->GetRecordCount()) {
            THROW_ERROR_EXCEPTION("Block count mismatch in journal session %v: expected %v, got %v",
                ChunkId_,
                Changelog_->GetRecordCount(),
                *blockCount);
        }
        sealResult = Changelog_->Seal(Changelog_->GetRecordCount());
    }

    auto this_ = MakeStrong(this);
    return sealResult.Apply(BIND([this, this_] (TError error) -> TErrorOr<IChunkPtr> {
        DoCancel();
        if (!error.IsOK()) {
            return error;
        }
        return IChunkPtr(Chunk_);
    }).AsyncVia(GetCurrentInvoker()));
}

TAsyncError TJournalSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool /*enableCaching*/)
{
    int recordCount = Changelog_->GetRecordCount();
    
    if (startBlockIndex > recordCount) {
        THROW_ERROR_EXCEPTION("Missing blocks %v:%v-%v",
            ChunkId_,
            recordCount,
            startBlockIndex - 1);
    }

    if (startBlockIndex < recordCount) {
        LOG_DEBUG("Skipped duplicate blocks %v:%v-%v",
            ChunkId_,
            startBlockIndex,
            recordCount - 1);
    }

    for (int index = recordCount - startBlockIndex;
         index < static_cast<int>(blocks.size());
         ++index)
    {
        LastAppendResult_ = Changelog_->Append(blocks[index]);
    }

    return OKFuture;
}

TAsyncError TJournalSession::DoSendBlocks(
    int /*startBlockIndex*/,
    int /*blockCount*/,
    const TNodeDescriptor& /*target*/)
{
    THROW_ERROR_EXCEPTION("Sending blocks is not supported for journal chunks");
}

TAsyncError TJournalSession::DoFlushBlocks(int blockIndex)
{
    int recordCount = Changelog_->GetRecordCount();
    
    if (blockIndex > recordCount) {
        THROW_ERROR_EXCEPTION("Missing blocks %v:%v-%v",
            ChunkId_,
            recordCount - 1,
            blockIndex);
    }

    return LastAppendResult_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
