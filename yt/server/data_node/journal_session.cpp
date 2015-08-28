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
    TStoreLocationPtr location,
    TLease lease)
    : TSessionBase(
        config,
        bootstrap,
        chunkId,
        options,
        location,
        lease)
{ }

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
        TChunkDescriptor(ChunkId_));
    Chunk_->SetActive(true);

    auto chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->RegisterNewChunk(Chunk_);

    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    auto asyncChangelog = dispatcher->CreateChangelog(Location_, ChunkId_, Options_.OptimizeForLatency);
    return asyncChangelog.Apply(BIND([=, this_ = MakeStrong(this)] (IChangelogPtr changelog) {
        if (Chunk_->IsRemoveScheduled())
            return;
        Chunk_->AttachChangelog(changelog);
    }).AsyncVia(Bootstrap_->GetControlInvoker()));
}

void TJournalSession::DoCancel()
{
    Chunk_->DetachChangelog();
    Chunk_->SetActive(false);
    
    auto chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->UpdateExistingChunk(Chunk_);

    Finished_.Fire(TError());
}

TFuture<IChunkPtr> TJournalSession::DoFinish(
    const TChunkMeta& /*chunkMeta*/,
    const TNullable<int>& blockCount)
{
    auto changelog = Chunk_->GetAttachedChangelog();
    auto result = changelog->Close();

    if (blockCount) {
        if (*blockCount != changelog->GetRecordCount()) {
            THROW_ERROR_EXCEPTION("Block count mismatch in journal session %v: expected %v, got %v",
                ChunkId_,
                changelog->GetRecordCount(),
                *blockCount);
        }
        result = result.Apply(BIND(&TJournalChunk::Seal, Chunk_)
            .AsyncVia(Bootstrap_->GetControlInvoker()));
    }

    return result.Apply(BIND([=, this_ = MakeStrong(this)] (const TError& error) -> IChunkPtr {
        DoCancel();
        error.ThrowOnError();
        return IChunkPtr(Chunk_);
    }).AsyncVia(Bootstrap_->GetControlInvoker()));
}

TFuture<void> TJournalSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool /*enableCaching*/)
{
    auto changelog = Chunk_->GetAttachedChangelog();
    int recordCount = changelog->GetRecordCount();
    
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

    TFuture<void> lastAppendResult;
    for (int index = recordCount - startBlockIndex;
         index < static_cast<int>(blocks.size());
         ++index)
    {
        lastAppendResult = changelog->Append(blocks[index]);
    }

    if (lastAppendResult) {
        LastAppendResult_ = lastAppendResult.ToUncancelable();
    }

    return VoidFuture;
}

TFuture<void> TJournalSession::DoSendBlocks(
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
            ChunkId_,
            recordCount - 1,
            blockIndex);
    }

    return LastAppendResult_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
