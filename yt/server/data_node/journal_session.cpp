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
    TLocationPtr location,
    TLease lease)
    : TSessionBase(
        config,
        bootstrap,
        chunkId,
        options,
        location,
        lease)
    , LastAppendResult_(VoidFuture)
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

    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    auto asyncChangelog = dispatcher->CreateChangelog(Chunk_, Options_.OptimizeForLatency);

    auto chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->RegisterNewChunk(Chunk_);

    auto this_ = MakeStrong(this);
    return asyncChangelog.Apply(BIND([=] (IChangelogPtr changelog) {
        UNUSED(this_);
        Chunk_->AttachChangelog(changelog);
        Chunk_->SetActive(true);
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
    auto sealResult = VoidFuture;
    auto changelog = Chunk_->GetAttachedChangelog();
    if (blockCount) {
        if (*blockCount != changelog->GetRecordCount()) {
            THROW_ERROR_EXCEPTION("Block count mismatch in journal session %v: expected %v, got %v",
                ChunkId_,
                changelog->GetRecordCount(),
                *blockCount);
        }
        sealResult = changelog->Seal(changelog->GetRecordCount());
    }

    auto this_ = MakeStrong(this);
    return sealResult.Apply(BIND([=] (const TError& error) -> IChunkPtr {
        UNUSED(this_);
        DoCancel();
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
        return IChunkPtr(Chunk_);
    }).AsyncVia(GetCurrentInvoker()));
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

    for (int index = recordCount - startBlockIndex;
         index < static_cast<int>(blocks.size());
         ++index)
    {
        LastAppendResult_ = changelog->Append(blocks[index]);
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
