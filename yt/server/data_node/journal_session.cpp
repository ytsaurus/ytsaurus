#include "stdafx.h"
#include "journal_session.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "chunk_store.h"

#include <server/hydra/changelog.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TJournalSession::TJournalSession(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap,
    const TChunkId& chunkId,
    EWriteSessionType type,
    bool syncOnClose,
    TLocationPtr location)
    : TSessionBase(
        config,
        bootstrap,
        chunkId,
        type,
        syncOnClose,
        location)
    , LastAppendResult_(OKFuture)
{ }

TChunkInfo TJournalSession::GetChunkInfo() const
{
    UpdateChunkInfo();
    return ChunkInfo_;
}

void TJournalSession::UpdateChunkInfo() const
{
    if (Changelog_) {
        ChunkInfo_.set_record_count(Changelog_->GetRecordCount());
        ChunkInfo_.set_sealed(Changelog_->IsSealed());
    }
}

void TJournalSession::DoStart()
{
    BIND(&TJournalSession::DoCreateChangelog, MakeStrong(this))
        .AsyncVia(WriteInvoker_)
        .Run()
        .Subscribe(
            BIND(&TJournalSession::OnChangelogCreated, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()));
}

void TJournalSession::DoCreateChangelog()
{
    Chunk_ = New<TJournalChunk>(
        Bootstrap_,
        Location_,
        ChunkId_,
        TChunkInfo());

    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    Changelog_ = dispatcher->CreateChangelog(Chunk_);

    Chunk_->SetChangelog(Changelog_);
}

void TJournalSession::OnChangelogCreated()
{
    auto chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->RegisterNewChunk(Chunk_);
}

void TJournalSession::DoCancel()
{
    UpdateChunkInfo();
    Chunk_->ResetChangelog();
    Finished_.Fire(TError());
}

TFuture<TErrorOr<IChunkPtr>> TJournalSession::DoFinish(const TChunkMeta& /*chunkMeta*/)
{
    DoCancel();
    return MakeFuture<TErrorOr<IChunkPtr>>(IChunkPtr(Chunk_));
}

TAsyncError TJournalSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool /*enableCaching*/)
{
    int recordCount = Changelog_->GetRecordCount();
    
    if (startBlockIndex > recordCount) {
        THROW_ERROR_EXCEPTION("Missing blocks %s:%d-%d",
            ~ToString(ChunkId_),
            recordCount,
            startBlockIndex - 1);
    }

    if (startBlockIndex < recordCount) {
        LOG_DEBUG("Skipped duplicate blocks %s:%d-%d",
            ~ToString(ChunkId_),
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
        THROW_ERROR_EXCEPTION("Missing blocks %s:%d-%d",
            ~ToString(ChunkId_),
            recordCount - 1,
            blockIndex);
    }

    return LastAppendResult_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
