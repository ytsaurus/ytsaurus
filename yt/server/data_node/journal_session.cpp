#include "stdafx.h"
#include "journal_session.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"

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
    : TSession(
        config,
        bootstrap,
        chunkId,
        type,
        syncOnClose,
        location)
    , LastAppendResult_(VoidFuture)
{ }

const TChunkInfo& TJournalSession::GetChunkInfo() const
{
    YUNREACHABLE();
}

void TJournalSession::Start(TLeaseManager::TLease lease)
{
    TSession::Start(lease);

    Chunk_ = New<TJournalChunk>(
        Location_,
        ChunkId_,
        TChunkInfo(), // TODO(babenko)
        Bootstrap_->GetMemoryUsageTracker());

    WriteInvoker_->Invoke(
        BIND(&TJournalSession::DoCreateChangelog, MakeStrong(this)));
}

void TJournalSession::DoCreateChangelog()
{
    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    Changelog_ = dispatcher->CreateChangelog(this);
}

void TJournalSession::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO(error, "Session canceled");

    CloseSession();
}

TFuture<TErrorOr<IChunkPtr>> TJournalSession::Finish(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Session finished");

    return MakeFuture<TErrorOr<IChunkPtr>>(CloseSession());
}

IChunkPtr TJournalSession::CloseSession()
{
    CloseLease();

    Finished_.Fire(TError());

    return Chunk_;
}

TAsyncError TJournalSession::PutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool /*enableCaching*/)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Ping();

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

TAsyncError TJournalSession::SendBlocks(
    int /*startBlockIndex*/,
    int /*blockCount*/,
    const TNodeDescriptor& /*target*/)
{
    THROW_ERROR_EXCEPTION("Sending blocks is not supported for journal chunks");
}

TAsyncError TJournalSession::FlushBlock(int blockIndex)
{
    int recordCount = Changelog_->GetRecordCount();
    
    if (blockIndex > recordCount) {
        THROW_ERROR_EXCEPTION("Missing blocks %s:%d-%d",
            ~ToString(ChunkId_),
            recordCount - 1,
            blockIndex);
    }

    auto this_ = MakeStrong(this);
    return LastAppendResult_.Apply(BIND([this, this_, blockIndex] () {
        Chunk_->SetRecordCount(blockIndex);
        return TError();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
