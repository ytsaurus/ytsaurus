#include "journal_session.h"

#include "bootstrap.h"
#include "chunk_store.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "location.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hydra/file_changelog.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

namespace NYT::NDataNode {

using namespace NHydra;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TJournalSession::DoStart()
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
    auto changelogFuture = dispatcher->CreateJournal(
        Location_,
        GetChunkId(),
        Options_.EnableMultiplexing,
        Options_.WorkloadDescriptor);

    return changelogFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] (const IFileChangelogPtr& changelog) {
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

        Changelog_ = changelog;
        Chunk_ = New<TJournalChunk>(
            TChunkContext::Create(Bootstrap_),
            Location_,
            TChunkDescriptor(GetChunkId()));
        Chunk_->SetActive(true);
        ChunkUpdateGuard_ = TChunkUpdateGuard::Acquire(Chunk_);

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->RegisterNewChunk(Chunk_, /*session*/ this, std::move(LockedChunkGuard_));
    }).AsyncVia(SessionInvoker_));
}

void TJournalSession::DoCancel(const TError& /*error*/)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    OnFinished();
}

i64 TJournalSession::GetMemoryUsage() const
{
    // TODO(don-dron): Calculate memory usage.
    return 0;
}

i64 TJournalSession::GetTotalSize() const
{
    // TODO(don-dron): Calculate size.
    return 0;
}

i64 TJournalSession::GetBlockCount() const
{
    // TODO(don-dron): Calculate block count.
    return 0;
}

i64 TJournalSession::GetWindowSize() const
{
    return 0;
}

i64 TJournalSession::GetIntermediateEmptyBlockCount() const
{
    return 0;
}

TFuture<ISession::TFinishResult> TJournalSession::DoFinish(
    const TRefCountedChunkMetaPtr& /*chunkMeta*/,
    std::optional<int> blockCount,
    bool truncateExtraBlocks)
{
    YT_VERIFY(!truncateExtraBlocks);
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    auto result = Changelog_->Finish();

    if (blockCount) {
        if (*blockCount != Changelog_->GetRecordCount()) {
            return MakeFuture<TFinishResult>(TError("Block count mismatch in journal session %v: expected %v, got %v",
                SessionId_,
                Changelog_->GetRecordCount(),
                *blockCount));
        }
        result = result.Apply(BIND(&TJournalChunk::Seal, Chunk_)
            .AsyncVia(SessionInvoker_));
    }

    return result.Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
        OnFinished();

        error.ThrowOnError();

        TChunkInfo info;
        info.set_disk_space(Chunk_->GetDataSize());
        info.set_sealed(Chunk_->IsSealed());

        return TFinishResult {
            .ChunkInfo = std::move(info),
            .ChunkWriterStatistics = WriteBlocksOptions_.ClientOptions.ChunkWriterStatistics,
        };
    }).AsyncVia(SessionInvoker_));
}

TFuture<NIO::TIOCounters> TJournalSession::DoPutBlocks(
    int startBlockIndex,
    std::vector<TBlock> blocks,
    i64 /*cumulativeBlockSize*/,
    bool /*enableCaching*/)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    int recordCount = Changelog_->GetRecordCount();

    if (startBlockIndex > recordCount) {
        auto error = TError("Missing blocks")
            << TErrorAttribute("chunk_id", SessionId_.ChunkId)
            << TErrorAttribute("medium_index", SessionId_.MediumIndex)
            << TErrorAttribute("start_block_index", recordCount)
            << TErrorAttribute("end_block_index", startBlockIndex - 1);

        THROW_ERROR error;
    }

    if (startBlockIndex < recordCount) {
        YT_LOG_DEBUG("Skipped duplicate blocks (ChunkId: %v, Blocks: %v)",
            SessionId_.ChunkId,
            FormatBlocks(startBlockIndex, recordCount - 1));
    }

    i64 payloadSize = 0;
    std::vector<TSharedRef> records;
    records.reserve(blocks.size() - recordCount + startBlockIndex);
    for (int index = recordCount - startBlockIndex;
         index < std::ssize(blocks);
         ++index)
    {
        if (auto error = blocks[index].ValidateChecksum(); !error.IsOK()) {
            error = TError("Error appending changelog records")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("changelog_id", Changelog_->GetId())
                << error;

            YT_LOG_ALERT(error);
            THROW_ERROR error;
        }
        records.push_back(blocks[index].Data);
        payloadSize += records.back().Size();
    }

    if (!Location_->HasEnoughSpace(payloadSize)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoLocationAvailable,
            "No enough space left on location");
    }

    if (!records.empty()) {
        auto flushedRowCount = startBlockIndex + blocks.size();
        LastAppendResult_ = Changelog_->Append(records)
            .Apply(BIND([chunk = Chunk_, changelog = Changelog_, flushedRowCount] {
                chunk->UpdateFlushedRowCount(flushedRowCount);
                chunk->UpdateDataSize(changelog->GetDataSize());
            }));
    }

    return MakeFuture(TIOCounters{
        .Bytes = Changelog_->EstimateChangelogSize(payloadSize),
        .IORequests = 1,
    });
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TJournalSession::DoSendBlocks(
    int /*startBlockIndex*/,
    int /*blockCount*/,
    i64 /*cumulativeBlockSize*/,
    const TNodeDescriptor& /*target*/)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    THROW_ERROR_EXCEPTION("Sending blocks is not supported for journal chunks");
}

TFuture<ISession::TFlushBlocksResult> TJournalSession::DoFlushBlocks(int blockIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    int recordCount = Changelog_->GetRecordCount();

    if (blockIndex > recordCount) {
        auto error = TError("Missing blocks")
            << TErrorAttribute("chunk_id", SessionId_.ChunkId)
            << TErrorAttribute("medium_index", SessionId_.MediumIndex)
            << TErrorAttribute("start_block_index", recordCount - 1)
            << TErrorAttribute("end_block_index", blockIndex);

        THROW_ERROR error;
    }

    return LastAppendResult_
        .Apply(BIND([this, this_ = MakeStrong(this)] {
            i64 newDataSize = Chunk_->GetDataSize();
            auto oldDataSize = std::exchange(LastDataSize_, newDataSize);
            YT_VERIFY(oldDataSize <= newDataSize);

            // FinishChunk must induce a barrier as follows:
            // if FinishChunk succeeds and is subsequently followed by GetChunkMeta returning N rows,
            // no client writing to this chunk may ever receive a successful flush acknowlegement for >N rows.
            // See YT-21626 for the details.
            ValidateActive();

            return TFlushBlocksResult {
                .IOCounters = TIOCounters{
                    .Bytes = newDataSize - oldDataSize,
                    .IORequests = oldDataSize == newDataSize ? 0 : 1,
                },
                .ChunkWriterStatistics = New<NChunkClient::TChunkWriterStatistics>(),
            };
        }).AsyncVia(SessionInvoker_));
}

void TJournalSession::OnFinished()
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    if (Chunk_ && Changelog_) {
        Chunk_->UpdateFlushedRowCount(Changelog_->GetRecordCount());
        Chunk_->UpdateDataSize(Changelog_->GetDataSize());
    }

    if (Chunk_) {
        Chunk_->SetActive(false);

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->UpdateExistingChunk(Chunk_);
    }

    Finished_.Fire(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
