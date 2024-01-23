#include "chunk_reader_base.h"
#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_memory_manager.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/misc/range_formatters.h>

#include <algorithm>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCompression;
using namespace NTableClient::NProto;
using namespace NTracing;
using NConcurrency::TAsyncSemaphore;

using NYT::FromProto;
using NChunkClient::TReadLimit;
using NChunkClient::TLegacyReadLimit;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderBase::TChunkReaderBase(
    TBlockFetcherConfigPtr config,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    const TClientChunkReadOptions& chunkReadOptions,
    const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder)
    : Config_(std::move(config))
    , BlockCache_(std::move(blockCache))
    , UnderlyingReader_(std::move(underlyingReader))
    , ChunkReadOptions_(chunkReadOptions)
    , TraceContext_(CreateTraceContextFromCurrent("ChunkReader"))
    , FinishGuard_(TraceContext_)
    , Logger(TableClientLogger.WithTag("ChunkId: %v", UnderlyingReader_->GetChunkId()))
{
    if (memoryManagerHolder) {
        MemoryManagerHolder_ = memoryManagerHolder;
    } else {
        MemoryManagerHolder_ = TChunkReaderMemoryManager::Create(TChunkReaderMemoryManagerOptions(Config_->WindowSize));
    }

    MemoryManagerHolder_->Get()->AddReadSessionInfo(ChunkReadOptions_.ReadSessionId);

    if (ChunkReadOptions_.ReadSessionId) {
        Logger.AddTag("ReadSessionId: %v", ChunkReadOptions_.ReadSessionId);
    }
}

TChunkReaderBase::~TChunkReaderBase()
{
    YT_LOG_DEBUG("Chunk reader timing statistics (TimingStatistics: %v)",
        TTimingReaderBase::GetTimingStatistics());
}

TFuture<void> TChunkReaderBase::DoOpen(
    std::vector<TBlockFetcher::TBlockInfo> blockSequence,
    const TMiscExt& miscExt,
    IInvokerPtr sessionInvoker)
{
    TCurrentTraceContextGuard traceGuard(TraceContext_);

    if (blockSequence.empty()) {
        // NB(psushin): typically memory manager is finalized by block fetcher.
        // When block fetcher is not created, we should do it explicitly.
        YT_UNUSED_FUTURE(MemoryManagerHolder_->Get()->Finalize());
        return VoidFuture;
    }

    SequentialBlockFetcher_ = New<TSequentialBlockFetcher>(
        Config_,
        std::move(blockSequence),
        MemoryManagerHolder_,
        std::vector{UnderlyingReader_},
        BlockCache_,
        CheckedEnumCast<ECodec>(miscExt.compression_codec()),
        static_cast<double>(miscExt.compressed_data_size()) / miscExt.uncompressed_data_size(),
        ChunkReadOptions_,
        std::move(sessionInvoker));
    SequentialBlockFetcher_->Start();

    InitFirstBlockNeeded_ = true;
    YT_VERIFY(SequentialBlockFetcher_->HasMoreBlocks());
    MemoryManagerHolder_->Get()->SetRequiredMemorySize(SequentialBlockFetcher_->GetNextBlockSize());
    // FIXME(akozhikhov): This may result in a redundant GetBlockSet call.
    CurrentBlock_ = SequentialBlockFetcher_->FetchNextBlock();
    return CurrentBlock_.As<void>();
}

bool TChunkReaderBase::BeginRead()
{
    if (!ReadyEvent().IsSet()) {
        return false;
    }

    if (!ReadyEvent().Get().IsOK()) {
        return false;
    }

    if (InitFirstBlockNeeded_) {
        InitFirstBlock();
        InitFirstBlockNeeded_ = false;
    }

    if (InitNextBlockNeeded_) {
        InitNextBlock();
        InitNextBlockNeeded_ = false;
    }

    return true;
}

bool TChunkReaderBase::OnBlockEnded()
{
    TCurrentTraceContextGuard traceGuard(TraceContext_);

    BlockEnded_ = false;

    if (!SequentialBlockFetcher_->HasMoreBlocks()) {
        return false;
    }

    MemoryManagerHolder_->Get()->SetRequiredMemorySize(SequentialBlockFetcher_->GetNextBlockSize());
    CurrentBlock_ = SequentialBlockFetcher_->FetchNextBlock();
    SetReadyEvent(CurrentBlock_.As<void>());
    InitNextBlockNeeded_ = true;
    return true;
}

void TChunkReaderBase::CheckBlockUpperKeyLimit(
    TLegacyKey blockLastKey,
    TLegacyKey upperLimit,
    int keyColumnCount,
    int commonKeyPrefix)
{
    CheckKeyLimit_ = !TestKeyWithWidening(
        ToKeyRef(blockLastKey, commonKeyPrefix),
        ToKeyBoundRef(upperLimit, true, keyColumnCount));
}

void TChunkReaderBase::CheckBlockUpperLimits(
    i64 blockChunkRowCount,
    TUnversionedRow blockLastKey,
    const TReadLimit& upperLimit,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix)
{
    if (upperLimit.GetRowIndex()) {
        CheckRowLimit_ = *upperLimit.GetRowIndex() < blockChunkRowCount;
    }

    if (upperLimit.KeyBound() && blockLastKey) {
        CheckKeyLimit_ = !TestKeyWithWidening(
            ToKeyRef(blockLastKey, commonKeyPrefix),
            ToKeyBoundRef(upperLimit.KeyBound()),
            sortOrders);
    }
}

int TChunkReaderBase::ApplyLowerRowLimit(const TDataBlockMetaExt& blockMeta, const TReadLimit& lowerLimit) const
{
    if (!lowerLimit.GetRowIndex()) {
        return 0;
    }

    const auto& blockMetaEntries = blockMeta.data_blocks();
    const auto& lastBlock = *(--blockMetaEntries.end());

    if (*lowerLimit.GetRowIndex() >= lastBlock.chunk_row_count()) {
        YT_LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: %v, RowCount: %v)",
            lowerLimit,
            lastBlock.chunk_row_count());

        return blockMeta.data_blocks_size();
    }

    using TIter = decltype(blockMetaEntries.end());
    auto rbegin = std::reverse_iterator<TIter>(blockMetaEntries.end() - 1);
    auto rend = std::reverse_iterator<TIter>(blockMetaEntries.begin());

    auto it = std::upper_bound(
        rbegin,
        rend,
        *lowerLimit.GetRowIndex(),
        [] (i64 index, const TDataBlockMeta& blockMeta) {
            // Global (chunk-wide) index of last row in block.
            auto maxRowIndex = blockMeta.chunk_row_count() - 1;
            return index > maxRowIndex;
        });

    return (it != rend) ? std::distance(it, rend) : 0;
}

int TChunkReaderBase::ApplyLowerKeyLimit(
    const TSharedRange<TUnversionedRow>& blockLastKeys,
    const TReadLimit& lowerLimit,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix) const
{
    const auto& lowerBound = lowerLimit.KeyBound();
    if (!lowerBound) {
        return 0;
    }

    YT_VERIFY(!lowerBound.IsUpper);

    // BinarySearch returns first iterator such that !pred(it).
    // We are looking for the first block such that comparator.TestKey(blockLastKey, lowerBound).
    auto it = BinarySearch(
        blockLastKeys.begin(),
        blockLastKeys.end(),
        [&] (const TUnversionedRow* blockLastKey) {
            return !TestKeyWithWidening(
                ToKeyRef(*blockLastKey, commonKeyPrefix),
                ToKeyBoundRef(lowerBound),
                sortOrders);
        });

    if (it == blockLastKeys.end()) {
        YT_LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: %v, MaxKey: %v, SortOrders: %v)",
            lowerLimit,
            blockLastKeys[blockLastKeys.size() - 1],
            sortOrders);
        return blockLastKeys.size();
    }

    return std::distance(blockLastKeys.begin(), it);
}

int TChunkReaderBase::ApplyUpperRowLimit(const TDataBlockMetaExt& blockMeta, const TReadLimit& upperLimit) const
{
    if (!upperLimit.GetRowIndex()) {
        return blockMeta.data_blocks_size();
    }

    auto begin = blockMeta.data_blocks().begin();
    auto end = blockMeta.data_blocks().end() - 1;
    auto it = std::lower_bound(
        begin,
        end,
        *upperLimit.GetRowIndex(),
        [] (const TDataBlockMeta& blockMeta, i64 index) {
            auto maxRowIndex = blockMeta.chunk_row_count() - 1;
            return maxRowIndex < index;
        });

    return  (it != end) ? std::distance(begin, it) + 1 : blockMeta.data_blocks_size();
}

int TChunkReaderBase::ApplyUpperKeyLimit(
    const TSharedRange<TUnversionedRow>& blockLastKeys,
    const TReadLimit& upperLimit,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix) const
{
    const auto& upperBound = upperLimit.KeyBound();
    if (!upperBound) {
        return blockLastKeys.size();
    }

    YT_VERIFY(upperBound.IsUpper);

    // BinarySearch returns first iterator such that !pred(it).
    // We are looking for the first block such that !comparator.TestKey(blockLastKey, upperBound).
    auto it = BinarySearch(
        blockLastKeys.begin(),
        blockLastKeys.end(),
        [&] (const TUnversionedRow* blockLastKey) {
            return TestKeyWithWidening(
                ToKeyRef(*blockLastKey, commonKeyPrefix),
                ToKeyBoundRef(upperBound),
                sortOrders);
        });

    // TODO(gritukan): Probably half-opened intervals here is not that great idea.
    if (it == blockLastKeys.end()) {
        return blockLastKeys.size();
    } else {
        return std::distance(blockLastKeys.begin(), it) + 1;
    }
}

TDataStatistics TChunkReaderBase::GetDataStatistics() const
{
    if (!SequentialBlockFetcher_) {
        return TDataStatistics();
    }

    TDataStatistics dataStatistics;
    dataStatistics.set_chunk_count(1);
    dataStatistics.set_uncompressed_data_size(SequentialBlockFetcher_->GetUncompressedDataSize());
    dataStatistics.set_compressed_data_size(SequentialBlockFetcher_->GetCompressedDataSize());
    return dataStatistics;
}

TCodecStatistics TChunkReaderBase::GetDecompressionStatistics() const
{
    TCodecStatistics statistics;
    if (SequentialBlockFetcher_) {
        statistics.Append(SequentialBlockFetcher_->GetDecompressionTime());
    }
    return statistics;
}

bool TChunkReaderBase::IsFetchingCompleted() const
{
    if (!SequentialBlockFetcher_) {
        return true;
    }
    return SequentialBlockFetcher_->IsFetchingCompleted();
}

std::vector<TChunkId> TChunkReaderBase::GetFailedChunkIds() const
{
    if (ReadyEvent().IsSet() && !ReadyEvent().Get().IsOK()) {
        return std::vector<TChunkId>(1, UnderlyingReader_->GetChunkId());
    } else {
        return std::vector<TChunkId>();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
