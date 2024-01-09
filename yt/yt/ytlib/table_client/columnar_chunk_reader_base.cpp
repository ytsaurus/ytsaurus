#include "columnar_chunk_reader_base.h"
#include "columnar_chunk_meta.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_memory_manager.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;
using namespace NTracing;

using NChunkClient::TLegacyReadLimit;

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkReaderBase::TColumnarChunkReaderBase(
    TColumnarChunkMetaPtr chunkMeta,
    const std::optional<NChunkClient::TDataSource>& dataSource,
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    IBlockCachePtr blockCache,
    const TClientChunkReadOptions& chunkReadOptions,
    std::function<void(int)> onRowsSkipped,
    const TChunkReaderMemoryManagerPtr& memoryManager)
    : ChunkMeta_(std::move(chunkMeta))
    , Config_(std::move(config))
    , UnderlyingReader_(std::move(underlyingReader))
    , BlockCache_(std::move(blockCache))
    , ChunkReadOptions_(chunkReadOptions)
    , SortOrders_(sortOrders.begin(), sortOrders.end())
    , CommonKeyPrefix_(commonKeyPrefix)
    , Sampler_(Config_->SamplingRate, std::random_device()())
    , OnRowsSkipped_(onRowsSkipped)
    , TraceContext_(CreateTraceContextFromCurrent("ChunkReader"))
    , FinishGuard_(TraceContext_)
{
    if (memoryManager) {
        MemoryManager_ = memoryManager;
    } else {
        MemoryManager_ = New<TChunkReaderMemoryManager>(TChunkReaderMemoryManagerOptions(Config_->WindowSize));
    }

    if (Config_->SamplingSeed) {
        auto chunkId = UnderlyingReader_->GetChunkId();
        auto seed = *Config_->SamplingSeed;
        seed ^= FarmFingerprint(chunkId.Parts64[0]);
        seed ^= FarmFingerprint(chunkId.Parts64[1]);
        Sampler_ = TBernoulliSampler(Config_->SamplingRate, seed);
    }

    if (dataSource) {
        PackBaggageForChunkReader(TraceContext_, *dataSource, MakeExtraChunkTags(ChunkMeta_->Misc()));
    }
}

TDataStatistics TColumnarChunkReaderBase::GetDataStatistics() const
{
    if (!BlockFetcher_) {
        return TDataStatistics();
    }

    TDataStatistics dataStatistics;
    dataStatistics.set_chunk_count(1);
    dataStatistics.set_uncompressed_data_size(BlockFetcher_->GetUncompressedDataSize());
    dataStatistics.set_compressed_data_size(BlockFetcher_->GetCompressedDataSize());
    return dataStatistics;
}

TCodecStatistics TColumnarChunkReaderBase::GetDecompressionStatistics() const
{
    return BlockFetcher_
        ? TCodecStatistics().Append(BlockFetcher_->GetDecompressionTime())
        : TCodecStatistics();
}

bool TColumnarChunkReaderBase::IsFetchingCompleted() const
{
    if (BlockFetcher_) {
        return BlockFetcher_->IsFetchingCompleted();
    } else {
        return true;
    }
}

std::vector<TChunkId> TColumnarChunkReaderBase::GetFailedChunkIds() const
{
    if (ReadyEvent().IsSet() && !ReadyEvent().Get().IsOK()) {
        return { UnderlyingReader_->GetChunkId() };
    } else {
        return std::vector<TChunkId>();
    }
}

void TColumnarChunkReaderBase::FeedBlocksToReaders()
{
    for (int i = 0; i < std::ssize(PendingBlocks_); ++i) {
        const auto& blockFuture = PendingBlocks_[i];
        const auto& column = Columns_[i];
        const auto& columnReader = column.ColumnReader;
        if (blockFuture) {
            YT_VERIFY(blockFuture.IsSet() && blockFuture.Get().IsOK());

            if (columnReader->GetCurrentBlockIndex() != -1) {
                RequiredMemorySize_ -= BlockFetcher_->GetBlockSize(columnReader->GetCurrentBlockIndex());
            }
            MemoryManager_->SetRequiredMemorySize(RequiredMemorySize_);

            const auto& block = blockFuture.Get().Value();
            columnReader->SetCurrentBlock(block.Data, column.PendingBlockIndex);
        }
    }

    if (SampledRangeIndexChanged_) {
        auto rowIndex = *SampledRanges_[SampledRangeIndex_].LowerLimit().GetRowIndex();
        for (auto& column : Columns_) {
            column.ColumnReader->SkipToRowIndex(rowIndex);
        }

        SampledRangeIndexChanged_ = false;
    }

    PendingBlocks_.clear();
}

void TColumnarChunkReaderBase::ArmColumnReaders()
{
    for (const auto& column : Columns_) {
        column.ColumnReader->Rearm();
    }
}

i64 TColumnarChunkReaderBase::GetReadyRowCount() const
{
    i64 result = Max<i64>();
    for (const auto& column : Columns_) {
        const auto& reader = column.ColumnReader;
        result = std::min(
            result,
            reader->GetReadyUpperRowIndex() - reader->GetCurrentRowIndex());
        if (SampledColumnIndex_) {
            const auto& sampledColumnReader = Columns_[*SampledColumnIndex_].ColumnReader;
            result = std::min(
                result,
                *SampledRanges_[SampledRangeIndex_].UpperLimit().GetRowIndex() - sampledColumnReader->GetCurrentRowIndex());
        }
    }
    return result;
}

TBlockFetcher::TBlockInfo TColumnarChunkReaderBase::CreateBlockInfo(int blockIndex) const
{
    YT_VERIFY(ChunkMeta_);
    // NB: This reader can only read data blocks, hence in block infos we set block type to UncompressedData.
    YT_VERIFY(blockIndex < ChunkMeta_->DataBlockMeta()->data_blocks_size());
    const auto& blockMeta = ChunkMeta_->DataBlockMeta()->data_blocks(blockIndex);
    return {
        .ReaderIndex = 0,
        .BlockIndex = blockIndex,
        .Priority = static_cast<int>(blockMeta.chunk_row_count() - blockMeta.row_count()),
        .UncompressedDataSize = blockMeta.uncompressed_size(),
        .BlockType = EBlockType::UncompressedData,
    };
}

i64 TColumnarChunkReaderBase::GetSegmentIndex(const TColumn& column, i64 rowIndex) const
{
    YT_VERIFY(ChunkMeta_);
    const auto& columnMeta = ChunkMeta_->ColumnMeta()->columns(column.ColumnMetaIndex);
    auto it = std::lower_bound(
        columnMeta.segments().begin(),
        columnMeta.segments().end(),
        rowIndex,
        [](const TSegmentMeta& segmentMeta, i64 index) {
            return segmentMeta.chunk_row_count() < index + 1;
        });
    return std::distance(columnMeta.segments().begin(), it);
}

i64 TColumnarChunkReaderBase::GetLowerKeyBoundIndex(TKeyBound lowerBound) const
{
    YT_VERIFY(ChunkMeta_);
    YT_VERIFY(lowerBound);
    YT_VERIFY(!lowerBound.IsUpper);

    const auto& blockLastKeys = ChunkMeta_->BlockLastKeys();

    // BinarySearch returns first element such that !pred(it).
    // We are looking for the first block such that Comparator_.TestKey(blockLastKey, lowerBound);
    auto it = BinarySearch(
        blockLastKeys.begin(),
        blockLastKeys.end(),
        [&] (const TUnversionedRow* blockLastKey) {
            return !TestKeyWithWidening(
                ToKeyRef(*blockLastKey, CommonKeyPrefix_),
                ToKeyBoundRef(lowerBound),
                SortOrders_);
        });

    if (it == blockLastKeys.end()) {
        return ChunkMeta_->Misc().row_count();
    }

    if (it == blockLastKeys.begin()) {
        return 0;
    }
    --it;

    int blockIndex = std::distance(blockLastKeys.begin(), it);
    const auto& blockMeta = ChunkMeta_->DataBlockMeta()->data_blocks(blockIndex);
    return blockMeta.chunk_row_count();
}

bool TColumnarChunkReaderBase::IsSamplingCompleted() const
{
    return IsSamplingCompleted_;
}

////////////////////////////////////////////////////////////////////////////////

void TColumnarRangeChunkReaderBase::InitLowerRowIndex()
{
    LowerRowIndex_ = 0;
    if (LowerLimit_.GetRowIndex()) {
        LowerRowIndex_ = std::max(LowerRowIndex_, *LowerLimit_.GetRowIndex());
    }

    if (LowerLimit_.KeyBound()) {
        LowerRowIndex_ = std::max(LowerRowIndex_, GetLowerKeyBoundIndex(LowerLimit_.KeyBound()));
    }
}

void TColumnarRangeChunkReaderBase::InitUpperRowIndex()
{
    SafeUpperRowIndex_ = HardUpperRowIndex_ = ChunkMeta_->Misc().row_count();
    if (UpperLimit_.GetRowIndex()) {
        SafeUpperRowIndex_ = HardUpperRowIndex_ = std::min(HardUpperRowIndex_, *UpperLimit_.GetRowIndex());
    }

    if (UpperLimit_.KeyBound()) {
        const auto& blockLastKeys = ChunkMeta_->BlockLastKeys();

        // BinarySearch returns first element such that !pred(it).
        // We are looking for the first block such that !Comparator_.TestKey(blockLastKey, upperBound);
        auto it = BinarySearch(
            blockLastKeys.begin(),
            blockLastKeys.end(),
            [&] (const TUnversionedRow* blockLastKey) {
                return TestKeyWithWidening(
                    ToKeyRef(*blockLastKey, CommonKeyPrefix_),
                    ToKeyBoundRef(UpperLimit_.KeyBound()),
                    SortOrders_);
            });

        if (it == blockLastKeys.end()) {
            SafeUpperRowIndex_ = HardUpperRowIndex_ = std::min(HardUpperRowIndex_, ChunkMeta_->Misc().row_count());
        } else {
            int blockIndex = std::distance(blockLastKeys.begin(), it);
            const auto& blockMeta = ChunkMeta_->DataBlockMeta()->data_blocks(blockIndex);

            HardUpperRowIndex_ = std::min(
                HardUpperRowIndex_,
                blockMeta.chunk_row_count());

            if (it == blockLastKeys.begin()) {
                SafeUpperRowIndex_ = 0;
            } else {
                --it;

                int prevBlockIndex = std::distance(blockLastKeys.begin(), it);
                const auto& prevBlockMeta = ChunkMeta_->DataBlockMeta()->data_blocks(prevBlockIndex);

                SafeUpperRowIndex_ = std::min(
                    SafeUpperRowIndex_,
                    prevBlockMeta.chunk_row_count());
            }
        }
    }
}

void TColumnarRangeChunkReaderBase::Initialize(NYT::TRange<IUnversionedColumnReader*> keyReaders)
{
    for (const auto& column : Columns_) {
        column.ColumnReader->SkipToRowIndex(LowerRowIndex_);
    }

    if (!LowerLimit_.KeyBound()) {
        return;
    }

    YT_VERIFY(keyReaders.Size() > 0);

    i64 lowerRowIndex = keyReaders[0]->GetCurrentRowIndex();
    i64 upperRowIndex = keyReaders[0]->GetBlockUpperRowIndex();
    int count = std::min<int>(LowerLimit_.KeyBound().Prefix.GetCount(), keyReaders.Size());
    for (int i = 0; i < count; ++i) {
        std::tie(lowerRowIndex, upperRowIndex) = keyReaders[i]->GetEqualRange(
            LowerLimit_.KeyBound().Prefix[i],
            lowerRowIndex,
            upperRowIndex);
    }

    if (LowerLimit_.KeyBound().IsInclusive) {
        LowerRowIndex_ = lowerRowIndex;
    } else {
        LowerRowIndex_ = upperRowIndex;
    }

    YT_VERIFY(LowerRowIndex_ < ChunkMeta_->Misc().row_count());
    for (const auto& column : Columns_) {
        column.ColumnReader->SkipToRowIndex(LowerRowIndex_);
    }
}

void TColumnarRangeChunkReaderBase::InitBlockFetcher(IInvokerPtr sessionInvoker)
{
    YT_VERIFY(LowerRowIndex_ < ChunkMeta_->Misc().row_count());

    TCurrentTraceContextGuard guard(TraceContext_);

    std::vector<TBlockFetcher::TBlockInfo> blockInfos;

    if (Config_->SamplingMode == ESamplingMode::Block) {
        // Select column to sample.
        int maxColumnSegmentCount = -1;
        for (int columnIndex = 0; columnIndex < std::ssize(Columns_); ++columnIndex) {
            const auto& column = Columns_[columnIndex];
            auto columnMetaIndex = column.ColumnMetaIndex;
            if (columnMetaIndex < 0) {
                continue;
            }
            const auto& columnMeta = ChunkMeta_->ColumnMeta()->columns(columnMetaIndex);
            auto columnSegmentCount = columnMeta.segments_size();
            if (columnSegmentCount > maxColumnSegmentCount) {
                maxColumnSegmentCount = columnSegmentCount;
                SampledColumnIndex_ = columnIndex;
            }
        }

        if (!SampledColumnIndex_) {
            return;
        }

        // Sample column blocks.
        const auto& column = Columns_[*SampledColumnIndex_];
        const auto& columnMeta = ChunkMeta_->ColumnMeta()->columns(column.ColumnMetaIndex);
        int segmentIndex = GetSegmentIndex(column, LowerRowIndex_);
        while (segmentIndex < columnMeta.segments_size()) {
            const auto& segment = columnMeta.segments(segmentIndex);
            if (segment.chunk_row_count() - segment.row_count() > HardUpperRowIndex_) {
                break;
            }
            auto blockIndex = segment.block_index();
            int nextBlockSegmentIndex = segmentIndex;
            while (
                nextBlockSegmentIndex < columnMeta.segments_size() &&
                columnMeta.segments(nextBlockSegmentIndex).block_index() == blockIndex)
            {
                ++nextBlockSegmentIndex;
            }

            const auto& lastBlockSegment = columnMeta.segments(nextBlockSegmentIndex - 1);
            if (Sampler_.Sample(blockIndex)) {
                NChunkClient::TReadRange readRange;
                readRange.LowerLimit().SetRowIndex(std::max<i64>(segment.chunk_row_count() - segment.row_count(), LowerRowIndex_));
                readRange.UpperLimit().SetRowIndex(std::min<i64>(lastBlockSegment.chunk_row_count(), HardUpperRowIndex_ + 1));
                SampledRanges_.push_back(std::move(readRange));
            }

            segmentIndex = nextBlockSegmentIndex;
        }

        if (SampledRanges_.empty()) {
            IsSamplingCompleted_ = true;
        } else {
            LowerRowIndex_ = *SampledRanges_[0].LowerLimit().GetRowIndex();
        }
    }

    for (auto& column : Columns_) {
        if (column.ColumnMetaIndex < 0) {
            // Column without meta, blocks, etc.
            // E.g. NullColumnReader.
            continue;
        }

        const auto& columnMeta = ChunkMeta_->ColumnMeta()->columns(column.ColumnMetaIndex);
        i64 segmentIndex = GetSegmentIndex(column, LowerRowIndex_);

        int lastBlockIndex = -1;
        int sampledRangeIndex = 0;
        for (; segmentIndex < columnMeta.segments_size(); ++segmentIndex) {
            const auto& segment = columnMeta.segments(segmentIndex);
            int firstRowIndex = segment.chunk_row_count() - segment.row_count();
            int lastRowIndex = segment.chunk_row_count() - 1;
            if (SampledColumnIndex_) {
                while (
                    sampledRangeIndex < std::ssize(SampledRanges_) &&
                    *SampledRanges_[sampledRangeIndex].UpperLimit().GetRowIndex() <= firstRowIndex)
                {
                    ++sampledRangeIndex;
                }
                if (sampledRangeIndex == std::ssize(SampledRanges_)) {
                    break;
                }
                if (*SampledRanges_[sampledRangeIndex].LowerLimit().GetRowIndex() > lastRowIndex) {
                    continue;
                }
            }
            if (segment.block_index() != lastBlockIndex) {
                lastBlockIndex = segment.block_index();
                if (column.BlockIndexSequence.empty()) {
                    column.BlockIndexSequence.push_back(lastBlockIndex);
                }
                blockInfos.push_back(CreateBlockInfo(lastBlockIndex));
            }
            if (segment.chunk_row_count() > HardUpperRowIndex_) {
                break;
            }
        }
    }

    if (!blockInfos.empty()) {
        BlockFetcher_ = New<TBlockFetcher>(
            Config_,
            std::move(blockInfos),
            MemoryManager_,
            std::vector{UnderlyingReader_},
            BlockCache_,
            CheckedEnumCast<NCompression::ECodec>(ChunkMeta_->Misc().compression_codec()),
            static_cast<double>(ChunkMeta_->Misc().compressed_data_size()) / ChunkMeta_->Misc().uncompressed_data_size(),
            ChunkReadOptions_,
            std::move(sessionInvoker));
        BlockFetcher_->Start();
    }
}

TFuture<void> TColumnarRangeChunkReaderBase::RequestFirstBlocks()
{
    TCurrentTraceContextGuard guard(TraceContext_);

    PendingBlocks_.clear();

    std::vector<TFuture<void>> blockFetchResult;
    for (auto& column : Columns_) {
        if (column.BlockIndexSequence.empty()) {
            // E.g. NullColumnReader.
            PendingBlocks_.emplace_back();
        } else {
            column.PendingBlockIndex = column.BlockIndexSequence.front();
            RequiredMemorySize_ += BlockFetcher_->GetBlockSize(column.PendingBlockIndex);
            MemoryManager_->SetRequiredMemorySize(RequiredMemorySize_);
            PendingBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex));
            blockFetchResult.push_back(PendingBlocks_.back().template As<void>());
        }
    }

    if (PendingBlocks_.empty()) {
        return VoidFuture;
    } else {
        return AllSucceeded(blockFetchResult);
    }
}

bool TColumnarRangeChunkReaderBase::TryFetchNextRow()
{
    std::vector<TFuture<void>> blockFetchResult;
    YT_VERIFY(PendingBlocks_.empty());
    YT_VERIFY(!IsSamplingCompleted_);

    if (SampledColumnIndex_) {
        auto& sampledColumn = Columns_[*SampledColumnIndex_];
        const auto& sampledColumnReader = sampledColumn.ColumnReader;
        if (sampledColumnReader->GetCurrentRowIndex() == SampledRanges_[SampledRangeIndex_].UpperLimit().GetRowIndex()) {
            ++SampledRangeIndex_;
            SampledRangeIndexChanged_ = true;
            if (SampledRangeIndex_ == std::ssize(SampledRanges_)) {
                IsSamplingCompleted_ = true;
                return false;
            }

            int rowsSkipped = *SampledRanges_[SampledRangeIndex_].LowerLimit().GetRowIndex() - sampledColumnReader->GetCurrentRowIndex();
            if (OnRowsSkipped_) {
                OnRowsSkipped_(rowsSkipped);
            }
        }
    }

    for (int columnIndex = 0; columnIndex < std::ssize(Columns_); ++columnIndex) {
        auto& column = Columns_[columnIndex];
        const auto& columnReader = column.ColumnReader;
        auto currentRowIndex = columnReader->GetCurrentRowIndex();
        if (SampledRangeIndexChanged_) {
            currentRowIndex = *SampledRanges_[SampledRangeIndex_].LowerLimit().GetRowIndex();
        }

        if (currentRowIndex >= columnReader->GetBlockUpperRowIndex()) {
            while (std::ssize(PendingBlocks_) < columnIndex) {
                PendingBlocks_.emplace_back();
            }

            const auto& columnMeta = ChunkMeta_->ColumnMeta()->columns(column.ColumnMetaIndex);
            int nextSegmentIndex = columnReader->GetCurrentSegmentIndex();
            while (columnMeta.segments(nextSegmentIndex).chunk_row_count() <= currentRowIndex) {
                ++nextSegmentIndex;
            }
            const auto& nextSegment = columnMeta.segments(nextSegmentIndex);
            auto nextBlockIndex = nextSegment.block_index();
            column.PendingBlockIndex = nextBlockIndex;

            RequiredMemorySize_ += BlockFetcher_->GetBlockSize(column.PendingBlockIndex);
            MemoryManager_->SetRequiredMemorySize(RequiredMemorySize_);
            PendingBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex));
            blockFetchResult.push_back(PendingBlocks_.back().template As<void>());
        }
    }

    if (!blockFetchResult.empty()) {
        SetReadyEvent(AllSucceeded(blockFetchResult));
    }

    return PendingBlocks_.empty();
}

////////////////////////////////////////////////////////////////////////////////

void TColumnarLookupChunkReaderBase::Initialize()
{
    RowIndexes_.reserve(Keys_.Size());
    for (const auto& key : Keys_) {
        RowIndexes_.push_back(GetLowerKeyBoundIndex(TKeyBound::FromRowUnchecked() >= key));
    }

    for (auto& column : Columns_) {
        if (column.ColumnMetaIndex < 0) {
            // E.g. null column reader for widened keys.
            continue;
        }

        for (auto rowIndex : RowIndexes_) {
            if (rowIndex < ChunkMeta_->Misc().row_count()) {
                const auto& columnMeta = ChunkMeta_->ColumnMeta()->columns(column.ColumnMetaIndex);
                auto segmentIndex = GetSegmentIndex(column, rowIndex);
                const auto& segment = columnMeta.segments(segmentIndex);
                column.BlockIndexSequence.push_back(segment.block_index());
            } else {
                // All keys left are outside boundary keys.
                break;
            }
        }
    }

    InitBlockFetcher();
}

void TColumnarLookupChunkReaderBase::InitBlockFetcher()
{
    TCurrentTraceContextGuard guard(TraceContext_);

    std::vector<TBlockFetcher::TBlockInfo> blockInfos;
    for (const auto& column : Columns_) {
        int lastBlockIndex = -1;
        for (auto blockIndex : column.BlockIndexSequence) {
            if (blockIndex != lastBlockIndex) {
                lastBlockIndex = blockIndex;
                blockInfos.push_back(CreateBlockInfo(lastBlockIndex));
            }
        }
    }

    if (blockInfos.empty()) {
        // NB(psushin): typically memory manager is finalized by block fetcher.
        // When block fetcher is not created, we should do it explicitly.
        YT_UNUSED_FUTURE(MemoryManager_->Finalize());
        return;
    }

    BlockFetcher_ = New<TBlockFetcher>(
        Config_,
        std::move(blockInfos),
        MemoryManager_,
        std::vector{UnderlyingReader_},
        BlockCache_,
        CheckedEnumCast<NCompression::ECodec>(ChunkMeta_->Misc().compression_codec()),
        static_cast<double>(ChunkMeta_->Misc().compressed_data_size()) / ChunkMeta_->Misc().uncompressed_data_size(),
        ChunkReadOptions_);
    BlockFetcher_->Start();
}

bool TColumnarLookupChunkReaderBase::TryFetchNextRow()
{
    SetReadyEvent(RequestFirstBlocks());
    return PendingBlocks_.empty();
}

TFuture<void> TColumnarLookupChunkReaderBase::RequestFirstBlocks()
{
    TCurrentTraceContextGuard guard(TraceContext_);

    if (RowIndexes_[NextKeyIndex_] >= ChunkMeta_->Misc().row_count()) {
        return VoidFuture;
    }

    std::vector<TFuture<void>> blockFetchResult;
    PendingBlocks_.clear();
    for (int i = 0; i < std::ssize(Columns_); ++i) {
        auto& column = Columns_[i];

        if (column.ColumnMetaIndex < 0) {
            // E.g. null column reader for widened keys.
            continue;
        }

        if (column.ColumnReader->GetCurrentBlockIndex() != column.BlockIndexSequence[NextKeyIndex_]) {
            while (std::ssize(PendingBlocks_) < i) {
                PendingBlocks_.emplace_back();
            }

            column.PendingBlockIndex = column.BlockIndexSequence[NextKeyIndex_];
            RequiredMemorySize_ += BlockFetcher_->GetBlockSize(column.PendingBlockIndex);
            MemoryManager_->SetRequiredMemorySize(RequiredMemorySize_);
            PendingBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex));
            blockFetchResult.push_back(PendingBlocks_.back().As<void>());
        }
    }

    return AllSucceeded(blockFetchResult);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
