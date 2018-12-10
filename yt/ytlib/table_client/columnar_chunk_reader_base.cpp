#include "columnar_chunk_reader_base.h"
#include "columnar_chunk_meta.h"

#include "config.h"

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/concurrency/async_semaphore.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkReaderBase::TColumnarChunkReaderBase(
    TColumnarChunkMetaPtr chunkMeta,
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    const TClientBlockReadOptions& blockReadOptions)
    : ChunkMeta_(std::move(chunkMeta))
    , Config_(std::move(config))
    , UnderlyingReader_(std::move(underlyingReader))
    , BlockCache_(std::move(blockCache))
    , BlockReadOptions_(blockReadOptions)
    , Semaphore_(New<TAsyncSemaphore>(Config_->WindowSize))
{ }

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
    if (ReadyEvent_.IsSet() && !ReadyEvent_.Get().IsOK()) {
        return { UnderlyingReader_->GetChunkId() };
    } else {
        return std::vector<TChunkId>();
    }
}

TFuture<void> TColumnarChunkReaderBase::GetReadyEvent()
{
    return ReadyEvent_;
}

void TColumnarChunkReaderBase::ResetExhaustedColumns()
{
    for (int i = 0; i < PendingBlocks_.size(); ++i) {
        if (PendingBlocks_[i]) {
            YCHECK(PendingBlocks_[i].IsSet());
            YCHECK(PendingBlocks_[i].Get().IsOK());
            Columns_[i].ColumnReader->ResetBlock(
                PendingBlocks_[i].Get().Value().Data,
                Columns_[i].PendingBlockIndex_);
        }
    }

    PendingBlocks_.clear();
}

TBlockFetcher::TBlockInfo TColumnarChunkReaderBase::CreateBlockInfo(int blockIndex) const
{
    YCHECK(ChunkMeta_);
    const auto& blockMeta = ChunkMeta_->BlockMeta()->blocks(blockIndex);
    TBlockFetcher::TBlockInfo blockInfo;
    blockInfo.Index = blockIndex;
    blockInfo.Priority = blockMeta.chunk_row_count() - blockMeta.row_count();
    blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
    return blockInfo;
}

i64 TColumnarChunkReaderBase::GetSegmentIndex(const TColumn& column, i64 rowIndex) const
{
    YCHECK(ChunkMeta_);
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

i64 TColumnarChunkReaderBase::GetLowerRowIndex(TKey key) const
{
    YCHECK(ChunkMeta_);
    auto it = std::lower_bound(
        ChunkMeta_->BlockLastKeys().begin(),
        ChunkMeta_->BlockLastKeys().end(),
        key);

    if (it == ChunkMeta_->BlockLastKeys().end()) {
        return ChunkMeta_->Misc().row_count();
    }

    if (it == ChunkMeta_->BlockLastKeys().begin()) {
        return 0;
    }
    --it;

    int blockIndex = std::distance(ChunkMeta_->BlockLastKeys().begin(), it);
    const auto& blockMeta = ChunkMeta_->BlockMeta()->blocks(blockIndex);
    return blockMeta.chunk_row_count();
}

////////////////////////////////////////////////////////////////////////////////

void TColumnarRangeChunkReaderBase::InitLowerRowIndex()
{
    LowerRowIndex_ = 0;
    if (LowerLimit_.HasRowIndex()) {
        LowerRowIndex_ = std::max(LowerRowIndex_, LowerLimit_.GetRowIndex());
    }

    if (LowerLimit_.HasKey()) {
        LowerRowIndex_ = std::max(LowerRowIndex_, GetLowerRowIndex(LowerLimit_.GetKey()));
    }
}

void TColumnarRangeChunkReaderBase::InitUpperRowIndex()
{
    SafeUpperRowIndex_ = HardUpperRowIndex_ = ChunkMeta_->Misc().row_count();
    if (UpperLimit_.HasRowIndex()) {
        SafeUpperRowIndex_ = HardUpperRowIndex_ = std::min(HardUpperRowIndex_, UpperLimit_.GetRowIndex());
    }

    if (UpperLimit_.HasKey()) {
        auto it = std::lower_bound(
            ChunkMeta_->BlockLastKeys().begin(),
            ChunkMeta_->BlockLastKeys().end(),
            UpperLimit_.GetKey());

        if (it == ChunkMeta_->BlockLastKeys().end()) {
            SafeUpperRowIndex_ = HardUpperRowIndex_ = std::min(HardUpperRowIndex_, ChunkMeta_->Misc().row_count());
        } else {
            int blockIndex = std::distance(ChunkMeta_->BlockLastKeys().begin(), it);
            const auto& blockMeta = ChunkMeta_->BlockMeta()->blocks(blockIndex);

            HardUpperRowIndex_ = std::min(
                HardUpperRowIndex_,
                blockMeta.chunk_row_count());

            if (it == ChunkMeta_->BlockLastKeys().begin()) {
                SafeUpperRowIndex_ = 0;
            } else {
                --it;

                int prevBlockIndex = std::distance(ChunkMeta_->BlockLastKeys().begin(), it);
                const auto& prevBlockMeta = ChunkMeta_->BlockMeta()->blocks(prevBlockIndex);

                SafeUpperRowIndex_ = std::min(
                    SafeUpperRowIndex_,
                    prevBlockMeta.chunk_row_count());
            }
        }
    }
}

void TColumnarRangeChunkReaderBase::Initialize(NYT::TRange<IUnversionedColumnReader*> keyReaders)
{
    for (auto& column : Columns_) {
        column.ColumnReader->SkipToRowIndex(LowerRowIndex_);
    }

    if (!LowerLimit_.HasKey()) {
        return;
    }

    YCHECK(keyReaders.Size() > 0);

    i64 lowerRowIndex = keyReaders[0]->GetCurrentRowIndex();
    i64 upperRowIndex = keyReaders[0]->GetBlockUpperRowIndex();
    int count = std::min(LowerLimit_.GetKey().GetCount(), static_cast<int>(keyReaders.Size()));
    for (int i = 0; i < count; ++i) {
        std::tie(lowerRowIndex, upperRowIndex) = keyReaders[i]->GetEqualRange(
            LowerLimit_.GetKey().Begin()[i],
            lowerRowIndex,
            upperRowIndex);
    }

    LowerRowIndex_ = count == LowerLimit_.GetKey().GetCount()
        ? lowerRowIndex
        : upperRowIndex;
    YCHECK(LowerRowIndex_ < ChunkMeta_->Misc().row_count());
    for (auto& column : Columns_) {
        column.ColumnReader->SkipToRowIndex(LowerRowIndex_);
    }
}

void TColumnarRangeChunkReaderBase::InitBlockFetcher()
{
    YCHECK(LowerRowIndex_ < ChunkMeta_->Misc().row_count());

    std::vector<TBlockFetcher::TBlockInfo> blockInfos;

    for (auto& column : Columns_) {
        if (column.ColumnMetaIndex < 0) {
            // Column without meta, blocks, etc.
            // E.g. NullColumnReader.
            continue;
        }

        const auto& columnMeta = ChunkMeta_->ColumnMeta()->columns(column.ColumnMetaIndex);
        i64 segmentIndex = GetSegmentIndex(column, LowerRowIndex_);
        column.BlockIndexSequence.push_back(columnMeta.segments(segmentIndex).block_index());

        int lastBlockIndex = -1;
        for (; segmentIndex < columnMeta.segments_size(); ++segmentIndex) {
            const auto& segment = columnMeta.segments(segmentIndex);
            if (segment.block_index() != lastBlockIndex) {
                lastBlockIndex = segment.block_index();
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
            Semaphore_,
            UnderlyingReader_,
            BlockCache_,
            NCompression::ECodec(ChunkMeta_->Misc().compression_codec()),
            static_cast<double>(ChunkMeta_->Misc().compressed_data_size()) / ChunkMeta_->Misc().uncompressed_data_size(),
            BlockReadOptions_);
    }
}

TFuture<void> TColumnarRangeChunkReaderBase::RequestFirstBlocks()
{
    PendingBlocks_.clear();

    std::vector<TFuture<void>> blockFetchResult;
    for (auto& column : Columns_) {
        if (column.BlockIndexSequence.empty()) {
            // E.g. NullColumnReader.
            PendingBlocks_.emplace_back();
        } else {
            column.PendingBlockIndex_ = column.BlockIndexSequence.front();
            PendingBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex_));
            blockFetchResult.push_back(PendingBlocks_.back().template As<void>());
        }
    }

    if (PendingBlocks_.empty()) {
        return VoidFuture;
    } else {
        return Combine(blockFetchResult);
    }
}

bool TColumnarRangeChunkReaderBase::TryFetchNextRow()
{
    std::vector<TFuture<void>> blockFetchResult;
    YCHECK(PendingBlocks_.empty());
    for (int i = 0; i < Columns_.size(); ++i) {
        auto& column = Columns_[i];
        if (column.ColumnReader->GetCurrentRowIndex() == column.ColumnReader->GetBlockUpperRowIndex()) {
            while (PendingBlocks_.size() < i) {
                PendingBlocks_.emplace_back();
            }

            auto nextBlockIndex = column.ColumnReader->GetNextBlockIndex();
            YCHECK(nextBlockIndex);
            column.PendingBlockIndex_ = *nextBlockIndex;
            PendingBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex_));
            blockFetchResult.push_back(PendingBlocks_.back().template As<void>());
        }
    }

    if (!blockFetchResult.empty()) {
        ReadyEvent_ = Combine(blockFetchResult);
    }

    return PendingBlocks_.empty();
}

////////////////////////////////////////////////////////////////////////////////

void TColumnarLookupChunkReaderBase::Initialize()
{
    RowIndexes_.reserve(Keys_.Size());
    for (const auto& key : Keys_) {
        RowIndexes_.push_back(GetLowerRowIndex(key));
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
        return;
    }

    BlockFetcher_ = New<TBlockFetcher>(
        Config_,
        std::move(blockInfos),
        Semaphore_,
        UnderlyingReader_,
        BlockCache_,
        NCompression::ECodec(ChunkMeta_->Misc().compression_codec()),
        static_cast<double>(ChunkMeta_->Misc().compressed_data_size()) / ChunkMeta_->Misc().uncompressed_data_size(),
        BlockReadOptions_);
}

bool TColumnarLookupChunkReaderBase::TryFetchNextRow()
{
    ReadyEvent_ = RequestFirstBlocks();
    return PendingBlocks_.empty();
}

TFuture<void> TColumnarLookupChunkReaderBase::RequestFirstBlocks()
{
    if (RowIndexes_[NextKeyIndex_] >= ChunkMeta_->Misc().row_count()) {
        return VoidFuture;
    }

    std::vector<TFuture<void>> blockFetchResult;
    PendingBlocks_.clear();
    for (int i = 0; i < Columns_.size(); ++i) {
        auto& column = Columns_[i];

        if (column.ColumnMetaIndex < 0) {
            // E.g. null column reader for widened keys.
            continue;
        }

        if (column.ColumnReader->GetCurrentBlockIndex() != column.BlockIndexSequence[NextKeyIndex_]) {
            while (PendingBlocks_.size() < i) {
                PendingBlocks_.emplace_back();
            }

            column.PendingBlockIndex_ = column.BlockIndexSequence[NextKeyIndex_];
            PendingBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex_));
            blockFetchResult.push_back(PendingBlocks_.back().As<void>());
        }
    }

    return Combine(blockFetchResult);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
