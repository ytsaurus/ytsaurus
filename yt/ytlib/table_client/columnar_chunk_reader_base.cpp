#include "columnar_chunk_reader_base.h"

#include "config.h"
#include "unversioned_row.h"

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/core/concurrency/async_semaphore.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

using NYT::FromProto;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkMeta::TColumnarChunkMeta(const TChunkMeta& chunkMeta)
{ 
    InitExtensions(chunkMeta);
}

void TColumnarChunkMeta::InitExtensions(const TChunkMeta& chunkMeta)
{
    ChunkType_ = EChunkType(chunkMeta.type());
    ChunkFormat_ = ETableChunkFormat(chunkMeta.version());

    Misc_ = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
    BlockMeta_ = GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions());

    // This is for old horizontal versioned chunks, since TCachedVersionedChunkMeta use this call.
    auto columnMeta = FindProtoExtension<TColumnMetaExt>(chunkMeta.extensions());
    if (columnMeta) {
        ColumnMeta_.Swap(&*columnMeta);
    }

    auto maybeKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
    auto tableSchemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
    if (maybeKeyColumnsExt) {
        FromProto(&ChunkSchema_, tableSchemaExt, *maybeKeyColumnsExt);
    } else {
        FromProto(&ChunkSchema_, tableSchemaExt);
    }
}

void TColumnarChunkMeta::InitBlockLastKeys(int keyColumnCount)
{
    BlockLastKeys_.reserve(BlockMeta_.blocks_size());
    for (const auto& block : BlockMeta_.blocks()) {
        YCHECK(block.has_last_key());
        auto key = FromProto<TOwningKey>(block.last_key());
        BlockLastKeys_.push_back(WidenKey(key, keyColumnCount));
    }
}

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkReaderBase::TColumnarChunkReaderBase(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache)
    : Config_(config)
    , UnderlyingReader_(underlyingReader)
    , BlockCache_(blockCache)
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
                PendingBlocks_[i].Get().Value(),
                Columns_[i].PendingBlockIndex_);
        }
    }

    PendingBlocks_.clear();
}

TBlockFetcher::TBlockInfo TColumnarChunkReaderBase::CreateBlockInfo(int blockIndex) const
{
    YCHECK(ChunkMeta_);
    const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(blockIndex);
    TBlockFetcher::TBlockInfo blockInfo;
    blockInfo.Index = blockIndex;
    blockInfo.Priority = blockMeta.chunk_row_count() - blockMeta.row_count();
    blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
    return blockInfo;
}

i64 TColumnarChunkReaderBase::GetSegmentIndex(const TColumn& column, i64 rowIndex) const
{
    YCHECK(ChunkMeta_);
    const auto& columnMeta = ChunkMeta_->ColumnMeta().columns(column.ColumnMetaIndex);
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
    const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(blockIndex);
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
            SafeUpperRowIndex_ = HardUpperRowIndex_ = ChunkMeta_->Misc().row_count();
        } else {
            int blockIndex = std::distance(ChunkMeta_->BlockLastKeys().begin(), it);
            const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(blockIndex);
            SafeUpperRowIndex_ = std::min(
                SafeUpperRowIndex_,
                blockMeta.chunk_row_count() - blockMeta.row_count() - 1);
            HardUpperRowIndex_ = std::min(
                HardUpperRowIndex_,
                blockMeta.chunk_row_count());
        }
    }
}

void TColumnarRangeChunkReaderBase::Initialize(NYT::TRange<std::unique_ptr<IUnversionedColumnReader>> keyReaders)
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

    LowerRowIndex_ = lowerRowIndex;
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
        const auto& columnMeta = ChunkMeta_->ColumnMeta().columns(column.ColumnMetaIndex);
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
            NCompression::ECodec(ChunkMeta_->Misc().compression_codec()));
    }
}

TFuture<void> TColumnarRangeChunkReaderBase::RequestFirstBlocks()
{
    PendingBlocks_.clear();
    for (auto& column : Columns_) {
        PendingBlocks_.push_back(BlockFetcher_->FetchBlock(column.BlockIndexSequence.front()));
        column.PendingBlockIndex_ = column.BlockIndexSequence.front();
    }

    if (PendingBlocks_.empty()) {
        return VoidFuture;
    } else {
        return Combine(PendingBlocks_).template As<void>();
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

} // namespace NTableClient
} // namespace NYT
