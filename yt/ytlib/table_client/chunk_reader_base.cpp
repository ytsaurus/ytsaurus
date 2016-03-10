#include "chunk_reader_base.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <algorithm>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCompression;
using namespace NTableClient::NProto;
using NConcurrency::TAsyncSemaphore;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderBase::TChunkReaderBase(
    TBlockFetcherConfigPtr config,
    NChunkClient::IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache)
    : Config_(std::move(config))
    , BlockCache_(std::move(blockCache))
    , UnderlyingReader_(std::move(underlyingReader))
    , AsyncSemaphore_(New<TAsyncSemaphore>(Config_->WindowSize))
{
    Logger = TableClientLogger;
    Logger.AddTag("ChunkId: %v", UnderlyingReader_->GetChunkId());
}

TFuture<void> TChunkReaderBase::DoOpen(
    std::vector<TBlockFetcher::TBlockInfo> blockSequence,
    const TMiscExt& miscExt)
{
    if (blockSequence.empty()) {
        return VoidFuture;
    }

    SequentialBlockFetcher_ = New<TSequentialBlockFetcher>(
        Config_,
        std::move(blockSequence),
        AsyncSemaphore_,
        UnderlyingReader_,
        BlockCache_,
        ECodec(miscExt.compression_codec()));

    InitFirstBlockNeeded_ = true;
    YCHECK(SequentialBlockFetcher_->HasMoreBlocks());
    CurrentBlock_ = SequentialBlockFetcher_->FetchNextBlock();
    ReadyEvent_ = CurrentBlock_.As<void>();
    return ReadyEvent_;
}

TFuture<void> TChunkReaderBase::GetReadyEvent()
{
    return ReadyEvent_;
}

bool TChunkReaderBase::BeginRead()
{
    if (!ReadyEvent_.IsSet()) {
        return false;
    }

    if (!ReadyEvent_.Get().IsOK()) {
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
    BlockEnded_ = false;

    if (!SequentialBlockFetcher_->HasMoreBlocks()) {
        return false;
    }

    CurrentBlock_ = SequentialBlockFetcher_->FetchNextBlock();
    ReadyEvent_ = CurrentBlock_.As<void>();
    InitNextBlockNeeded_ = true;
    return true;
}

int TChunkReaderBase::GetBlockIndexByKey(const TKey& pivotKey, const std::vector<TOwningKey>& blockIndexKeys, int beginBlockIndex)
{
    YCHECK(!blockIndexKeys.empty());
    YCHECK(beginBlockIndex < blockIndexKeys.size());
    const auto& maxKey = blockIndexKeys.back();
    if (pivotKey > maxKey) {
        return blockIndexKeys.size();
    }

    typedef decltype(blockIndexKeys.end()) TIter;
    auto rbegin = std::reverse_iterator<TIter>(blockIndexKeys.end() - 1);
    auto rend = std::reverse_iterator<TIter>(blockIndexKeys.begin() + beginBlockIndex);
    auto it = std::upper_bound(
        rbegin,
        rend,
        pivotKey,
        [] (const TKey& pivot, const TOwningKey& key) {
            return pivot > key;
        });

    return beginBlockIndex + ((it != rend) ? std::distance(it, rend) : 0);
}

void TChunkReaderBase::CheckBlockUpperLimits(
    const TBlockMeta& blockMeta,
    const NChunkClient::TReadLimit& upperLimit,
    TNullable<int> keyColumnCount)
{
    if (upperLimit.HasRowIndex()) {
        CheckRowLimit_ = upperLimit.GetRowIndex() < blockMeta.chunk_row_count();
    }

    if (upperLimit.HasKey()) {
        const auto key = FromProto<TOwningKey>(blockMeta.last_key());
        auto wideKey = WidenKey(key, keyColumnCount ? keyColumnCount.Get() : key.GetCount());

        auto upperKey = upperLimit.GetKey();
        CheckKeyLimit_ = CompareRows(
            upperKey.Begin(),
            upperKey.End(),
            wideKey.data(),
            wideKey.data() + wideKey.size()) <= 0;
    }
}

int TChunkReaderBase::ApplyLowerRowLimit(const TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& lowerLimit) const
{
    if (!lowerLimit.HasRowIndex()) {
        return 0;
    }

    const auto& blockMetaEntries = blockMeta.blocks();
    const auto& lastBlock = *(--blockMetaEntries.end());

    if (lowerLimit.GetRowIndex() >= lastBlock.chunk_row_count()) {
        LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: %v, RowCount: %v)",
            lowerLimit,
            lastBlock.chunk_row_count());

        return blockMeta.blocks_size();
    }

    typedef decltype(blockMetaEntries.end()) TIter;
    auto rbegin = std::reverse_iterator<TIter>(blockMetaEntries.end() - 1);
    auto rend = std::reverse_iterator<TIter>(blockMetaEntries.begin());
    
    auto it = std::upper_bound(
        rbegin,
        rend,
        lowerLimit.GetRowIndex(),
        [] (int index, const TBlockMeta& blockMeta) {
            // Global (chunk-wide) index of last row in block.
            auto maxRowIndex = blockMeta.chunk_row_count() - 1;
            return index > maxRowIndex;
        });

    return (it != rend) ? std::distance(it, rend) : 0;
}

int TChunkReaderBase::ApplyLowerKeyLimit(const std::vector<TOwningKey>& blockIndexKeys, const NChunkClient::TReadLimit& lowerLimit) const
{
    if (!lowerLimit.HasKey()) {
        return 0;
    }

    int blockIndex = GetBlockIndexByKey(lowerLimit.GetKey(), blockIndexKeys);
    if (blockIndex == blockIndexKeys.size()) {
        LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: %v, MaxKey: {%v})",
            lowerLimit,
            blockIndexKeys.back());
    }
    return blockIndex;
}

int TChunkReaderBase::ApplyLowerKeyLimit(const TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& lowerLimit) const
{
    if (!lowerLimit.HasKey()) {
        return 0;
    }

    const auto& blockMetaEntries = blockMeta.blocks();
    auto& lastBlock = *(--blockMetaEntries.end());
    auto maxKey = FromProto<TOwningKey>(lastBlock.last_key());
    if (lowerLimit.GetKey() > maxKey) {
        LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: %v, MaxKey: {%v})",
            lowerLimit,
            maxKey);
        return blockMetaEntries.size();
    }

    typedef decltype(blockMetaEntries.end()) TIter;
    auto rbegin = std::reverse_iterator<TIter>(blockMetaEntries.end() - 1);
    auto rend = std::reverse_iterator<TIter>(blockMetaEntries.begin());
    auto it = std::upper_bound(
        rbegin,
        rend,
        lowerLimit.GetKey(),
        [] (const TOwningKey& pivot, const TBlockMeta& block) {
            YCHECK(block.has_last_key());
            return pivot > FromProto<TOwningKey>(block.last_key());
        });

    return (it != rend) ? std::distance(it, rend) : 0;
}

int TChunkReaderBase::ApplyUpperRowLimit(const TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& upperLimit) const
{
    if (!upperLimit.HasRowIndex()) {
        return blockMeta.blocks_size();
    }

    auto begin = blockMeta.blocks().begin();
    auto end = blockMeta.blocks().end() - 1;
    auto it = std::lower_bound(
        begin,
        end,
        upperLimit.GetRowIndex(),
        [] (const TBlockMeta& blockMeta, int index) {
            auto maxRowIndex = blockMeta.chunk_row_count() - 1;
            return maxRowIndex < index;
        });

    return  (it != end) ? std::distance(begin, it) + 1 : blockMeta.blocks_size();
}

int TChunkReaderBase::ApplyUpperKeyLimit(const std::vector<TOwningKey>& blockIndexKeys, const NChunkClient::TReadLimit& upperLimit) const
{
    YCHECK(!blockIndexKeys.empty());
    if (!upperLimit.HasKey()) {
        return blockIndexKeys.size();
    }

    auto begin = blockIndexKeys.begin();
    auto end = blockIndexKeys.end() - 1;
    auto it = std::lower_bound(
        begin,
        end,
        upperLimit.GetKey(),
        [] (const TOwningKey& key, const TOwningKey& pivot) {
            return key < pivot;
        });

    return  (it != end) ? std::distance(begin, it) + 1 : blockIndexKeys.size();
}

int TChunkReaderBase::ApplyUpperKeyLimit(const TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& upperLimit) const
{
    if (!upperLimit.HasKey()) {
        return blockMeta.blocks_size();
    }

    auto begin = blockMeta.blocks().begin();
    auto end = blockMeta.blocks().end() - 1;
    auto it = std::lower_bound(
        begin,
        end,
        upperLimit.GetKey(),
        [] (const TBlockMeta& block, const TOwningKey& pivot) {
            return FromProto<TOwningKey>(block.last_key()) < pivot;
        });

    return (it != end) ? std::distance(begin, it) + 1 : blockMeta.blocks_size();
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

bool TChunkReaderBase::IsFetchingCompleted() const
{
    if (!SequentialBlockFetcher_) {
        return true;
    }
    return SequentialBlockFetcher_->IsFetchingCompleted();
}

std::vector<TChunkId> TChunkReaderBase::GetFailedChunkIds() const
{
    if (ReadyEvent_.IsSet() && !ReadyEvent_.Get().IsOK()) {
        return std::vector<TChunkId>(1, UnderlyingReader_->GetChunkId());
    } else {
        return std::vector<TChunkId>();
    }
}

std::vector<TUnversionedValue> TChunkReaderBase::WidenKey(const TOwningKey &key, int keyColumnCount)
{
    YCHECK(keyColumnCount >= key.GetCount());
    std::vector<TUnversionedValue> wideKey;
    wideKey.resize(keyColumnCount, MakeUnversionedSentinelValue(EValueType::Null));
    std::copy(key.Begin(), key.End(), wideKey.data());
    return wideKey;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
