#include "stdafx.h"

#include "chunk_reader_base.h"

#include "config.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/compression/codec.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCompression;
using namespace NConcurrency;
using namespace NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderBase::TChunkReaderBase(
    TChunkReaderConfigPtr config,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    NChunkClient::IChunkReaderPtr underlyingReader,
    const NChunkClient::NProto::TMiscExt& misc,
    IBlockCachePtr uncompressedBlockCache)
    : Logger(TableClientLogger)
    , Config_(config)
    , LowerLimit_(lowerLimit)
    , UpperLimit_(upperLimit)
    , UncompressedBlockCache_(uncompressedBlockCache)
    , UnderlyingReader_(underlyingReader)
    , Misc_(misc)
{
    Logger.AddTag("ChunkId: %v", UnderlyingReader_->GetChunkId());
}

TFuture<void> TChunkReaderBase::Open()
{
    ReadyEvent_ = BIND(&TChunkReaderBase::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
    return ReadyEvent_;
}

TFuture<void> TChunkReaderBase::GetReadyEvent()
{
    return ReadyEvent_;
}

void TChunkReaderBase::DoOpen()
{
    auto blocks = GetBlockSequence();

    if (blocks.empty()) {
        return;
    }

    SequentialReader_ = New<TSequentialReader>(
        Config_,
        std::move(blocks),
        UnderlyingReader_,
        UncompressedBlockCache_,
        ECodec(Misc_.compression_codec()));

    YCHECK(SequentialReader_->HasMoreBlocks());
    WaitFor(SequentialReader_->FetchNextBlock())
        .ThrowOnError();

    InitFirstBlock();
}

void TChunkReaderBase::DoSwitchBlock()
{
    WaitFor(SequentialReader_->FetchNextBlock())
        .ThrowOnError();
    InitNextBlock();
}

bool TChunkReaderBase::OnBlockEnded()
{
    BlockEnded_ = false;

    if (!SequentialReader_->HasMoreBlocks()) {
        return false;
    }

    ReadyEvent_ = BIND(&TChunkReaderBase::DoSwitchBlock, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
    return true;
}

int TChunkReaderBase::ApplyLowerRowLimit(const TBlockMetaExt& blockMeta) const
{
    if (!LowerLimit_.HasRowIndex()) {
        return 0;
    }

    if (LowerLimit_.GetRowIndex() >= Misc_.row_count()) {
        LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: {%v}, RowCount: %v)",
            LowerLimit_,
            Misc_.row_count());
        return blockMeta.blocks_size();
    }

    const auto& blockMetaEntries = blockMeta.blocks();

    typedef decltype(blockMetaEntries.end()) TIter;
    auto rbegin = std::reverse_iterator<TIter>(blockMetaEntries.end() - 1);
    auto rend = std::reverse_iterator<TIter>(blockMetaEntries.begin());
    auto it = std::upper_bound(
        rbegin,
        rend,
        LowerLimit_.GetRowIndex(),
        [] (int index, const TBlockMeta& blockMeta) {
            // Global (chunk-wide) index of last row in block.
            auto maxRowIndex = blockMeta.chunk_row_count() - 1;
            return index > maxRowIndex;
        });

    return (it != rend) ? std::distance(it, rend) : 0;   
}

int TChunkReaderBase::GetBlockIndexByKey(const TKey& pivotKey, const std::vector<TOwningKey>& blockIndexKeys, int beginBlockIndex)
{
    YCHECK(!blockIndexKeys.empty());
    YCHECK(beginBlockIndex < blockIndexKeys.size());
    const auto& maxKey = blockIndexKeys.back();
    if (pivotKey > maxKey.Get()) {
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
            return pivot > key.Get();
        });

    return beginBlockIndex + (it != rend) ? std::distance(it, rend) : 0;
}

int TChunkReaderBase::ApplyLowerKeyLimit(const std::vector<TOwningKey>& blockIndexKeys) const
{
    if (!LowerLimit_.HasKey()) {
        return 0;
    }

    int blockIndex = GetBlockIndexByKey(LowerLimit_.GetKey().Get(), blockIndexKeys);
    if (blockIndex == blockIndexKeys.size()) {
        LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: {%v}, MaxKey: {%v})",
            LowerLimit_,
            blockIndexKeys.back());
    }
    return blockIndex;
}

int TChunkReaderBase::ApplyLowerKeyLimit(const TBlockMetaExt& blockMeta) const
{
    if (!LowerLimit_.HasKey()) {
        return 0;
    }

    const auto& blockMetaEntries = blockMeta.blocks();
    auto& lastBlock = *(--blockMetaEntries.end());
    auto maxKey = FromProto<TOwningKey>(lastBlock.last_key());
    if (LowerLimit_.GetKey() > maxKey) {
        LOG_DEBUG("Lower limit oversteps chunk boundaries (LowerLimit: {%v}, MaxKey: {%v})",
            LowerLimit_,
            maxKey);
        return blockMetaEntries.size();
    }

    typedef decltype(blockMetaEntries.end()) TIter;
    auto rbegin = std::reverse_iterator<TIter>(blockMetaEntries.end() - 1);
    auto rend = std::reverse_iterator<TIter>(blockMetaEntries.begin());
    auto it = std::upper_bound(
        rbegin,
        rend,
        LowerLimit_.GetKey(),
        [] (const TOwningKey& pivot, const TBlockMeta& block) {
            YCHECK(block.has_last_key());
            return pivot > FromProto<TOwningKey>(block.last_key());
        });

    return (it != rend) ? std::distance(it, rend) : 0;
}

int TChunkReaderBase::ApplyUpperRowLimit(const TBlockMetaExt& blockMeta) const
{
    if (!UpperLimit_.HasRowIndex()) {
        return blockMeta.blocks_size();
    }

    auto begin = blockMeta.blocks().begin();
    auto end = blockMeta.blocks().end() - 1;
    auto it = std::lower_bound(
        begin,
        end,
        UpperLimit_.GetRowIndex(),
        [] (const TBlockMeta& blockMeta, int index) {
            auto maxRowIndex = blockMeta.chunk_row_count() - 1;
            return maxRowIndex < index;
        });

    return  (it != end) ? std::distance(begin, it) + 1 : blockMeta.blocks_size();
}

int TChunkReaderBase::ApplyUpperKeyLimit(const std::vector<TOwningKey>& blockIndexKeys) const
{
    YCHECK(!blockIndexKeys.empty());
    if (!UpperLimit_.HasKey()) {
        return blockIndexKeys.size();
    }

    auto begin = blockIndexKeys.begin();
    auto end = blockIndexKeys.end() - 1;
    auto it = std::lower_bound(
        begin,
        end,
        UpperLimit_.GetKey(),
        [] (const TOwningKey& key, const TOwningKey& pivot) {
            return key < pivot;
        });

    return  (it != end) ? std::distance(begin, it) + 1 : blockIndexKeys.size();
}

int TChunkReaderBase::ApplyUpperKeyLimit(const TBlockMetaExt& blockMeta) const
{
    if (!UpperLimit_.HasKey()) {
        return blockMeta.blocks_size();
    }

    auto begin = blockMeta.blocks().begin();
    auto end = blockMeta.blocks().end() - 1;
    auto it = std::lower_bound(
        begin,
        end,
        UpperLimit_.GetKey(),
        [] (const TBlockMeta& block, const TOwningKey& pivot) {
            return FromProto<TOwningKey>(block.last_key()) < pivot;
        });

    return (it != end) ? std::distance(begin, it) + 1 : blockMeta.blocks_size();
}

TDataStatistics TChunkReaderBase::GetDataStatistics() const
{
    if (!SequentialReader_) {
        return ZeroDataStatistics();
    }
    TDataStatistics dataStatistics;
    dataStatistics.set_chunk_count(1);
    dataStatistics.set_uncompressed_data_size(SequentialReader_->GetUncompressedDataSize());
    dataStatistics.set_compressed_data_size(SequentialReader_->GetCompressedDataSize());
    return dataStatistics;
}

TFuture<void> TChunkReaderBase::GetFetchingCompletedEvent()
{
    if (!SequentialReader_) {
        return VoidFuture;
    }
    return SequentialReader_->GetFetchingCompletedEvent();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
