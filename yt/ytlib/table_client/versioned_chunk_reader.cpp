#include "stdafx.h"
#include "versioned_chunk_reader.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "config.h"
#include "schema.h"
#include "versioned_block_reader.h"
#include "versioned_reader.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/sequential_reader.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

struct TVersionedChunkReaderPoolTag { };

class TVersionedChunkReaderBase
    : public IVersionedReader
    , public TChunkReaderBase
{
public:
    TVersionedChunkReaderBase(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        TReadLimit lowerLimit,
        TReadLimit upperLimit,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp);

protected:
    const TTimestamp Timestamp_;

    TCachedVersionedChunkMetaPtr CachedChunkMeta_;
    std::vector<TColumnIdMapping> SchemaIdMapping_;

    std::unique_ptr<TSimpleVersionedBlockReader> BlockReader_;

    TChunkedMemoryPool MemoryPool_;

    i64 RowCount_ = 0;

    TChunkReaderPerformanceCountersPtr PerformanceCounters_;
};

TVersionedChunkReaderBase::TVersionedChunkReaderBase(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
    : TChunkReaderBase(
        std::move(config),
        std::move(lowerLimit),
        std::move(upperLimit),
        std::move(underlyingReader),
        chunkMeta->Misc(),
        std::move(blockCache))
    , Timestamp_(timestamp)
    , CachedChunkMeta_(std::move(chunkMeta))
    , MemoryPool_(TVersionedChunkReaderPoolTag())
    , PerformanceCounters_(std::move(performanceCounters))
{
    YCHECK(CachedChunkMeta_->Misc().sorted());
    YCHECK(EChunkType(CachedChunkMeta_->ChunkMeta().type()) == EChunkType::Table);
    YCHECK(ETableChunkFormat(CachedChunkMeta_->ChunkMeta().version()) == ETableChunkFormat::VersionedSimple);
    YCHECK(Timestamp_ != AllCommittedTimestamp || columnFilter.All);
    YCHECK(PerformanceCounters_);

    if (columnFilter.All) {
        SchemaIdMapping_ = CachedChunkMeta_->SchemaIdMapping();
    } else {
        SchemaIdMapping_.reserve(CachedChunkMeta_->SchemaIdMapping().size());
        int keyColumnCount = static_cast<int>(CachedChunkMeta_->KeyColumns().size());
        for (auto index : columnFilter.Indexes) {
            if (index < keyColumnCount) {
                continue;
            }

            for (const auto& mapping : CachedChunkMeta_->SchemaIdMapping()) {
                if (mapping.ReaderSchemaIndex == index) {
                    SchemaIdMapping_.push_back(mapping);
                    break;
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedRangeChunkReader
    : public TVersionedChunkReaderBase
{
public:
    TVersionedRangeChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        TReadLimit lowerLimit,
        TReadLimit upperLimit,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp);

    virtual bool Read(std::vector<TVersionedRow>* rows) override;

private:
    int CurrentBlockIndex_ = 0;
    i64 CurrentRowIndex_ = 0;

    virtual std::vector<TSequentialReader::TBlockInfo> GetBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;
};

////////////////////////////////////////////////////////////////////////////////

TVersionedRangeChunkReader::TVersionedRangeChunkReader(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
    : TVersionedChunkReaderBase(
        std::move(config),
        std::move(chunkMeta),
        std::move(underlyingReader),
        std::move(blockCache),
        std::move(lowerLimit),
        std::move(upperLimit),
        columnFilter,
        std::move(performanceCounters),
        timestamp)
{ }

bool TVersionedRangeChunkReader::Read(std::vector<TVersionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!BeginRead()) {
        // Not ready yet.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        return OnBlockEnded();
    }

    while (rows->size() < rows->capacity()) {
        if (CheckRowLimit_ && CurrentRowIndex_ == UpperLimit_.GetRowIndex()) {
            PerformanceCounters_->StaticChunkRowReadCount += rows->size();
            return !rows->empty();
        }

        if (CheckKeyLimit_ && CompareRows(BlockReader_->GetKey(), UpperLimit_.GetKey().Get()) >= 0) {
            PerformanceCounters_->StaticChunkRowReadCount += rows->size();
            return !rows->empty();
        }

        auto row = BlockReader_->GetRow(&MemoryPool_);
        if (row) {
            YASSERT(
                rows->empty() ||
                CompareRows(
                    rows->back().BeginKeys(), rows->back().EndKeys(),
                    row.BeginKeys(), row.EndKeys()) < 0);
            rows->push_back(row);
            ++RowCount_;
        }

        ++CurrentRowIndex_;
        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            PerformanceCounters_->StaticChunkRowReadCount += rows->size();
            return true;
        }
    }

    PerformanceCounters_->StaticChunkRowReadCount += rows->size();
    return true;
}

std::vector<TSequentialReader::TBlockInfo> TVersionedRangeChunkReader::GetBlockSequence()
{
    const auto& blockMetaExt = CachedChunkMeta_->BlockMeta();
    const auto& blockIndexKeys = CachedChunkMeta_->BlockIndexKeys();

    CurrentBlockIndex_ = std::max(ApplyLowerRowLimit(blockMetaExt), ApplyLowerKeyLimit(blockIndexKeys));
    int endBlockIndex = std::min(ApplyUpperRowLimit(blockMetaExt), ApplyUpperKeyLimit(blockIndexKeys));
    
    std::vector<TSequentialReader::TBlockInfo> blocks;
    if (CurrentBlockIndex_ >= blockMetaExt.blocks_size()) {
        return blocks;
    }

    auto& blockMeta = blockMetaExt.blocks(CurrentBlockIndex_);
    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    for (int blockIndex = CurrentBlockIndex_; blockIndex < endBlockIndex; ++blockIndex) {
        auto& blockMeta = blockMetaExt.blocks(blockIndex);
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = blockIndex;
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blocks.push_back(blockInfo);
    }

    return blocks;
}

void TVersionedRangeChunkReader::InitFirstBlock()
{
    CheckBlockUpperLimits(
        CachedChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        CachedChunkMeta_->GetKeyPadding());

    BlockReader_.reset(new TSimpleVersionedBlockReader(
        SequentialReader_->GetCurrentBlock(),
        CachedChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        CachedChunkMeta_->ChunkSchema(),
        CachedChunkMeta_->GetKeyColumnCount(),
        CachedChunkMeta_->GetKeyPadding(),
        SchemaIdMapping_,
        Timestamp_));

    if (LowerLimit_.HasRowIndex()  && CurrentRowIndex_ < LowerLimit_.GetRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
        CurrentRowIndex_ = LowerLimit_.GetRowIndex();
    }

    if (LowerLimit_.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey().Get()));
        CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void TVersionedRangeChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;

    CheckBlockUpperLimits(
        CachedChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        CachedChunkMeta_->GetKeyPadding());

    BlockReader_.reset(new TSimpleVersionedBlockReader(
        SequentialReader_->GetCurrentBlock(),
        CachedChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        CachedChunkMeta_->ChunkSchema(),
        CachedChunkMeta_->GetKeyColumnCount(),
        CachedChunkMeta_->GetKeyPadding(),
        SchemaIdMapping_,
        Timestamp_));
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
{
    auto formatVersion = ETableChunkFormat(chunkMeta->ChunkMeta().version());
    switch (formatVersion) {
        case ETableChunkFormat::VersionedSimple:
            return New<TVersionedRangeChunkReader>(
                std::move(config),
                std::move(chunkMeta),
                std::move(chunkReader),
                std::move(blockCache),
                std::move(lowerLimit),
                std::move(upperLimit),
                columnFilter,
                std::move(performanceCounters),
                timestamp);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedLookupChunkReader
    : public TVersionedChunkReaderBase
{
public:
    TVersionedLookupChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TSharedRange<TKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp);

    virtual bool Read(std::vector<TVersionedRow>* rows) override;

private:
    const TSharedRange<TKey> Keys_;
    std::vector<bool> KeyFilterTest_;
    std::vector<int> BlockIndexes_;

    int CurrentBlockIndex_ = -1;

    virtual std::vector<TSequentialReader::TBlockInfo> GetBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;
};

TVersionedLookupChunkReader::TVersionedLookupChunkReader(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
    : TVersionedChunkReaderBase(
        std::move(config),
        std::move(chunkMeta),
        std::move(underlyingReader),
        std::move(blockCache),
        TReadLimit(),
        TReadLimit(),
        columnFilter,
        std::move(performanceCounters),
        timestamp)
    , Keys_(keys)
    , KeyFilterTest_(Keys_.Size(), true)
{ }

std::vector<TSequentialReader::TBlockInfo> TVersionedLookupChunkReader::GetBlockSequence()
{
    const auto& blockMetaExt = CachedChunkMeta_->BlockMeta();
    const auto& blockIndexKeys = CachedChunkMeta_->BlockIndexKeys();

    std::vector<TSequentialReader::TBlockInfo> blocks;
    if (Keys_.Empty()) {
        return blocks;
    }

    for (int keyIndex = 0; keyIndex < Keys_.Size(); ++keyIndex) {
        auto& key = Keys_[keyIndex];
#if 0
        //FIXME use bloom filter here.
        if (!CachedChunkMeta_->KeyFilter().Contains(key)) {
            KeyFilterTest_[keyIndex] = false;
            continue;
        }
#endif
        int blockIndex = GetBlockIndexByKey(
            key, 
            blockIndexKeys, 
            BlockIndexes_.empty() ? 0 : BlockIndexes_.back());

        if (blockIndex == blockIndexKeys.size()) {
            break;
        }
        if (BlockIndexes_.empty() || BlockIndexes_.back() < blockIndex) {
            BlockIndexes_.push_back(blockIndex);
        }
        YCHECK(blockIndex == BlockIndexes_.back());
        YCHECK(blockIndex < blockIndexKeys.size());
    }

    for (int blockIndex : BlockIndexes_) {
        auto& blockMeta = blockMetaExt.blocks(blockIndex);
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = blockIndex;
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blocks.push_back(blockInfo);
    }

    return blocks;
}

void TVersionedLookupChunkReader::InitFirstBlock()
{
    InitNextBlock();
}

void TVersionedLookupChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    int chunkBlockIndex = BlockIndexes_ [CurrentBlockIndex_];
    BlockReader_.reset(new TSimpleVersionedBlockReader(
        SequentialReader_->GetCurrentBlock(),
        CachedChunkMeta_->BlockMeta().blocks(chunkBlockIndex),
        CachedChunkMeta_->ChunkSchema(),
        CachedChunkMeta_->GetKeyColumnCount(),
        CachedChunkMeta_->GetKeyPadding(),
        SchemaIdMapping_,
        Timestamp_));
}

bool TVersionedLookupChunkReader::Read(std::vector<TVersionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!BeginRead()) {
        // Not ready yet.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        if (RowCount_ == Keys_.Size()) {
            return false;
        }

        while (rows->size() < rows->capacity() && RowCount_ < Keys_.Size()) {
            rows->push_back(TVersionedRow());
            ++RowCount_;
        }
        PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
        return true;
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        OnBlockEnded();
        return true;
    }

    while (rows->size() < rows->capacity()) {
        if (RowCount_ == Keys_.Size()) {
            BlockEnded_ = true;
            PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
            return true;
        }

        if (!KeyFilterTest_[RowCount_]) {
            rows->push_back(TVersionedRow());
            ++PerformanceCounters_->StaticChunkRowLookupTrueNegativeCount;
        } else {
            const auto& key = Keys_[RowCount_];
            if (!BlockReader_->SkipToKey(key)) {
                BlockEnded_ = true;
                PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
                return true;
            }

            if (key == BlockReader_->GetKey()) {
                auto row = BlockReader_->GetRow(&MemoryPool_);
                rows->push_back(row);
            } else {
                rows->push_back(TVersionedRow());
                ++PerformanceCounters_->StaticChunkRowLookupFalsePositiveCount;
            }
        }
        ++RowCount_;
    }

    PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
    return true;
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
{
    return New<TVersionedLookupChunkReader>(
        std::move(config),
        std::move(chunkMeta),
        std::move(chunkReader),
        std::move(blockCache),
        keys,
        columnFilter,
        std::move(performanceCounters),
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
