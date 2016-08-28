#include "cache_based_versioned_chunk_reader.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "config.h"
#include "private.h"
#include "schema.h"
#include "schemaless_block_reader.h"
#include "unversioned_row.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"
#include "versioned_chunk_reader.h"
#include "versioned_reader.h"

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/block_id.h>
#include <yt/ytlib/chunk_client/cache_reader.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/timestamp_reader.h>
#include <yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/compression/codec.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkLookupHashTable
    : public IChunkLookupHashTable
{
public:
    explicit TChunkLookupHashTable(size_t size);
    virtual void Insert(TKey key, std::pair<ui16, ui32> index) override;
    virtual SmallVector<std::pair<ui16, ui32>, 1> Find(TKey key) const override;
    virtual size_t GetByteSize() const override;

private:
    TLinearProbeHashTable HashTable_;
};

////////////////////////////////////////////////////////////////////////////////

// We put 16-bit block index and 32-bit row index into 48-bit value entry in LinearProbeHashTable.

static constexpr i64 MaxBlockIndex = std::numeric_limits<ui16>::max();

TChunkLookupHashTable::TChunkLookupHashTable(size_t size)
    : HashTable_(size)
{ }

void TChunkLookupHashTable::Insert(TKey key, std::pair<ui16, ui32> index)
{
    YCHECK(HashTable_.Insert(GetFarmFingerprint(key), (static_cast<ui64>(index.first) << 32) | index.second));
}

SmallVector<std::pair<ui16, ui32>, 1> TChunkLookupHashTable::Find(TKey key) const
{
    SmallVector<std::pair<ui16, ui32>, 1> result;
    SmallVector<ui64, 1> items;
    HashTable_.Find(GetFarmFingerprint(key), &items);
    for (const auto& value : items) {
        result.emplace_back(value >> 32, static_cast<ui32>(value));
    }
    return result;
}

size_t TChunkLookupHashTable::GetByteSize() const
{
    return HashTable_.GetByteSize();
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleBlockCache
    : public IBlockCache
{
public:
    explicit TSimpleBlockCache(const std::vector<TSharedRef>& blocks)
        : Blocks_(blocks)
    { }

    virtual void Put(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TSharedRef& /*block*/,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& /*source*/) override
    {
        Y_UNREACHABLE();
    }

    virtual TSharedRef Find(
        const TBlockId& id,
        EBlockType type) override
    {
        Y_ASSERT(type == EBlockType::UncompressedData);
        Y_ASSERT(id.BlockIndex >= 0 && id.BlockIndex < Blocks_.size());
        return Blocks_[id.BlockIndex];
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::UncompressedData;
    }

private:
    const std::vector<TSharedRef>& Blocks_;
};

////////////////////////////////////////////////////////////////////////////////

IChunkLookupHashTablePtr CreateChunkLookupHashTable(
    const std::vector<TSharedRef>& blocks,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TKeyComparer keyComparer)
{
    if (chunkMeta->GetChunkFormat() != ETableChunkFormat::VersionedSimple &&
        chunkMeta->GetChunkFormat() != ETableChunkFormat::SchemalessHorizontal)
    {
        LOG_INFO("Cannot create lookup hash table for %Qlv chunk format (ChunkId: %v)",
            chunkMeta->GetChunkId(),
            chunkMeta->GetChunkFormat());
        return nullptr;
    }

    if (chunkMeta->BlockMeta().blocks_size() > MaxBlockIndex) {
        LOG_INFO("Cannot create lookup hash table because chunk has too many blocks (ChunkId: %v, BlockCount: %v)",
            chunkMeta->GetChunkId(),
            chunkMeta->BlockMeta().blocks_size());
        return nullptr;
    }

    auto blockCache = New<TSimpleBlockCache>(blocks);
    auto chunkSize = chunkMeta->BlockMeta().blocks(chunkMeta->BlockMeta().blocks_size() - 1).chunk_row_count();

    auto hashTable = New<TChunkLookupHashTable>(chunkSize);

    for (int blockIndex = 0; blockIndex < chunkMeta->BlockMeta().blocks_size(); ++blockIndex) {
        const auto& blockMeta = chunkMeta->BlockMeta().blocks(blockIndex);

        auto blockId = TBlockId(chunkMeta->GetChunkId(), blockIndex);
        auto uncompressedBlock = blockCache->Find(blockId, EBlockType::UncompressedData);
        if (!uncompressedBlock) {
            LOG_INFO("Cannot create lookup hash table because chunk data is missing in the cache (ChunkId: %v, BlockIndex: %v)",
                chunkMeta->GetChunkId(),
                blockIndex);
            return nullptr;
        }

        std::unique_ptr<IVersionedBlockReader> blockReader;

        switch(chunkMeta->GetChunkFormat()) {
            case ETableChunkFormat::VersionedSimple:
                blockReader = std::make_unique<TSimpleVersionedBlockReader>(
                    uncompressedBlock,
                    blockMeta,
                    chunkMeta->ChunkSchema(),
                    chunkMeta->GetChunkKeyColumnCount(),
                    chunkMeta->GetKeyColumnCount(),
                    BuildVersionedSimpleSchemaIdMapping(TColumnFilter(), chunkMeta),
                    keyComparer,
                    AllCommittedTimestamp);
                break;

            case ETableChunkFormat::SchemalessHorizontal:
                blockReader = std::make_unique<THorizontalSchemalessVersionedBlockReader>(
                    uncompressedBlock,
                    blockMeta,
                    BuildSchemalessHorizontalSchemaIdMapping(TColumnFilter(), chunkMeta),
                    chunkMeta->GetChunkKeyColumnCount(),
                    chunkMeta->GetKeyColumnCount());
                break;

            default:
                Y_UNREACHABLE();
        }

        // Verify that row index fits into 32 bits.
        YCHECK(sizeof(blockMeta.row_count()) <= sizeof(ui32));

        for (int index = 0; index < blockMeta.row_count(); ++index) {
            auto key = blockReader->GetKey();
            hashTable->Insert(key, std::make_pair<ui16, ui32>(blockIndex, index));
            blockReader->NextRow();
        }
    }

    return hashTable;
}

////////////////////////////////////////////////////////////////////////////////

struct TCacheBasedVersionedChunkReaderPoolTag
{ };

template <class TBlockReader>
class TCacheBasedVersionedChunkReaderBase
    : public IVersionedReader
{
public:
    TCacheBasedVersionedChunkReaderBase(
        TCachedVersionedChunkMetaPtr chunkMeta,
        IBlockCachePtr blockCache,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        TKeyComparer keyComparer = [] (TKey lhs, TKey rhs) {
            return CompareRows(lhs, rhs);
        })
        : ChunkMeta_(std::move(chunkMeta))
        , BlockCache_(std::move(blockCache))
        , PerformanceCounters_(std::move(performanceCounters))
        , Timestamp_(timestamp)
        , KeyComparer_(std::move(keyComparer))
        , MemoryPool_(TCacheBasedVersionedChunkReaderPoolTag())
    {
        InitializeIdMapping(columnFilter);
    }

    virtual TFuture<void> Open() override
    {
        return VoidFuture;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        // Drop all references except the last one, as the last surviving block
        // reader may still be alive.
        if (!RetainedUncompressedBlocks_.empty()) {
            RetainedUncompressedBlocks_.erase(
                RetainedUncompressedBlocks_.begin(),
                RetainedUncompressedBlocks_.end() - 1);
        }

        MemoryPool_.Clear();
        rows->clear();

        if (Finished_) {
            // Now we may safely drop all references to blocks.
            RetainedUncompressedBlocks_.clear();
            return false;
        }

        Finished_ = !DoRead(rows);

        return true;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        Y_UNREACHABLE();
    }

    virtual bool IsFetchingCompleted() const override
    {
        Y_UNREACHABLE();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        Y_UNREACHABLE();
    }

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const IBlockCachePtr BlockCache_;
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;
    const TTimestamp Timestamp_;
    const TKeyComparer KeyComparer_;

    std::vector<TColumnIdMapping> IdMapping_;

    //! Returns |false| on EOF.
    virtual bool DoRead(std::vector<TVersionedRow>* rows) = 0;

    int GetBlockIndex(TKey key)
    {
        const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

        typedef decltype(blockIndexKeys.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockIndexKeys.end());
        auto rend = std::reverse_iterator<TIter>(blockIndexKeys.begin());
        auto it = std::upper_bound(
            rbegin,
            rend,
            key,
            [this] (TKey pivot, const TOwningKey& indexKey) {
                return KeyComparer_(pivot, indexKey) > 0;
            });

        return it == rend ? 0 : std::distance(it, rend);
    }

    const TSharedRef& GetUncompressedBlock(int blockIndex)
    {
        // XXX(sandello): When called from |LookupWithHashTable|, we may randomly
        // jump between blocks due to hash collisions. This happens rarely, but
        // makes YCHECK below invalid.
        // YCHECK(blockIndex >= LastRetainedBlockIndex_);

        if (LastRetainedBlockIndex_ != blockIndex) {
            auto uncompressedBlock = GetUncompressedBlockFromCache(blockIndex);
            // Retain a reference to prevent uncompressed block from being evicted.
            // This may happen, for example, if the table is compressed.
            RetainedUncompressedBlocks_.push_back(std::move(uncompressedBlock));
            LastRetainedBlockIndex_ = blockIndex;
        }

        return RetainedUncompressedBlocks_.back();
    }

    TVersionedRow CaptureRow(TBlockReader* blockReader)
    {
        return blockReader->GetRow(&MemoryPool_);
    }

    TBlockReader CreateBlockReader(
        const TSharedRef& block,
        const NProto::TBlockMeta& meta,
        bool initialize = true);

    TBlockReader* CreateBlockReaderPtr(
        const TSharedRef& block,
        const NProto::TBlockMeta& meta,
        bool initialize = true);

private:
    bool Finished_ = false;

    //! Holds uncompressed blocks for the returned rows (for string references).
    //! In compressed mode, also serves as a per-request cache of uncompressed blocks.
    SmallVector<TSharedRef, 4> RetainedUncompressedBlocks_;
    int LastRetainedBlockIndex_ = -1;

    //! Holds row values for the returned rows.
    TChunkedMemoryPool MemoryPool_;

    TSharedRef GetUncompressedBlockFromCache(int blockIndex)
    {
        TBlockId blockId(ChunkMeta_->GetChunkId(), blockIndex);

        auto uncompressedBlock = BlockCache_->Find(blockId, EBlockType::UncompressedData);
        if (uncompressedBlock) {
            return uncompressedBlock;
        }

        auto compressedBlock = BlockCache_->Find(blockId, EBlockType::CompressedData);
        if (compressedBlock) {
            auto codecId = NCompression::ECodec(ChunkMeta_->Misc().compression_codec());
            auto* codec = NCompression::GetCodec(codecId);

            auto uncompressedBlock = codec->Decompress(compressedBlock);
            if (codecId != NCompression::ECodec::None) {
                BlockCache_->Put(blockId, EBlockType::UncompressedData, uncompressedBlock, Null);
            }
            return uncompressedBlock;
        }

        LOG_FATAL("Cached block is missing (BlockId: %v)", blockId);
    }

    void InitializeIdMapping(const TColumnFilter& columnFilter);
};

template<> void TCacheBasedVersionedChunkReaderBase<TSimpleVersionedBlockReader>::InitializeIdMapping(
    const TColumnFilter& columnFilter)
{
    IdMapping_ = BuildVersionedSimpleSchemaIdMapping(columnFilter, ChunkMeta_);
}

template<> void TCacheBasedVersionedChunkReaderBase<THorizontalSchemalessVersionedBlockReader>::InitializeIdMapping(
    const TColumnFilter& columnFilter)
{
    IdMapping_ = BuildSchemalessHorizontalSchemaIdMapping(columnFilter, ChunkMeta_);
}

template<> TSimpleVersionedBlockReader TCacheBasedVersionedChunkReaderBase<TSimpleVersionedBlockReader>::CreateBlockReader(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return TSimpleVersionedBlockReader(
        block,
        meta,
        ChunkMeta_->ChunkSchema(),
        ChunkMeta_->GetChunkKeyColumnCount(),
        ChunkMeta_->GetKeyColumnCount(),
        IdMapping_,
        KeyComparer_,
        Timestamp_,
        initialize);
}
template<> THorizontalSchemalessVersionedBlockReader TCacheBasedVersionedChunkReaderBase<THorizontalSchemalessVersionedBlockReader>::CreateBlockReader(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return THorizontalSchemalessVersionedBlockReader(
        block,
        meta,
        IdMapping_,
        ChunkMeta_->GetChunkKeyColumnCount(),
        ChunkMeta_->GetKeyColumnCount());
}

template<> TSimpleVersionedBlockReader* TCacheBasedVersionedChunkReaderBase<TSimpleVersionedBlockReader>::CreateBlockReaderPtr(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return new TSimpleVersionedBlockReader(
        block,
        meta,
        ChunkMeta_->ChunkSchema(),
        ChunkMeta_->GetChunkKeyColumnCount(),
        ChunkMeta_->GetKeyColumnCount(),
        IdMapping_,
        KeyComparer_,
        Timestamp_,
        initialize);
}

template<> THorizontalSchemalessVersionedBlockReader* TCacheBasedVersionedChunkReaderBase<THorizontalSchemalessVersionedBlockReader>::CreateBlockReaderPtr(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return new THorizontalSchemalessVersionedBlockReader(
        block,
        meta,
        IdMapping_,
        ChunkMeta_->GetChunkKeyColumnCount(),
        ChunkMeta_->GetKeyColumnCount());
}

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TCacheBasedSimpleVersionedLookupChunkReader
    : public TCacheBasedVersionedChunkReaderBase<TBlockReader>
{
public:
    TCacheBasedSimpleVersionedLookupChunkReader(
        TCachedVersionedChunkMetaPtr chunkMeta,
        IBlockCachePtr blockCache,
        IChunkLookupHashTablePtr lookupHashTable,
        const TSharedRange<TKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TKeyComparer keyComparer,
        TTimestamp timestamp)
        : TCacheBasedVersionedChunkReaderBase<TBlockReader>(
            std::move(chunkMeta),
            std::move(blockCache),
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            std::move(keyComparer))
        , LookupHashTable_(std::move(lookupHashTable))
        , Keys_(keys)
    { }

private:
    const IChunkLookupHashTablePtr LookupHashTable_;
    const TSharedRange<TKey> Keys_;

    int KeyIndex_ = 0;


    virtual bool DoRead(std::vector<TVersionedRow>* rows) override
    {
        int count = 0;

        while (KeyIndex_ < Keys_.Size() && rows->size() < rows->capacity()) {
            ++count;
            rows->push_back(Lookup(Keys_[KeyIndex_++]));
        }

        this->PerformanceCounters_->StaticChunkRowLookupCount += count;

        return KeyIndex_ < Keys_.Size();
    }

    TVersionedRow Lookup(TKey key)
    {
        if (LookupHashTable_) {
            return LookupWithHashTable(key);
        } else {
            return LookupWithoutHashTable(key);
        }
    }

    TVersionedRow LookupWithHashTable(TKey key)
    {
        auto indices = LookupHashTable_->Find(key);
        for (auto index : indices) {
            const auto& uncompressedBlock = this->GetUncompressedBlock(index.first);
            const auto& blockMeta = this->ChunkMeta_->BlockMeta().blocks(index.first);

            auto blockReader = this->CreateBlockReader(
                uncompressedBlock,
                blockMeta,
                false);

            YCHECK(blockReader.SkipToRowIndex(index.second));

            if (this->KeyComparer_(blockReader.GetKey(), key) == 0) {
                return this->CaptureRow(&blockReader);
            }
        }

        return TVersionedRow();
    }

    TVersionedRow LookupWithoutHashTable(TKey key)
    {
        // FIXME(savrus): Use bloom filter here.
        if (this->KeyComparer_(key, this->ChunkMeta_->MinKey()) < 0 || this->KeyComparer_(key, this->ChunkMeta_->MaxKey()) > 0) {
            return TVersionedRow();
        }

        int blockIndex = this->GetBlockIndex(key);
        const auto& uncompressedBlock = this->GetUncompressedBlock(blockIndex);
        const auto& blockMeta = this->ChunkMeta_->BlockMeta().blocks(blockIndex);

        auto blockReader = this->CreateBlockReader(
            uncompressedBlock,
            blockMeta);

        if (!blockReader.SkipToKey(key) || this->KeyComparer_(blockReader.GetKey(), key) != 0) {
            ++this->PerformanceCounters_->StaticChunkRowLookupFalsePositiveCount;
            return TVersionedRow();
        }

        return this->CaptureRow(&blockReader);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkLookupHashTablePtr lookupHashTable,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
    TTimestamp timestamp)
{
    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::SchemalessHorizontal:
            YCHECK(chunkMeta->Schema().GetUniqueKeys());
            return New<TCacheBasedSimpleVersionedLookupChunkReader<THorizontalSchemalessVersionedBlockReader>>(
                std::move(chunkMeta),
                std::move(blockCache),
                std::move(lookupHashTable),
                keys,
                columnFilter,
                std::move(performanceCounters),
                std::move(keyComparer),
                timestamp);

        case ETableChunkFormat::VersionedSimple:
            return New<TCacheBasedSimpleVersionedLookupChunkReader<TSimpleVersionedBlockReader>>(
                std::move(chunkMeta),
                std::move(blockCache),
                std::move(lookupHashTable),
                keys,
                columnFilter,
                std::move(performanceCounters),
                std::move(keyComparer),
                timestamp);

        case ETableChunkFormat::VersionedColumnar: {
            auto underlyingReader = CreateCacheReader(chunkMeta->GetChunkId(), blockCache);

            return CreateVersionedChunkReader(
                New<TChunkReaderConfig>(),
                std::move(underlyingReader),
                std::move(blockCache),
                std::move(chunkMeta),
                keys,
                columnFilter,
                std::move(performanceCounters),
                std::move(keyComparer),
                timestamp);
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TSimpleCacheBasedVersionedRangeChunkReader
    : public TCacheBasedVersionedChunkReaderBase<TBlockReader>
{
public:
    TSimpleCacheBasedVersionedRangeChunkReader(
        TCachedVersionedChunkMetaPtr chunkMeta,
        IBlockCachePtr blockCache,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp)
        : TCacheBasedVersionedChunkReaderBase<TBlockReader>(
            std::move(chunkMeta),
            std::move(blockCache),
            columnFilter,
            std::move(performanceCounters),
            timestamp)
        , LowerBound_(std::move(lowerBound))
        , UpperBound_(std::move(upperBound))
    { }

private:
    const TOwningKey LowerBound_;
    const TOwningKey UpperBound_;

    int BlockIndex_ = -1;
    std::unique_ptr<TBlockReader> BlockReader_;
    bool UpperBoundCheckNeeded_ = false;

    virtual bool DoRead(std::vector<TVersionedRow>* rows) override
    {
        if (BlockIndex_ < 0) {
            // First read, not initialized yet.
            if (this->LowerBound_ > this->ChunkMeta_->MaxKey()) {
                return false;
            }

            BlockIndex_ = this->GetBlockIndex(this->LowerBound_);
            UpdateBlockReader();

            if (!BlockReader_->SkipToKey(this->LowerBound_)) {
                return false;
            }
        }

        bool finished = false;

        while (rows->size() < rows->capacity()) {
            if (this->UpperBoundCheckNeeded_ && BlockReader_->GetKey() >= this->UpperBound_.Get()) {
                finished = true;
                break;
            }

            auto row = this->CaptureRow(BlockReader_.get());
            if (row) {
                rows->push_back(row);
            }

            if (!BlockReader_->NextRow()) {
                // End-of-block.
                if (++BlockIndex_ >= this->ChunkMeta_->BlockMeta().blocks_size()) {
                    // End-of-chunk.
                    finished = true;
                    break;
                }
                UpdateBlockReader();
            }
        }

        this->PerformanceCounters_->StaticChunkRowReadCount += rows->size();

        return !finished;
    }

    void UpdateBlockReader()
    {
        const auto& uncompressedBlock = this->GetUncompressedBlock(BlockIndex_);
        const auto& blockMeta = this->ChunkMeta_->BlockMeta().blocks(BlockIndex_);

        BlockReader_.reset(this->CreateBlockReaderPtr(
            uncompressedBlock,
            blockMeta));
        this->UpperBoundCheckNeeded_ = (this->UpperBound_ <= this->ChunkMeta_->BlockLastKeys()[BlockIndex_]);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
{
    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<TSimpleCacheBasedVersionedRangeChunkReader<THorizontalSchemalessVersionedBlockReader>>(
                std::move(chunkMeta),
                std::move(blockCache),
                std::move(lowerBound),
                std::move(upperBound),
                columnFilter,
                std::move(performanceCounters),
                timestamp);

        case ETableChunkFormat::VersionedSimple:
            return New<TSimpleCacheBasedVersionedRangeChunkReader<TSimpleVersionedBlockReader>>(
                std::move(chunkMeta),
                std::move(blockCache),
                std::move(lowerBound),
                std::move(upperBound),
                columnFilter,
                std::move(performanceCounters),
                timestamp);

        case ETableChunkFormat::VersionedColumnar: {
            auto underlyingReader = CreateCacheReader(chunkMeta->GetChunkId(), blockCache);

            TReadLimit lowerLimit;
            lowerLimit.SetKey(std::move(lowerBound));

            TReadLimit upperLimit;
            upperLimit.SetKey(std::move(upperBound));

            return CreateVersionedChunkReader(
                New<TChunkReaderConfig>(),
                std::move(underlyingReader),
                std::move(blockCache),
                std::move(chunkMeta),
                std::move(lowerLimit),
                std::move(upperLimit),
                columnFilter,
                std::move(performanceCounters),
                timestamp);
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
