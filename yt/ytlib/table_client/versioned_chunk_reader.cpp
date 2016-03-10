#include "versioned_chunk_reader.h"
#include "private.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "config.h"
#include "schema.h"
#include "unversioned_row.h"
#include "versioned_block_reader.h"
#include "versioned_reader.h"

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/block_id.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/sequential_reader.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <yt/core/compression/codec.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TColumnIdMapping> BuildSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    if (columnFilter.All) {
        return chunkMeta->SchemaIdMapping();
    }

    std::vector<TColumnIdMapping> schemaIdMapping;
    schemaIdMapping.reserve(chunkMeta->SchemaIdMapping().size());
    for (auto index : columnFilter.Indexes) {
        if (index < chunkMeta->GetKeyColumnCount()) {
            continue;
        }

        for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
            if (mapping.ReaderSchemaIndex == index) {
                schemaIdMapping.push_back(mapping);
                break;
            }
        }
    }

    return schemaIdMapping;
}

template <template <class TBlockReader> class TImpl, class... Ts>
IVersionedReaderPtr CreateReaderForFormat(ETableChunkFormat format, Ts&&... args)
{
    switch (format) {
        case ETableChunkFormat::VersionedSimple:
            return New<TImpl<TSimpleVersionedBlockReader>>(std::forward<Ts>(args)...);

        default:
            YUNREACHABLE();
    }
}

} // namespace

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
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        TKeyComparer keyComparer = [] (TKey lhs, TKey rhs) {
            return CompareRows(lhs, rhs);
        });


    virtual TFuture<void> Open() override;

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const TTimestamp Timestamp_;
    const TKeyComparer KeyComparer_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

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
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp,
    TKeyComparer keyComparer)
    : TChunkReaderBase(
        std::move(config),
        std::move(underlyingReader),
        std::move(blockCache))
    , ChunkMeta_(std::move(chunkMeta))
    , Timestamp_(timestamp)
    , KeyComparer_(std::move(keyComparer))
    , SchemaIdMapping_(BuildSchemaIdMapping(columnFilter, ChunkMeta_))
    , MemoryPool_(TVersionedChunkReaderPoolTag())
    , PerformanceCounters_(std::move(performanceCounters))
{
    YCHECK(ChunkMeta_->Misc().sorted());
    YCHECK(EChunkType(ChunkMeta_->ChunkMeta().type()) == EChunkType::Table);
    YCHECK(ETableChunkFormat(ChunkMeta_->ChunkMeta().version()) == ETableChunkFormat::VersionedSimple);
    YCHECK(Timestamp_ != AllCommittedTimestamp || columnFilter.All);
    YCHECK(PerformanceCounters_);
}

TFuture<void> TVersionedChunkReaderBase::Open()
{
    return GetReadyEvent();
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
    TReadLimit LowerLimit_;
    TReadLimit UpperLimit_;

    std::vector<TSequentialReader::TBlockInfo> GetBlockSequence();

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
        columnFilter,
        std::move(performanceCounters),
        timestamp)
    , LowerLimit_(std::move(lowerLimit))
    , UpperLimit_(std::move(upperLimit))
{
    ReadyEvent_ = DoOpen(GetBlockSequence(), ChunkMeta_->Misc());
}

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

        if (CheckKeyLimit_ && KeyComparer_(BlockReader_->GetKey(), UpperLimit_.GetKey()) >= 0) {
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
            break;
        }
    }

    PerformanceCounters_->StaticChunkRowReadCount += rows->size();
    return true;
}

std::vector<TSequentialReader::TBlockInfo> TVersionedRangeChunkReader::GetBlockSequence()
{
    const auto& blockMetaExt = ChunkMeta_->BlockMeta();
    const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

    CurrentBlockIndex_ = std::max(
        ApplyLowerRowLimit(blockMetaExt, LowerLimit_),
        ApplyLowerKeyLimit(blockIndexKeys, LowerLimit_));
    int endBlockIndex = std::min(
        ApplyUpperRowLimit(blockMetaExt, UpperLimit_),
        ApplyUpperKeyLimit(blockIndexKeys, UpperLimit_));

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
        ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        UpperLimit_,
        ChunkMeta_->GetKeyColumnCount());

    BlockReader_.reset(new TSimpleVersionedBlockReader(
        SequentialReader_->GetCurrentBlock(),
        ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        ChunkMeta_->ChunkSchema(),
        ChunkMeta_->GetChunkKeyColumnCount(),
        ChunkMeta_->GetKeyColumnCount(),
        SchemaIdMapping_,
        KeyComparer_,
        Timestamp_));

    if (LowerLimit_.HasRowIndex()  && CurrentRowIndex_ < LowerLimit_.GetRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
        CurrentRowIndex_ = LowerLimit_.GetRowIndex();
    }

    if (LowerLimit_.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey()));
        CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void TVersionedRangeChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;

    CheckBlockUpperLimits(
        ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        UpperLimit_,
        ChunkMeta_->GetKeyColumnCount());

    BlockReader_.reset(new TSimpleVersionedBlockReader(
        SequentialReader_->GetCurrentBlock(),
        ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        ChunkMeta_->ChunkSchema(),
        ChunkMeta_->GetChunkKeyColumnCount(),
        ChunkMeta_->GetKeyColumnCount(),
        SchemaIdMapping_,
        KeyComparer_,
        Timestamp_));
}

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
    // TODO(babenko): consider using CraeteReaderForFormat
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
        TKeyComparer keyComparer,
        TTimestamp timestamp);

    virtual bool Read(std::vector<TVersionedRow>* rows) override;

private:
    const TSharedRange<TKey> Keys_;
    std::vector<bool> KeyFilterTest_;
    std::vector<int> BlockIndexes_;

    int CurrentBlockIndex_ = -1;

    std::vector<TSequentialReader::TBlockInfo> GetBlockSequence();

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
    TKeyComparer keyComparer,
    TTimestamp timestamp)
    : TVersionedChunkReaderBase(
        std::move(config),
        std::move(chunkMeta),
        std::move(underlyingReader),
        std::move(blockCache),
        columnFilter,
        std::move(performanceCounters),
        timestamp,
        std::move(keyComparer))
    , Keys_(keys)
    , KeyFilterTest_(Keys_.Size(), true)
{ 
    ReadyEvent_ = DoOpen(GetBlockSequence(), ChunkMeta_->Misc());
}

std::vector<TSequentialReader::TBlockInfo> TVersionedLookupChunkReader::GetBlockSequence()
{
    const auto& blockMetaExt = ChunkMeta_->BlockMeta();
    const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

    std::vector<TSequentialReader::TBlockInfo> blocks;
    if (Keys_.Empty()) {
        return blocks;
    }

    for (int keyIndex = 0; keyIndex < Keys_.Size(); ++keyIndex) {
        auto& key = Keys_[keyIndex];
#if 0
        //FIXME(savrus): use bloom filter here.
        if (!ChunkMeta_->KeyFilter().Contains(key)) {
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
        ChunkMeta_->BlockMeta().blocks(chunkBlockIndex),
        ChunkMeta_->ChunkSchema(),
        ChunkMeta_->GetChunkKeyColumnCount(),
        ChunkMeta_->GetKeyColumnCount(),
        SchemaIdMapping_,
        KeyComparer_,
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

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
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
        std::move(keyComparer),
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

// We put 16-bit block index and 32-bit row index into 48-bit value entry in LinearProbeHashTable.

static constexpr i64 MaxBlockIndex = std::numeric_limits<ui16>::max();

TVersionedChunkLookupHashTable::TVersionedChunkLookupHashTable(size_t size)
    : HashTable_(size)
{ }

void TVersionedChunkLookupHashTable::Insert(TKey key, std::pair<ui16, ui32> index)
{
    YCHECK(HashTable_.Insert(GetFarmFingerprint(key), (static_cast<ui64>(index.first) << 32) | index.second));
}

SmallVector<std::pair<ui16, ui32>, 1> TVersionedChunkLookupHashTable::Find(TKey key) const
{
    SmallVector<std::pair<ui16, ui32>, 1> result;
    SmallVector<ui64, 1> items;
    HashTable_.Find(GetFarmFingerprint(key), &items);
    for (const auto& value : items) {
        result.emplace_back(value >> 32, static_cast<ui32>(value));
    }
    return result;
}

size_t TVersionedChunkLookupHashTable::GetByteSize() const
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
        YUNREACHABLE();
    }

    virtual TSharedRef Find(
        const TBlockId& id,
        EBlockType type) override
    {
        YASSERT(type == EBlockType::UncompressedData);
        YASSERT(id.BlockIndex >= 0 && id.BlockIndex < Blocks_.size());
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

TVersionedChunkLookupHashTablePtr CreateChunkLookupHashTable(
    const std::vector<TSharedRef>& blocks,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TKeyComparer keyComparer)
{
    auto blockCache = New<TSimpleBlockCache>(blocks);

    if (chunkMeta->BlockMeta().blocks_size() > MaxBlockIndex) {
        LOG_INFO("Cannot create lookup hash table because chunk has too many blocks (ChunkId: %v, BlockCount: %v)",
            chunkMeta->GetChunkId(),
            chunkMeta->BlockMeta().blocks_size());
        return nullptr;
    }

    auto chunkSize = chunkMeta->BlockMeta().blocks(chunkMeta->BlockMeta().blocks_size() - 1).chunk_row_count();

    auto hashTable = New<TVersionedChunkLookupHashTable>(chunkSize);

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

        TSimpleVersionedBlockReader blockReader(
            uncompressedBlock,
            blockMeta,
            chunkMeta->ChunkSchema(),
            chunkMeta->GetChunkKeyColumnCount(),
            chunkMeta->GetKeyColumnCount(),
            BuildSchemaIdMapping(TColumnFilter(), chunkMeta),
            keyComparer,
            AllCommittedTimestamp);

        // Verify that row index fits into 32 bits.
        YCHECK(sizeof(blockMeta.row_count()) <= sizeof(ui32));

        for (int index = 0; index < blockMeta.row_count(); ++index) {
            auto key = blockReader.GetKey();
            hashTable->Insert(key, std::make_pair<ui16, ui32>(blockIndex, index));
            blockReader.NextRow();
        }
    }

    return hashTable;
}

////////////////////////////////////////////////////////////////////////////////

struct TCacheBasedVersionedChunkReaderPoolTag
{ };

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
        , SchemaIdMapping_(BuildSchemaIdMapping(columnFilter, ChunkMeta_))
        , MemoryPool_(TCacheBasedVersionedChunkReaderPoolTag())
    { }

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
        YUNREACHABLE();
    }

    virtual bool IsFetchingCompleted() const override
    {
        YUNREACHABLE();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YUNREACHABLE();
    }

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const IBlockCachePtr BlockCache_;
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;
    const TTimestamp Timestamp_;
    const TKeyComparer KeyComparer_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

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
        YCHECK(blockIndex >= LastUncompressedBlockIndex_);

        if (LastUncompressedBlockIndex_ != blockIndex) {
            auto uncompressedBlock = GetUncompressedBlockFromCache(blockIndex);
            // Retain a reference to prevent uncompressed block from being evicted.
            // This may happen, for example, if the table is compressed.
            RetainedUncompressedBlocks_.push_back(uncompressedBlock);
            LastUncompressedBlockIndex_ = blockIndex;
        }

        return RetainedUncompressedBlocks_.back();
    }

    template <class TBlockReader>
    TVersionedRow CaptureRow(TBlockReader* blockReader)
    {
        return blockReader->GetRow(&MemoryPool_);
    }

private:
    bool Finished_ = false;

    //! Holds uncompressed blocks for the returned rows (for string references).
    //! In compressed mode, also serves as a per-request cache of uncompressed blocks.
    SmallVector<TSharedRef, 2> RetainedUncompressedBlocks_;
    int LastUncompressedBlockIndex_ = -1;

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
            return codec->Decompress(compressedBlock);
        }

        LOG_FATAL("Cached block is missing (BlockId: %v)", blockId);
        YUNREACHABLE();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TCacheBasedVersionedLookupChunkReader
    : public TCacheBasedVersionedChunkReaderBase
{
public:
    TCacheBasedVersionedLookupChunkReader(
        TCachedVersionedChunkMetaPtr chunkMeta,
        IBlockCachePtr blockCache,
        TVersionedChunkLookupHashTablePtr lookupHashTable,
        const TSharedRange<TKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TKeyComparer keyComparer,
        TTimestamp timestamp)
        : TCacheBasedVersionedChunkReaderBase(
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
    const TVersionedChunkLookupHashTablePtr LookupHashTable_;
    const TSharedRange<TKey> Keys_;

    int KeyIndex_ = 0;


    virtual bool DoRead(std::vector<TVersionedRow>* rows) override
    {
        int count = 0;

        while (KeyIndex_ < Keys_.Size() && rows->size() < rows->capacity()) {
            ++count;
            rows->push_back(Lookup(Keys_[KeyIndex_++]));
        }

        PerformanceCounters_->StaticChunkRowLookupCount += count;

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
            const auto& uncompressedBlock = GetUncompressedBlock(index.first);
            const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(index.first);

            TBlockReader blockReader(
                uncompressedBlock,
                blockMeta,
                ChunkMeta_->ChunkSchema(),
                ChunkMeta_->GetChunkKeyColumnCount(),
                ChunkMeta_->GetKeyColumnCount(),
                SchemaIdMapping_,
                KeyComparer_,
                Timestamp_,
                false);

            YCHECK(blockReader.SkipToRowIndex(index.second));

            if (KeyComparer_(blockReader.GetKey(), key) == 0) {
                return CaptureRow(&blockReader);
            }
        }

        return TVersionedRow();
    }

    TVersionedRow LookupWithoutHashTable(TKey key)
    {
        // FIXME(savrus): Use bloom filter here.
        if (KeyComparer_(key, ChunkMeta_->MinKey()) < 0 || KeyComparer_(key, ChunkMeta_->MaxKey()) > 0) {
            return TVersionedRow();
        }

        int blockIndex = GetBlockIndex(key);
        const auto& uncompressedBlock = GetUncompressedBlock(blockIndex);
        const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(blockIndex);

        TBlockReader blockReader(
            uncompressedBlock,
            blockMeta,
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_);

        if (!blockReader.SkipToKey(key) || KeyComparer_(blockReader.GetKey(), key) != 0) {
            ++PerformanceCounters_->StaticChunkRowLookupFalsePositiveCount;
            return TVersionedRow();
        }

        return CaptureRow(&blockReader);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TVersionedChunkLookupHashTablePtr lookupHashTable,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
    TTimestamp timestamp)
{
    return CreateReaderForFormat<TCacheBasedVersionedLookupChunkReader>(
        ETableChunkFormat(chunkMeta->ChunkMeta().version()),
        std::move(chunkMeta),
        std::move(blockCache),
        std::move(lookupHashTable),
        keys,
        columnFilter,
        std::move(performanceCounters),
        std::move(keyComparer),
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TCacheBasedVersionedRangeChunkReader
    : public TCacheBasedVersionedChunkReaderBase
{
public:
    TCacheBasedVersionedRangeChunkReader(
        TCachedVersionedChunkMetaPtr chunkMeta,
        IBlockCachePtr blockCache,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp)
        : TCacheBasedVersionedChunkReaderBase(
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
            if (LowerBound_ > ChunkMeta_->MaxKey()) {
                return false;
            }

            BlockIndex_ = GetBlockIndex(LowerBound_);
            CreateBlockReader();

            if (!BlockReader_->SkipToKey(LowerBound_)) {
                return false;
            }
        }

        while ((!UpperBoundCheckNeeded_ || BlockReader_->GetKey() < UpperBound_) &&
               rows->size() < rows->capacity())
        {
            auto row = CaptureRow(BlockReader_.get());
            if (row) {
                rows->push_back(row);
            }

            if (!BlockReader_->NextRow()) {
                // End-of-block.
                if (++BlockIndex_ >= ChunkMeta_->BlockMeta().blocks_size()) {
                    // End-of-chunk.
                    return false;
                }
                CreateBlockReader();
            }
        }

        PerformanceCounters_->StaticChunkRowReadCount += rows->size();

        return true;
    }

    void CreateBlockReader()
    {
        const auto& uncompressedBlock = GetUncompressedBlock(BlockIndex_);
        const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(BlockIndex_);

        BlockReader_ = std::make_unique<TBlockReader>(
            uncompressedBlock,
            blockMeta,
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_);
        UpperBoundCheckNeeded_ = (UpperBound_ <= ChunkMeta_->BlockLastKeys()[BlockIndex_]);
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
    return CreateReaderForFormat<TCacheBasedVersionedRangeChunkReader>(
        ETableChunkFormat(chunkMeta->ChunkMeta().version()),
        std::move(chunkMeta),
        std::move(blockCache),
        std::move(lowerBound),
        std::move(upperBound),
        columnFilter,
        std::move(performanceCounters),
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
