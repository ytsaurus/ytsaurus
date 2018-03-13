#include "cache_based_versioned_chunk_reader.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_lookup_hash_table.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "config.h"
#include "private.h"
#include "schema.h"
#include "schemaless_block_reader.h"
#include "unversioned_row.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"
#include "versioned_reader.h"

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/block_id.h>
#include <yt/ytlib/chunk_client/cache_reader.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
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
using namespace NTableClient::NProto;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

using NChunkClient::TReadLimit;
using NChunkClient::NProto::TDataStatistics;

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

class TSimpleBlockCache
    : public IBlockCache
{
public:
    explicit TSimpleBlockCache(const std::vector<TBlock>& blocks)
        : Blocks_(blocks)
    { }

    virtual void Put(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*block*/,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& /*source*/) override
    {
        Y_UNREACHABLE();
    }

    virtual TBlock Find(
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
    const std::vector<TBlock>& Blocks_;
};

////////////////////////////////////////////////////////////////////////////////

struct TCacheBasedVersionedChunkReaderPoolTag
{ };

template <class TBlockReader>
class TCacheBasedVersionedChunkReaderBase
    : public IVersionedReader
{
public:
    TCacheBasedVersionedChunkReaderBase(
        const TChunkStatePtr& state,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions)
        : ChunkState_(state)
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , SchemaIdMapping_(BuildIdMapping(columnFilter, state->ChunkMeta))
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

    virtual TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics dataStatistics;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return DecompressionStatistics;
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
    const TChunkStatePtr ChunkState_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TCodecStatistics DecompressionStatistics;

    //! Returns |false| on EOF.
    virtual bool DoRead(std::vector<TVersionedRow>* rows) = 0;

    int GetBlockIndex(TKey key)
    {
        const auto& blockIndexKeys = ChunkState_->ChunkMeta->BlockLastKeys();

        typedef decltype(blockIndexKeys.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockIndexKeys.end());
        auto rend = std::reverse_iterator<TIter>(blockIndexKeys.begin());
        auto it = std::upper_bound(
            rbegin,
            rend,
            key,
            [&] (TKey pivot, TKey indexKey) {
                return ChunkState_->KeyComparer(pivot, indexKey) > 0;
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
        const auto& chunkMeta = ChunkState_->ChunkMeta;
        const auto& blockCache = ChunkState_->BlockCache;

        TBlockId blockId(chunkMeta->GetChunkId(), blockIndex);

        auto cachedBlock = blockCache->Find(blockId, EBlockType::UncompressedData);
        if (cachedBlock) {
            return cachedBlock.Data;
        }

        auto compressedBlock = blockCache->Find(blockId, EBlockType::CompressedData);
        if (compressedBlock) {
            auto codecId = NCompression::ECodec(chunkMeta->Misc().compression_codec());
            auto* codec = NCompression::GetCodec(codecId);

            NProfiling::TCpuTimer timer;
            auto uncompressedBlock = codec->Decompress(compressedBlock.Data);
            DecompressionStatistics.Append(std::make_pair(codecId, timer.GetElapsedTime()));

            if (codecId != NCompression::ECodec::None) {
                blockCache->Put(blockId, EBlockType::UncompressedData, TBlock(uncompressedBlock), Null);
            }
            return uncompressedBlock;
        }

        LOG_FATAL("Cached block is missing (BlockId: %v)", blockId);
        Y_UNREACHABLE();
    }

    static std::vector<TColumnIdMapping> BuildIdMapping(
        const TColumnFilter& columnFilter,
        const TCachedVersionedChunkMetaPtr& chunkMeta);
};

template <> std::vector<TColumnIdMapping>
TCacheBasedVersionedChunkReaderBase<TSimpleVersionedBlockReader>::BuildIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    return BuildVersionedSimpleSchemaIdMapping(columnFilter, chunkMeta);
}

template <> std::vector<TColumnIdMapping>
TCacheBasedVersionedChunkReaderBase<THorizontalSchemalessVersionedBlockReader>::BuildIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    return BuildSchemalessHorizontalSchemaIdMapping(columnFilter, chunkMeta);
}

template <> TSimpleVersionedBlockReader
TCacheBasedVersionedChunkReaderBase<TSimpleVersionedBlockReader>::CreateBlockReader(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return TSimpleVersionedBlockReader(
        block,
        meta,
        ChunkState_->ChunkMeta->ChunkSchema(),
        ChunkState_->ChunkMeta->GetChunkKeyColumnCount(),
        ChunkState_->ChunkMeta->GetKeyColumnCount(),
        SchemaIdMapping_,
        ChunkState_->KeyComparer,
        Timestamp_,
        ProduceAllVersions_,
        initialize);
}

template <> THorizontalSchemalessVersionedBlockReader
TCacheBasedVersionedChunkReaderBase<THorizontalSchemalessVersionedBlockReader>::CreateBlockReader(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return THorizontalSchemalessVersionedBlockReader(
        block,
        meta,
        SchemaIdMapping_,
        ChunkState_->ChunkMeta->GetChunkKeyColumnCount(),
        ChunkState_->ChunkMeta->GetKeyColumnCount(),
        Timestamp_);
}

template <>
TSimpleVersionedBlockReader*
TCacheBasedVersionedChunkReaderBase<TSimpleVersionedBlockReader>::CreateBlockReaderPtr(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return new TSimpleVersionedBlockReader(
        block,
        meta,
        ChunkState_->ChunkMeta->ChunkSchema(),
        ChunkState_->ChunkMeta->GetChunkKeyColumnCount(),
        ChunkState_->ChunkMeta->GetKeyColumnCount(),
        SchemaIdMapping_,
        ChunkState_->KeyComparer,
        Timestamp_,
        ProduceAllVersions_,
        initialize);
}

template <>
THorizontalSchemalessVersionedBlockReader*
TCacheBasedVersionedChunkReaderBase<THorizontalSchemalessVersionedBlockReader>::CreateBlockReaderPtr(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    bool initialize)
{
    return new THorizontalSchemalessVersionedBlockReader(
        block,
        meta,
        SchemaIdMapping_,
        ChunkState_->ChunkMeta->GetChunkKeyColumnCount(),
        ChunkState_->ChunkMeta->GetKeyColumnCount(),
        Timestamp_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TCacheBasedSimpleVersionedLookupChunkReader
    : public TCacheBasedVersionedChunkReaderBase<TBlockReader>
{
public:
    TCacheBasedSimpleVersionedLookupChunkReader(
        const TChunkStatePtr& chunkState,
        const TSharedRange<TKey>& keys,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions)
        : TCacheBasedVersionedChunkReaderBase<TBlockReader>(
            chunkState,
            columnFilter,
            timestamp,
            produceAllVersions)
        , Keys_(keys)
    { }

private:
    const TSharedRange<TKey> Keys_;

    int KeyIndex_ = 0;


    virtual bool DoRead(std::vector<TVersionedRow>* rows) override
    {
        i64 rowCount = 0;
        i64 dataWeight = 0;

        while (KeyIndex_ < Keys_.Size() && rows->size() < rows->capacity()) {
            rows->push_back(Lookup(Keys_[KeyIndex_++]));

            ++rowCount;
            dataWeight += GetDataWeight(rows->back());
        }

        this->RowCount_ += rowCount;
        this->DataWeight_ += dataWeight;
        this->ChunkState_->PerformanceCounters->StaticChunkRowLookupCount += rowCount;
        this->ChunkState_->PerformanceCounters->StaticChunkRowLookupDataWeightCount += dataWeight;

        return KeyIndex_ < Keys_.Size();
    }

    TVersionedRow Lookup(TKey key)
    {
        if (this->ChunkState_->LookupHashTable) {
            return LookupWithHashTable(key);
        } else {
            return LookupWithoutHashTable(key);
        }
    }

    TVersionedRow LookupWithHashTable(TKey key)
    {
        auto indices = this->ChunkState_->LookupHashTable->Find(key);
        for (auto index : indices) {
            const auto& uncompressedBlock = this->GetUncompressedBlock(index.first);
            const auto& blockMeta = this->ChunkState_->ChunkMeta->BlockMeta()->blocks(index.first);

            auto blockReader = this->CreateBlockReader(
                uncompressedBlock,
                blockMeta,
                false);

            YCHECK(blockReader.SkipToRowIndex(index.second));

            if (this->ChunkState_->KeyComparer(blockReader.GetKey(), key) == 0) {
                return this->CaptureRow(&blockReader);
            }
        }

        return TVersionedRow();
    }

    TVersionedRow LookupWithoutHashTable(TKey key)
    {
        // FIXME(savrus): Use bloom filter here.
        auto cmpMinKey = this->ChunkState_->KeyComparer(key, this->ChunkState_->ChunkMeta->MinKey());
        auto cmpMaxKey = this->ChunkState_->KeyComparer(key, this->ChunkState_->ChunkMeta->MaxKey());
        if (cmpMinKey < 0 || cmpMaxKey > 0) {
            return TVersionedRow();
        }

        int blockIndex = this->GetBlockIndex(key);
        const auto& uncompressedBlock = this->GetUncompressedBlock(blockIndex);
        const auto& blockMeta = this->ChunkState_->ChunkMeta->BlockMeta()->blocks(blockIndex);

        auto blockReader = this->CreateBlockReader(
            uncompressedBlock,
            blockMeta);

        if (!blockReader.SkipToKey(key) || this->ChunkState_->KeyComparer(blockReader.GetKey(), key) != 0) {
            ++this->ChunkState_->PerformanceCounters->StaticChunkRowLookupFalsePositiveCount;
            return TVersionedRow();
        }

        return this->CaptureRow(&blockReader);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TChunkStatePtr& chunkState,
    const TReadSessionId& sessionId,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions)
{
    switch (chunkState->ChunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::SchemalessHorizontal: {
            // COMPAT(sandello): Fix me.
            if (produceAllVersions && timestamp != AllCommittedTimestamp) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with a particular timestamp");
            }
            auto chunkTimestamp = static_cast<TTimestamp>(chunkState->ChunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader(keys.Size());
            }

            YCHECK(chunkState->ChunkMeta->Schema().GetUniqueKeys());
            return New<TCacheBasedSimpleVersionedLookupChunkReader<THorizontalSchemalessVersionedBlockReader>>(
                chunkState,
                keys,
                columnFilter,
                chunkTimestamp,
                produceAllVersions);
        }

        case ETableChunkFormat::VersionedSimple:
            return New<TCacheBasedSimpleVersionedLookupChunkReader<TSimpleVersionedBlockReader>>(
                chunkState,
                keys,
                columnFilter,
                timestamp,
                produceAllVersions);

        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::VersionedColumnar: {
            // COMPAT(sandello): Fix me.
            if (produceAllVersions && timestamp != AllCommittedTimestamp) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with a particular timestamp");
            }
            auto underlyingReader = CreateCacheReader(
                chunkState->ChunkMeta->GetChunkId(),
                chunkState->BlockCache);
            return CreateVersionedChunkReader(
                New<TChunkReaderConfig>(),
                std::move(underlyingReader),
                chunkState,
                sessionId,
                keys,
                columnFilter,
                timestamp,
                produceAllVersions);
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
        const TChunkStatePtr& chunkState,
        TSharedRange<TRowRange> ranges,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions)
        : TCacheBasedVersionedChunkReaderBase<TBlockReader>(
            chunkState,
            columnFilter,
            timestamp,
            produceAllVersions)
        , Ranges_(std::move(ranges))
    { }

private:
    TKey LowerBound_;
    TKey UpperBound_;

    TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;

    bool UpdateLimits()
    {
        if (RangeIndex_ >= Ranges_.Size()) {
            return false;
        }

        LowerBound_ = Ranges_[RangeIndex_].first;
        UpperBound_ = Ranges_[RangeIndex_].second;

        ++RangeIndex_;

        // First read, not initialized yet.
        if (LowerBound_ > this->ChunkState_->ChunkMeta->MaxKey()) {
            return false;
        }

        auto newBlockIndex = this->GetBlockIndex(LowerBound_);
        if (newBlockIndex != BlockIndex_) {
            BlockIndex_ = newBlockIndex;
            UpdateBlockReader();
        }

        if (!BlockReader_->SkipToKey(LowerBound_)) {
            return false;
        }

        return true;
    }

    int BlockIndex_ = -1;
    std::unique_ptr<TBlockReader> BlockReader_;
    bool UpperBoundCheckNeeded_ = false;
    bool NeedLimitUpdate_ = true;

    virtual bool DoRead(std::vector<TVersionedRow>* rows) override
    {
        if (NeedLimitUpdate_) {
            if (UpdateLimits()) {
                NeedLimitUpdate_ = false;
            } else {
                return false;
            }
        }

        i64 rowCount = 0;
        i64 dataWeight = 0;

        while (rows->size() < rows->capacity()) {
            if (UpperBoundCheckNeeded_ && BlockReader_->GetKey() >= UpperBound_) {
                NeedLimitUpdate_ = true;
                break;
            }

            auto row = this->CaptureRow(BlockReader_.get());
            if (row) {
                rows->push_back(row);

                ++rowCount;
                dataWeight += GetDataWeight(row);
            }

            if (!BlockReader_->NextRow()) {
                // End-of-block.
                if (++BlockIndex_ >= this->ChunkState_->ChunkMeta->BlockMeta()->blocks_size()) {
                    // End-of-chunk.
                    NeedLimitUpdate_ = true;
                    break;
                }
                UpdateBlockReader();
            }
        }

        this->RowCount_ += rowCount;
        this->DataWeight_ += dataWeight;
        this->ChunkState_->PerformanceCounters->StaticChunkRowReadCount += rowCount;
        this->ChunkState_->PerformanceCounters->StaticChunkRowReadDataWeightCount += dataWeight;

        return true;
    }

    void UpdateBlockReader()
    {
        const auto& uncompressedBlock = this->GetUncompressedBlock(BlockIndex_);
        const auto& blockMeta = this->ChunkState_->ChunkMeta->BlockMeta()->blocks(BlockIndex_);

        BlockReader_.reset(this->CreateBlockReaderPtr(
            uncompressedBlock,
            blockMeta));
        this->UpperBoundCheckNeeded_ = (this->UpperBound_ <= this->ChunkState_->ChunkMeta->BlockLastKeys()[BlockIndex_]);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TChunkStatePtr& chunkState,
    const TReadSessionId& sessionId,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions)
{

    switch (chunkState->ChunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::SchemalessHorizontal: {
            // COMPAT(sandello): Fix me.
            if (produceAllVersions && timestamp != AllCommittedTimestamp) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with a particular timestamp");
            }
            auto chunkTimestamp = static_cast<TTimestamp>(chunkState->ChunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader();
            }
            return New<TSimpleCacheBasedVersionedRangeChunkReader<THorizontalSchemalessVersionedBlockReader>>(
                chunkState,
                std::move(ranges),
                columnFilter,
                chunkTimestamp,
                produceAllVersions);
        }

        case ETableChunkFormat::VersionedSimple:
            return New<TSimpleCacheBasedVersionedRangeChunkReader<TSimpleVersionedBlockReader>>(
                chunkState,
                std::move(ranges),
                columnFilter,
                timestamp,
                produceAllVersions);

        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::VersionedColumnar: {
            // COMPAT(sandello): Fix me.
            if (produceAllVersions && timestamp != AllCommittedTimestamp) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with a particular timestamp");
            }
            auto underlyingReader = CreateCacheReader(
                chunkState->ChunkMeta->GetChunkId(),
                chunkState->BlockCache);

            return CreateVersionedChunkReader(
                New<TChunkReaderConfig>(),
                std::move(underlyingReader),
                chunkState,
                sessionId,
                std::move(ranges),
                columnFilter,
                timestamp,
                produceAllVersions);
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
