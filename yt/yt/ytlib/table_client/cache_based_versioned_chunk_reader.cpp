#include "cache_based_versioned_chunk_reader.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_lookup_hash_table.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "config.h"
#include "private.h"
#include "schemaless_block_reader.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>
#include <yt/yt/ytlib/chunk_client/cache_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/timestamp_reader.h>
#include <yt/yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

using NChunkClient::TLegacyReadLimit;
using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkLookupHashTable
    : public IChunkLookupHashTable
{
public:
    explicit TChunkLookupHashTable(size_t size);
    virtual void Insert(TLegacyKey key, std::pair<ui16, ui32> index) override;
    virtual SmallVector<std::pair<ui16, ui32>, 1> Find(TLegacyKey key) const override;
    virtual size_t GetByteSize() const override;

private:
    TLinearProbeHashTable HashTable_;
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

    virtual TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        // Drop all references except the last one, as the last surviving block
        // reader may still be alive.
        if (!RetainedUncompressedBlocks_.empty()) {
            RetainedUncompressedBlocks_.erase(
                RetainedUncompressedBlocks_.begin(),
                RetainedUncompressedBlocks_.end() - 1);
        }

        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        MemoryPool_.Clear();

        if (Finished_) {
            // Now we may safely drop all references to blocks.
            RetainedUncompressedBlocks_.clear();
            return nullptr;
        }

        Finished_ = !DoRead(&rows);

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
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
        return DecompressionStatistics_;
    }

    virtual bool IsFetchingCompleted() const override
    {
        return false;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

protected:
    const TChunkStatePtr ChunkState_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TCodecStatistics DecompressionStatistics_;

    //! Returns |false| on EOF.
    virtual bool DoRead(std::vector<TVersionedRow>* rows) = 0;

    int GetBlockIndex(TLegacyKey key)
    {
        const auto& blockIndexKeys = ChunkState_->ChunkMeta->LegacyBlockLastKeys();

        typedef decltype(blockIndexKeys.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockIndexKeys.end());
        auto rend = std::reverse_iterator<TIter>(blockIndexKeys.begin());
        auto it = std::upper_bound(
            rbegin,
            rend,
            key,
            [&] (TLegacyKey pivot, TLegacyKey indexKey) {
                return ChunkState_->KeyComparer(pivot, indexKey) > 0;
            });

        return it == rend ? 0 : std::distance(it, rend);
    }

    const TSharedRef& GetUncompressedBlock(int blockIndex)
    {
        // XXX(sandello): When called from |LookupWithHashTable|, we may randomly
        // jump between blocks due to hash collisions. This happens rarely, but
        // makes YT_VERIFY below invalid.
        // YT_VERIFY(blockIndex >= LastRetainedBlockIndex_);

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

        auto cachedBlock = blockCache->FindBlock(blockId, EBlockType::UncompressedData).Block;
        if (cachedBlock) {
            return cachedBlock.Data;
        }

        auto compressedBlock = blockCache->FindBlock(blockId, EBlockType::CompressedData).Block;
        if (compressedBlock) {
            NCompression::ECodec codecId;
            YT_VERIFY(TryEnumCast(chunkMeta->Misc().compression_codec(), &codecId));
            auto* codec = NCompression::GetCodec(codecId);

            NProfiling::TFiberWallTimer timer;
            auto uncompressedBlock = codec->Decompress(compressedBlock.Data);
            DecompressionStatistics_.Append(TCodecDuration{codecId, timer.GetElapsedTime()});

            if (codecId != NCompression::ECodec::None) {
                blockCache->PutBlock(blockId, EBlockType::UncompressedData, TBlock(uncompressedBlock), std::nullopt);
            }
            return uncompressedBlock;
        }

        YT_LOG_FATAL("Cached block is missing (BlockId: %v)", blockId);
        YT_ABORT();
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
        ChunkState_->ChunkMeta->GetChunkSchema(),
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
        ChunkState_->ChunkMeta->GetSchema(),
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
        ChunkState_->ChunkMeta->GetChunkSchema(),
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
        ChunkState_->ChunkMeta->GetSchema(),
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
        const TSharedRange<TLegacyKey>& keys,
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
    const TSharedRange<TLegacyKey> Keys_;

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

    TVersionedRow Lookup(TLegacyKey key)
    {
        if (this->ChunkState_->LookupHashTable) {
            return LookupWithHashTable(key);
        } else {
            return LookupWithoutHashTable(key);
        }
    }

    TVersionedRow LookupWithHashTable(TLegacyKey key)
    {
        auto indices = this->ChunkState_->LookupHashTable->Find(key);
        for (auto index : indices) {
            const auto& uncompressedBlock = this->GetUncompressedBlock(index.first);
            const auto& blockMeta = this->ChunkState_->ChunkMeta->BlockMeta()->blocks(index.first);

            auto blockReader = this->CreateBlockReader(
                uncompressedBlock,
                blockMeta,
                false);

            YT_VERIFY(blockReader.SkipToRowIndex(index.second));

            if (this->ChunkState_->KeyComparer(blockReader.GetKey(), key) == 0) {
                return this->CaptureRow(&blockReader);
            }
        }

        return TVersionedRow();
    }

    TVersionedRow LookupWithoutHashTable(TLegacyKey key)
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
    const TClientBlockReadOptions& blockReadOptions,
    const TSharedRange<TLegacyKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions)
{
    auto createGenericVersionedReader = [&] {
        if (produceAllVersions && !columnFilter.IsUniversal()) {
            THROW_ERROR_EXCEPTION("Reading all value versions is not supported with non-universal column filter");
        }

        auto underlyingReader = CreateCacheReader(
            chunkState->ChunkMeta->GetChunkId(),
            chunkState->BlockCache);
        return CreateVersionedChunkReader(
            New<TChunkReaderConfig>(),
            std::move(underlyingReader),
            chunkState,
            chunkState->ChunkMeta,
            blockReadOptions,
            keys,
            columnFilter,
            timestamp,
            produceAllVersions);
    };

    if (produceAllVersions && timestamp != AllCommittedTimestamp) {
        return createGenericVersionedReader();
    }

    switch (chunkState->ChunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::SchemalessHorizontal: {
            auto chunkTimestamp = static_cast<TTimestamp>(chunkState->ChunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader(keys.Size());
            }

            YT_VERIFY(chunkState->ChunkMeta->GetSchema()->GetUniqueKeys());
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
        case ETableChunkFormat::VersionedColumnar:
            return createGenericVersionedReader();

        default:
            YT_ABORT();
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
        bool produceAllVersions,
        const TSharedRange<TRowRange>& singletonClippingRange)
        : TCacheBasedVersionedChunkReaderBase<TBlockReader>(
            chunkState,
            columnFilter,
            timestamp,
            produceAllVersions)
        , Ranges_(std::move(ranges))
        , ClippingRange_(singletonClippingRange)
    { }

private:
    TLegacyKey LowerBound_;
    TLegacyKey UpperBound_;

    TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;

    TSharedRange<TRowRange> ClippingRange_;

    bool UpdateLimits()
    {
        if (RangeIndex_ >= Ranges_.Size()) {
            return false;
        }

        LowerBound_ = Ranges_[RangeIndex_].first;
        UpperBound_ = Ranges_[RangeIndex_].second;

        if (RangeIndex_ == 0 && ClippingRange_) {
            if (auto clippingLowerBound = ClippingRange_.Front().first) {
                LowerBound_ = std::max(LowerBound_, clippingLowerBound);
            }
        }

        if (RangeIndex_ == Ranges_.Size() - 1 && ClippingRange_) {
            if (auto clippingUpperBound = ClippingRange_.Front().second) {
                UpperBound_ = std::min(UpperBound_, clippingUpperBound);
            }
        }

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
        this->UpperBoundCheckNeeded_ = (this->UpperBound_ <= this->ChunkState_->ChunkMeta->LegacyBlockLastKeys()[BlockIndex_]);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TChunkStatePtr& chunkState,
    const TClientBlockReadOptions& blockReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TSharedRange<TRowRange>& singletonClippingRange)
{
    auto createGenericVersionedReader = [&] {
        if (produceAllVersions && !columnFilter.IsUniversal()) {
            THROW_ERROR_EXCEPTION("Reading all value versions is not supported with non-universal column filter");
        }

        auto underlyingReader = CreateCacheReader(
            chunkState->ChunkMeta->GetChunkId(),
            chunkState->BlockCache);
        return CreateVersionedChunkReader(
            New<TChunkReaderConfig>(),
            std::move(underlyingReader),
            chunkState,
            chunkState->ChunkMeta,
            blockReadOptions,
            std::move(ranges),
            columnFilter,
            timestamp,
            produceAllVersions,
            singletonClippingRange);
    };

    if (produceAllVersions && timestamp != AllCommittedTimestamp) {
        return createGenericVersionedReader();
    }

    switch (chunkState->ChunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::SchemalessHorizontal: {
            auto chunkTimestamp = static_cast<TTimestamp>(chunkState->ChunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader();
            }
            return New<TSimpleCacheBasedVersionedRangeChunkReader<THorizontalSchemalessVersionedBlockReader>>(
                chunkState,
                std::move(ranges),
                columnFilter,
                chunkTimestamp,
                produceAllVersions,
                singletonClippingRange);
        }

        case ETableChunkFormat::VersionedSimple:
            return New<TSimpleCacheBasedVersionedRangeChunkReader<TSimpleVersionedBlockReader>>(
                chunkState,
                std::move(ranges),
                columnFilter,
                timestamp,
                produceAllVersions,
                singletonClippingRange);

        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::VersionedColumnar:
            return createGenericVersionedReader();

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
