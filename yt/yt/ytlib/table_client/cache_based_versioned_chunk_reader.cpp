#include "cache_based_versioned_chunk_reader.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_column_mapping.h"
#include "chunk_lookup_hash_table.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "config.h"
#include "private.h"
#include "schemaless_block_reader.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"
#include "hunks.h"

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>
#include <yt/yt/ytlib/chunk_client/cache_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/timestamp_reader.h>
#include <yt/yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>
#include <yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

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

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkLookupHashTable
    : public IChunkLookupHashTable
{
public:
    explicit TChunkLookupHashTable(size_t size);
    void Insert(TLegacyKey key, std::pair<ui16, ui32> index) override;
    TCompactVector<std::pair<ui16, ui32>, 1> Find(TLegacyKey key) const override;
    size_t GetByteSize() const override;

private:
    TLinearProbeHashTable HashTable_;
};

////////////////////////////////////////////////////////////////////////////////

struct TCacheBasedVersionedChunkReaderPoolTag
{ };

class TCacheBasedVersionedChunkReaderBase
    : public IVersionedReader
{
public:
    TCacheBasedVersionedChunkReaderBase(TChunkStatePtr state)
        : ChunkState_(std::move(state))
        , HasHunkColumns_(ChunkState_->ChunkMeta->GetChunkSchema()->HasHunkColumns())
        , MemoryPool_(TCacheBasedVersionedChunkReaderPoolTag())
    { }

    TFuture<void> Open() override
    {
        return VoidFuture;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        // Drop all references except the last one, as the last surviving block
        // reader may still be alive.
        if (!RetainedUncompressedBlocks_.empty()) {
            RetainedUncompressedBlocks_.erase(
                RetainedUncompressedBlocks_.begin(),
                RetainedUncompressedBlocks_.end() - 1);
        }

        MemoryPool_.Clear();

        if (Finished_) {
            // Now we may safely drop all references to blocks.
            RetainedUncompressedBlocks_.clear();
            return nullptr;
        }

        std::vector<TVersionedRow> rows;
        std::tie(rows, Finished_) = DoRead(options);

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return DecompressionStatistics_;
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

protected:
    const TChunkStatePtr ChunkState_;
    const bool HasHunkColumns_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TCodecStatistics DecompressionStatistics_;

    //! First is read rows, second is EOF mark.
    virtual std::tuple<std::vector<TVersionedRow>, bool> DoRead(const TRowBatchReadOptions& options) = 0;

    int GetBlockIndex(TLegacyKey key)
    {
        const auto& blockLastKeys = ChunkState_->ChunkMeta->LegacyBlockLastKeys();

        return BinarySearch(
            blockLastKeys.begin(),
            blockLastKeys.end(),
            [&] (const TLegacyKey* pivot) {
                return ChunkState_->KeyComparer(*pivot, key) < 0;
            }) - blockLastKeys.begin();

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

    template <class TBlockReader>
    TVersionedRow CaptureRow(TBlockReader* blockReader)
    {
        auto row = blockReader->GetRow(&MemoryPool_);
        if (row && HasHunkColumns_) {
            GlobalizeHunkValues(&MemoryPool_, ChunkState_->ChunkMeta, row);
        }
        return row;
    }

private:
    bool Finished_ = false;

    //! Holds uncompressed blocks for the returned rows (for string references).
    //! In compressed mode, also serves as a per-request cache of uncompressed blocks.
    TCompactVector<TSharedRef, 4> RetainedUncompressedBlocks_;
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
                blockCache->PutBlock(blockId, EBlockType::UncompressedData, TBlock(uncompressedBlock));
            }
            return uncompressedBlock;
        }

        YT_LOG_FATAL("Cached block is missing (BlockId: %v)", blockId);
        YT_ABORT();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TBlockReaderFactory;

template <>
class TBlockReaderFactory<TSimpleVersionedBlockReader>
    : public TCacheBasedVersionedChunkReaderBase
{
public:
    TBlockReaderFactory(
        TChunkStatePtr state,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions)
        : TCacheBasedVersionedChunkReaderBase(std::move(state))
        , SchemaIdMapping_(
            ChunkState_->ChunkColumnMapping->BuildVersionedSimpleSchemaIdMapping(columnFilter))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
    { }

    TSimpleVersionedBlockReader* CreateBlockReader(
        const TSharedRef& block,
        const NProto::TDataBlockMeta& meta,
        bool initialize = true)
    {
        BlockReader_.emplace(
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

        return &BlockReader_.value();
    }

protected:
    const std::vector<TColumnIdMapping> SchemaIdMapping_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;

private:
    std::optional<TSimpleVersionedBlockReader> BlockReader_;
};

template <>
class TBlockReaderFactory<THorizontalSchemalessVersionedBlockReader>
    : public TCacheBasedVersionedChunkReaderBase
{
public:
    TBlockReaderFactory(
        TChunkStatePtr state,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool /*produceAllVersions*/)
        : TCacheBasedVersionedChunkReaderBase(std::move(state))
        , ChunkToReaderIdMapping_(
            ChunkState_->ChunkColumnMapping->BuildSchemalessHorizontalSchemaIdMapping(columnFilter))
        , Timestamp_(timestamp)
    { }

    THorizontalSchemalessVersionedBlockReader* CreateBlockReader(
        const TSharedRef& block,
        const NProto::TDataBlockMeta& meta,
        bool /*initialize*/ = true)
    {
        BlockReader_.emplace(
            block,
            meta,
            GetCompositeColumnFlags(ChunkState_->ChunkMeta->GetChunkSchema()),
            ChunkToReaderIdMapping_,
            ChunkState_->ChunkMeta->GetChunkKeyColumnCount(),
            ChunkState_->ChunkMeta->GetKeyColumnCount(),
            Timestamp_);

        return &BlockReader_.value();
    }

protected:
    const std::vector<int> ChunkToReaderIdMapping_;
    const TTimestamp Timestamp_;

private:
    std::optional<THorizontalSchemalessVersionedBlockReader> BlockReader_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TCacheBasedSimpleVersionedLookupChunkReader
    : public TBlockReaderFactory<TBlockReader>
{
public:
    TCacheBasedSimpleVersionedLookupChunkReader(
        TChunkStatePtr chunkState,
        TSharedRange<TLegacyKey> keys,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions)
        : TBlockReaderFactory<TBlockReader>(
            std::move(chunkState),
            columnFilter,
            timestamp,
            produceAllVersions)
        , Keys_(std::move(keys))
    { }

private:
    const TSharedRange<TLegacyKey> Keys_;

    int KeyIndex_ = 0;


    std::tuple<std::vector<TVersionedRow>, bool> DoRead(const TRowBatchReadOptions& options) override
    {
        std::vector<TVersionedRow> rows;
        rows.reserve(
            std::min(
                std::ssize(Keys_) - KeyIndex_,
                options.MaxRowsPerRead));

        i64 rowCount = 0;
        i64 dataWeight = 0;

        while (rows.size() < rows.capacity()) {
            YT_VERIFY(KeyIndex_ < std::ssize(Keys_));

            rows.push_back(Lookup(Keys_[KeyIndex_++]));

            ++rowCount;
            dataWeight += GetDataWeight(rows.back());
        }

        this->RowCount_ += rowCount;
        this->DataWeight_ += dataWeight;
        this->ChunkState_->PerformanceCounters->StaticChunkRowLookupCount += rowCount;
        this->ChunkState_->PerformanceCounters->StaticChunkRowLookupDataWeightCount += dataWeight;

        return {
            std::move(rows),
            KeyIndex_ == std::ssize(Keys_)
        };
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
            const auto& blockMeta = this->ChunkState_->ChunkMeta->DataBlockMeta()->data_blocks(index.first);

            auto* blockReader = this->CreateBlockReader(
                uncompressedBlock,
                blockMeta,
                false);

            YT_VERIFY(blockReader->SkipToRowIndex(index.second));

            if (this->ChunkState_->KeyComparer(blockReader->GetKey(), key) == 0) {
                return this->CaptureRow(blockReader);
            }
        }

        return TVersionedRow();
    }

    TVersionedRow LookupWithoutHashTable(TLegacyKey key)
    {
        // FIXME(savrus): Use bloom filter here.

        int blockIndex = this->GetBlockIndex(key);
        auto blockCount = this->ChunkState_->ChunkMeta->DataBlockMeta()->data_blocks_size();

        if (blockIndex >= blockCount) {
            return TVersionedRow();
        }

        const auto& uncompressedBlock = this->GetUncompressedBlock(blockIndex);
        const auto& blockMeta = this->ChunkState_->ChunkMeta->DataBlockMeta()->data_blocks(blockIndex);

        auto* blockReader = this->CreateBlockReader(
            uncompressedBlock,
            blockMeta);

        if (!blockReader->SkipToKey(key) || this->ChunkState_->KeyComparer(blockReader->GetKey(), key) != 0) {
            ++this->ChunkState_->PerformanceCounters->StaticChunkRowLookupFalsePositiveCount;
            return TVersionedRow();
        }

        return this->CaptureRow(blockReader);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TChunkStatePtr& chunkState,
    const TClientChunkReadOptions& chunkReadOptions,
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
            TChunkReaderConfig::GetDefault(),
            std::move(underlyingReader),
            chunkState,
            chunkState->ChunkMeta,
            chunkReadOptions,
            keys,
            columnFilter,
            timestamp,
            produceAllVersions);
    };

    if (produceAllVersions && timestamp != AllCommittedTimestamp) {
        return createGenericVersionedReader();
    }

    switch (chunkState->ChunkMeta->GetChunkFormat()) {
        case EChunkFormat::TableSchemalessHorizontal: {
            auto chunkTimestamp = chunkState->ChunkMeta->Misc().min_timestamp();
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader(keys.Size());
            }

            YT_VERIFY(chunkState->TableSchema->GetUniqueKeys());
            return New<TCacheBasedSimpleVersionedLookupChunkReader<THorizontalSchemalessVersionedBlockReader>>(
                chunkState,
                keys,
                columnFilter,
                chunkTimestamp,
                produceAllVersions);
        }

        case EChunkFormat::TableVersionedSimple:
            return New<TCacheBasedSimpleVersionedLookupChunkReader<TSimpleVersionedBlockReader>>(
                chunkState,
                keys,
                columnFilter,
                timestamp,
                produceAllVersions);

        case EChunkFormat::TableUnversionedColumnar:
        case EChunkFormat::TableVersionedColumnar:
            return createGenericVersionedReader();

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TSimpleCacheBasedVersionedRangeChunkReader
    : public TBlockReaderFactory<TBlockReader>
{
public:
    TSimpleCacheBasedVersionedRangeChunkReader(
        TChunkStatePtr chunkState,
        TSharedRange<TRowRange> ranges,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions,
        TSharedRange<TRowRange> clippingRange)
        : TBlockReaderFactory<TBlockReader>(
            std::move(chunkState),
            columnFilter,
            timestamp,
            produceAllVersions)
        , Ranges_(std::move(ranges))
        , ClippingRange_(std::move(clippingRange))
    { }

private:
    TLegacyKey LowerBound_;
    TLegacyKey UpperBound_;

    TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;

    TSharedRange<TRowRange> ClippingRange_;

    // Returns false if finished.
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

        auto newBlockIndex = this->GetBlockIndex(LowerBound_);
        auto blockCount = this->ChunkState_->ChunkMeta->DataBlockMeta()->data_blocks_size();

        if (newBlockIndex >= blockCount) {
            return false;
        }

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
    TBlockReader* BlockReader_;
    bool UpperBoundCheckNeeded_ = false;
    bool NeedLimitUpdate_ = true;

    std::tuple<std::vector<TVersionedRow>, bool> DoRead(const TRowBatchReadOptions& options) override
    {
        if (NeedLimitUpdate_) {
            if (UpdateLimits()) {
                NeedLimitUpdate_ = false;
            } else {
                return {{}, true};
            }
        }

        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        i64 rowCount = 0;
        i64 dataWeight = 0;

        while (rows.size() < rows.capacity()) {
            if (UpperBoundCheckNeeded_ && BlockReader_->GetKey() >= UpperBound_) {
                NeedLimitUpdate_ = true;
                break;
            }

            auto row = this->CaptureRow(BlockReader_);
            if (row) {
                rows.push_back(row);

                ++rowCount;
                dataWeight += GetDataWeight(row);
            }

            if (!BlockReader_->NextRow()) {
                // End-of-block.
                if (++BlockIndex_ >= this->ChunkState_->ChunkMeta->DataBlockMeta()->data_blocks_size()) {
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

        return {
            std::move(rows),
            false
        };
    }

    void UpdateBlockReader()
    {
        const auto& uncompressedBlock = this->GetUncompressedBlock(BlockIndex_);
        const auto& blockMeta = this->ChunkState_->ChunkMeta->DataBlockMeta()->data_blocks(BlockIndex_);

        BlockReader_ = this->CreateBlockReader(
            uncompressedBlock,
            blockMeta);
        this->UpperBoundCheckNeeded_ = (this->UpperBound_ <= this->ChunkState_->ChunkMeta->LegacyBlockLastKeys()[BlockIndex_]);
    }
};

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TChunkStatePtr& chunkState,
    const TClientChunkReadOptions& chunkReadOptions,
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
            TChunkReaderConfig::GetDefault(),
            std::move(underlyingReader),
            chunkState,
            chunkState->ChunkMeta,
            chunkReadOptions,
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
        case EChunkFormat::TableSchemalessHorizontal: {
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

        case EChunkFormat::TableVersionedSimple:
            return New<TSimpleCacheBasedVersionedRangeChunkReader<TSimpleVersionedBlockReader>>(
                chunkState,
                std::move(ranges),
                columnFilter,
                timestamp,
                produceAllVersions,
                singletonClippingRange);

        case EChunkFormat::TableUnversionedColumnar:
        case EChunkFormat::TableVersionedColumnar:
            return createGenericVersionedReader();

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
