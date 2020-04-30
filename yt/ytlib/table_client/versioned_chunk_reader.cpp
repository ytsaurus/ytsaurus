#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "columnar_chunk_reader_base.h"
#include "config.h"
#include "private.h"
#include "schemaless_multi_chunk_reader.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"
#include "versioned_reader_adapter.h"

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/block_id.h>
#include <yt/ytlib/chunk_client/cache_reader.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>
#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/timestamp_reader.h>
#include <yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/compression/codec.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NChunkClient::TDataSliceDescriptor;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const i64 CacheSize = 32 * 1024;
static const i64 MinRowsPerRead = 32;

////////////////////////////////////////////////////////////////////////////////

std::vector<TColumnIdMapping> BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    if (columnFilter.IsUniversal()) {
        return chunkMeta->SchemaIdMapping();
    }

    std::vector<TColumnIdMapping> schemaIdMapping;
    schemaIdMapping.reserve(chunkMeta->SchemaIdMapping().size());
    for (auto index : columnFilter.GetIndexes()) {
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

std::vector<TColumnIdMapping> BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    int keyColumnCount = chunkMeta->GetChunkKeyColumnCount();
    std::vector<TColumnIdMapping> idMapping;
    idMapping.resize(
        keyColumnCount + chunkMeta->SchemaIdMapping().size(),
        TColumnIdMapping{-1,-1});

    for (int index = 0; index < keyColumnCount; ++index) {
        idMapping[index].ReaderSchemaIndex = index;
    }

    if (columnFilter.IsUniversal()) {
        for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
            YT_VERIFY(mapping.ChunkSchemaIndex < idMapping.size());
            YT_VERIFY(mapping.ChunkSchemaIndex >= keyColumnCount);
            idMapping[mapping.ChunkSchemaIndex].ReaderSchemaIndex = mapping.ReaderSchemaIndex;
        }
    } else {
        for (auto index : columnFilter.GetIndexes()) {
            for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
                if (mapping.ReaderSchemaIndex == index) {
                    YT_VERIFY(mapping.ChunkSchemaIndex < idMapping.size());
                    YT_VERIFY(mapping.ChunkSchemaIndex >= keyColumnCount);
                    idMapping[mapping.ChunkSchemaIndex].ReaderSchemaIndex = mapping.ReaderSchemaIndex;
                    break;
                }
            }
        }
    }

    return idMapping;
}

////////////////////////////////////////////////////////////////////////////////

struct TVersionedChunkReaderPoolTag { };

class TSimpleVersionedChunkReaderBase
    : public IVersionedReader
    , public TChunkReaderBase
{
public:
    TSimpleVersionedChunkReaderBase(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientBlockReadOptions& blockReadOptions,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TChunkReaderMemoryManagerPtr& memoryManager,
        TKeyComparer keyComparer = [] (auto lhs, auto rhs) {
            return CompareRows(lhs, rhs);
        });

    virtual TFuture<void> Open() override
    {
        return GetReadyEvent();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TChunkReaderBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TKeyComparer KeyComparer_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

    std::unique_ptr<TSimpleVersionedBlockReader> BlockReader_;

    TChunkedMemoryPool MemoryPool_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TChunkReaderPerformanceCountersPtr PerformanceCounters_;
};

TSimpleVersionedChunkReaderBase::TSimpleVersionedChunkReaderBase(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TChunkReaderMemoryManagerPtr& memoryManager,
    TKeyComparer keyComparer)
    : TChunkReaderBase(
        std::move(config),
        std::move(underlyingReader),
        std::move(blockCache),
        blockReadOptions,
        memoryManager)
    , ChunkMeta_(std::move(chunkMeta))
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , KeyComparer_(std::move(keyComparer))
    , SchemaIdMapping_(BuildVersionedSimpleSchemaIdMapping(columnFilter, ChunkMeta_))
    , MemoryPool_(TVersionedChunkReaderPoolTag())
    , PerformanceCounters_(std::move(performanceCounters))
{
    YT_VERIFY(ChunkMeta_->Misc().sorted());
    YT_VERIFY(ChunkMeta_->GetChunkType() == EChunkType::Table);
    YT_VERIFY(ChunkMeta_->GetChunkFormat() == ETableChunkFormat::VersionedSimple);
    YT_VERIFY(Timestamp_ != AllCommittedTimestamp || columnFilter.IsUniversal());
    YT_VERIFY(PerformanceCounters_);
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedRangeChunkReader
    : public TSimpleVersionedChunkReaderBase
{
public:
    TSimpleVersionedRangeChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientBlockReadOptions& blockReadOptions,
        TSharedRange<TRowRange> ranges,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TSharedRange<TRowRange>& singletonClippingRange,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TSimpleVersionedChunkReaderBase(
            std::move(config),
            std::move(chunkMeta),
            std::move(underlyingReader),
            std::move(blockCache),
            blockReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            produceAllVersions,
            memoryManager)
        , Ranges_(std::move(ranges))
        , ClippingRange_(singletonClippingRange)
    {
        ReadyEvent_ = DoOpen(GetBlockSequence(), ChunkMeta_->Misc());
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        YT_VERIFY(rows->capacity() > 0);

        MemoryPool_.Clear();
        rows->clear();

        if (RangeIndex_ >= Ranges_.Size()) {
            return false;
        }

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

        i64 rowCount = 0;
        i64 dataWeight = 0;

        while (rows->size() < rows->capacity()) {
            if (CheckKeyLimit_ && KeyComparer_(
                BlockReader_->GetKey(), GetCurrentRangeUpperKey()) >= 0)
            {
                if (++RangeIndex_ < Ranges_.Size()) {
                    if (!BlockReader_->SkipToKey(GetCurrentRangeLowerKey())) {
                        BlockEnded_ = true;
                        break;
                    } else {
                        continue;
                    }
                } else {
                    // TODO(lukyan): return false and fix usages of method Read
                    break;
                }
            }

            auto row = BlockReader_->GetRow(&MemoryPool_);
            if (row) {
                YT_ASSERT(
                    rows->empty() ||
                    !rows->back() ||
                    CompareRows(
                        rows->back().BeginKeys(), rows->back().EndKeys(),
                        row.BeginKeys(), row.EndKeys()) < 0);
            }
            rows->push_back(row);
            ++rowCount;
            dataWeight += GetDataWeight(row);

            if (!BlockReader_->NextRow()) {
                BlockEnded_ = true;
                break;
            }
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowReadCount += rowCount;
        PerformanceCounters_->StaticChunkRowReadDataWeightCount += dataWeight;

        return true;
    }

private:
    std::vector<size_t> BlockIndexes_;
    size_t NextBlockIndex_ = 0;
    TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;
    TSharedRange<TRowRange> ClippingRange_;

    const TKey& GetCurrentRangeLowerKey() const
    {
        return GetRangeLowerKey(RangeIndex_);
    }

    const TKey& GetCurrentRangeUpperKey() const
    {
        return GetRangeUpperKey(RangeIndex_);
    }

    const TKey& GetRangeLowerKey(int rangeIndex) const
    {
        if (rangeIndex == 0 && ClippingRange_) {
            if (const auto& clippingLowerBound = ClippingRange_.Front().first) {
                return std::max(clippingLowerBound, Ranges_[rangeIndex].first);
            }
        }
        return Ranges_[rangeIndex].first;
    }

    const TKey& GetRangeUpperKey(int rangeIndex) const
    {
        if (rangeIndex + 1 == Ranges_.Size() && ClippingRange_) {
            if (const auto& clippingUpperBound = ClippingRange_.Front().second) {
                return std::min(clippingUpperBound, Ranges_[rangeIndex].second);
            }
        }
        return Ranges_[rangeIndex].second;
    }

    std::vector<TBlockFetcher::TBlockInfo> GetBlockSequence()
    {
        const auto& blockMetaExt = ChunkMeta_->BlockMeta();
        const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

        std::vector<TBlockFetcher::TBlockInfo> blocks;

        int rangeIndex = 0;
        auto blocksIt = blockIndexKeys.begin();

        while (rangeIndex != Ranges_.size()) {
            blocksIt = std::lower_bound(
                blocksIt,
                blockIndexKeys.end(),
                GetRangeLowerKey(rangeIndex));

            auto blockKeysEnd = std::lower_bound(
                blocksIt,
                blockIndexKeys.end(),
                GetRangeUpperKey(rangeIndex));

            if (blockKeysEnd != blockIndexKeys.end()) {
                auto saved = rangeIndex;
                rangeIndex = BinarySearch(
                    rangeIndex,
                    static_cast<int>(Ranges_.size()),
                    [&] (int index) {
                        return GetRangeUpperKey(index) <= *blockKeysEnd;
                    });

                ++blockKeysEnd;
                YT_VERIFY(rangeIndex > saved);
            } else {
                ++rangeIndex;
            }

            for (auto it = blocksIt; it < blockKeysEnd; ++it) {
                int blockIndex = std::distance(blockIndexKeys.begin(), it);
                BlockIndexes_.push_back(blockIndex);
                auto& blockMeta = blockMetaExt->blocks(blockIndex);
                int priority = blocks.size();
                blocks.push_back(TBlockFetcher::TBlockInfo{blockIndex, blockMeta.uncompressed_size(), priority});
            }

            blocksIt = blockKeysEnd;
        }

        return blocks;
    }

    void LoadBlock()
    {
        int chunkBlockIndex = BlockIndexes_[NextBlockIndex_];
        CheckBlockUpperKeyLimit(
            ChunkMeta_->BlockLastKeys()[chunkBlockIndex],
            GetCurrentRangeUpperKey(),
            ChunkMeta_->GetKeyColumnCount());

        YT_VERIFY(CurrentBlock_ && CurrentBlock_.IsSet());
        BlockReader_.reset(new TSimpleVersionedBlockReader(
            CurrentBlock_.Get().ValueOrThrow().Data,
            ChunkMeta_->BlockMeta()->blocks(chunkBlockIndex),
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_,
            ProduceAllVersions_,
            true));
    }

    virtual void InitFirstBlock() override
    {
        InitNextBlock();
    }

    virtual void InitNextBlock() override
    {
        LoadBlock();
        YT_VERIFY(BlockReader_->SkipToKey(GetCurrentRangeLowerKey()));
        ++NextBlockIndex_;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedLookupChunkReader
    : public TSimpleVersionedChunkReaderBase
{
public:
    TSimpleVersionedLookupChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientBlockReadOptions& blockReadOptions,
        const TSharedRange<TKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TKeyComparer keyComparer,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TSimpleVersionedChunkReaderBase(
            std::move(config),
            std::move(chunkMeta),
            std::move(underlyingReader),
            std::move(blockCache),
            blockReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            produceAllVersions,
            memoryManager,
            std::move(keyComparer))
        , Keys_(keys)
        , KeyFilterTest_(Keys_.Size(), true)
    {
        ReadyEvent_ = DoOpen(GetBlockSequence(), ChunkMeta_->Misc());
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        YT_VERIFY(rows->capacity() > 0);

        MemoryPool_.Clear();
        rows->clear();

        if (!BeginRead()) {
            // Not ready yet.
            return true;
        }

        i64 dataWeight = 0;

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
            PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;

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
                break;
            }

            if (!KeyFilterTest_[RowCount_]) {
                rows->push_back(TVersionedRow());
                ++PerformanceCounters_->StaticChunkRowLookupTrueNegativeCount;
            } else {
                const auto& key = Keys_[RowCount_];
                if (!BlockReader_->SkipToKey(key)) {
                    BlockEnded_ = true;
                    break;
                }

                if (key == BlockReader_->GetKey()) {
                    auto row = BlockReader_->GetRow(&MemoryPool_);
                    rows->push_back(row);
                    ++RowCount_;
                    dataWeight += GetDataWeight(rows->back());
                } else if (BlockReader_->GetKey() > key) {
                    auto nextKeyIt = std::lower_bound(
                        Keys_.begin() + RowCount_,
                        Keys_.end(),
                        BlockReader_->GetKey());

                    size_t skippedKeys = std::distance(Keys_.begin() + RowCount_, nextKeyIt);
                    skippedKeys = std::min(skippedKeys, rows->capacity() - rows->size());

                    rows->insert(rows->end(), skippedKeys, TVersionedRow());
                    RowCount_ += skippedKeys;
                    dataWeight += skippedKeys * GetDataWeight(TVersionedRow());
                } else {
                    YT_ABORT();
                }
            }
        }

        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
        PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;

        return true;
    }

private:
    const TSharedRange<TKey> Keys_;
    std::vector<bool> KeyFilterTest_;
    std::vector<int> BlockIndexes_;

    int NextBlockIndex_ = 0;

    std::vector<TBlockFetcher::TBlockInfo> GetBlockSequence()
    {
        const auto& blockMetaExt = ChunkMeta_->BlockMeta();
        const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

        std::vector<TBlockFetcher::TBlockInfo> blocks;

        auto keyIt = Keys_.begin();
        auto blocksIt = blockIndexKeys.begin();

        while (keyIt != Keys_.end()) {
            blocksIt = std::lower_bound(
                blocksIt,
                blockIndexKeys.end(),
                *keyIt);

            if (blocksIt != blockIndexKeys.end()) {
                auto saved = keyIt;
                keyIt = std::upper_bound(keyIt, Keys_.end(), *blocksIt);
                YT_VERIFY(keyIt > saved);

                int blockIndex = std::distance(blockIndexKeys.begin(), blocksIt);
                BlockIndexes_.push_back(blockIndex);
                auto& blockMeta = blockMetaExt->blocks(blockIndex);
                int priority = blocks.size();
                blocks.push_back(TBlockFetcher::TBlockInfo{blockIndex, blockMeta.uncompressed_size(), priority});

                ++blocksIt;
            } else {
                break;
            }
        }

        return blocks;
    }

    virtual void InitFirstBlock() override
    {
        InitNextBlock();
    }

    virtual void InitNextBlock() override
    {
        int chunkBlockIndex = BlockIndexes_[NextBlockIndex_];
        BlockReader_.reset(new TSimpleVersionedBlockReader(
            CurrentBlock_.Get().ValueOrThrow().Data,
            ChunkMeta_->BlockMeta()->blocks(chunkBlockIndex),
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_,
            ProduceAllVersions_,
            true));
        ++NextBlockIndex_;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TColumnarVersionedChunkReaderBase
    : public TBase
    , public IVersionedReader
{
public:
    TColumnarVersionedChunkReaderBase(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientBlockReadOptions& blockReadOptions,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TBase(chunkMeta, std::move(config), std::move(underlyingReader), std::move(blockCache), blockReadOptions, memoryManager)
        , VersionedChunkMeta_(std::move(chunkMeta))
        , Timestamp_(timestamp)
        , SchemaIdMapping_(BuildVersionedSimpleSchemaIdMapping(columnFilter, VersionedChunkMeta_))
        , PerformanceCounters_(std::move(performanceCounters))
    {
        YT_VERIFY(VersionedChunkMeta_->Misc().sorted());
        YT_VERIFY(VersionedChunkMeta_->GetChunkType() == EChunkType::Table);
        YT_VERIFY(VersionedChunkMeta_->GetChunkFormat() == ETableChunkFormat::VersionedColumnar);
        YT_VERIFY(Timestamp_ != AllCommittedTimestamp || columnFilter.IsUniversal());
        YT_VERIFY(PerformanceCounters_);

        KeyColumnReaders_.resize(VersionedChunkMeta_->GetKeyColumnCount());
        for (int keyColumnIndex = 0;
             keyColumnIndex < VersionedChunkMeta_->GetChunkKeyColumnCount();
             ++keyColumnIndex)
        {
            auto columnReader = CreateUnversionedColumnReader(
                VersionedChunkMeta_->ChunkSchema().Columns()[keyColumnIndex],
                VersionedChunkMeta_->ColumnMeta()->columns(keyColumnIndex),
                keyColumnIndex,
                keyColumnIndex);
            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            TBase::Columns_.emplace_back(std::move(columnReader), keyColumnIndex);
        }

        // Null readers for wider keys.
        for (int keyColumnIndex = VersionedChunkMeta_->GetChunkKeyColumnCount();
             keyColumnIndex < KeyColumnReaders_.size();
             ++keyColumnIndex)
        {
            auto columnReader = CreateBlocklessUnversionedNullColumnReader(
                keyColumnIndex,
                keyColumnIndex);
            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            TBase::Columns_.emplace_back(std::move(columnReader));
        }

        for (const auto& idMapping : SchemaIdMapping_) {
            auto columnReader = CreateVersionedColumnReader(
                VersionedChunkMeta_->ChunkSchema().Columns()[idMapping.ChunkSchemaIndex],
                VersionedChunkMeta_->ColumnMeta()->columns(idMapping.ChunkSchemaIndex),
                idMapping.ReaderSchemaIndex);

            ValueColumnReaders_.push_back(columnReader.get());
            TBase::Columns_.emplace_back(std::move(columnReader), idMapping.ChunkSchemaIndex);
        }
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TFuture<void> Open() override
    {
        return VoidFuture;
    }

protected:
    const TCachedVersionedChunkMetaPtr VersionedChunkMeta_;
    const TTimestamp Timestamp_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TChunkReaderPerformanceCountersPtr PerformanceCounters_;

    std::vector<IUnversionedColumnReader*> KeyColumnReaders_;
    std::vector<IVersionedColumnReader*> ValueColumnReaders_;
};

////////////////////////////////////////////////////////////////////////////////

class TScanColumnarRowBuilder
{
public:
    TScanColumnarRowBuilder(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<IVersionedColumnReader*>& valueColumnReaders,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp)
        : ChunkMeta_(chunkMeta)
        , ValueColumnReaders_(valueColumnReaders)
        , Pool_(TVersionedChunkReaderPoolTag())
        , SchemaIdMapping_(schemaIdMapping)
        , Timestamp_(timestamp)
    { }

    // We pass away the ownership of the reader.
    // All column reader are owned by the chunk reader.
    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(TimestampReader_ == nullptr);

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TScanTransactionTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex),
            Timestamp_);
        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableRange<TMutableVersionedRow> AllocateRows(
        std::vector<TVersionedRow>* rows,
        i64 rowLimit,
        i64 currentRowIndex,
        i64 safeUpperRowIndex)
    {
        TimestampReader_->PrepareRows(rowLimit);
        auto timestampIndexRanges = TimestampReader_->GetTimestampIndexRanges(rowLimit);

        std::vector<ui32> valueCountPerRow(rowLimit, 0);
        std::vector<ui32> columnValueCount(rowLimit, 0);
        for (int valueColumnIndex = 0; valueColumnIndex < SchemaIdMapping_.size(); ++valueColumnIndex) {
            const auto& idMapping = SchemaIdMapping_[valueColumnIndex];
            const auto& columnSchema = ChunkMeta_->ChunkSchema().Columns()[idMapping.ChunkSchemaIndex];
            if (columnSchema.Aggregate()) {
                // Possibly multiple values per column for aggregate columns.
                ValueColumnReaders_[valueColumnIndex]->GetValueCounts(TMutableRange<ui32>(columnValueCount));
            } else {
                // No more than one value per column for aggregate columns.
                columnValueCount.resize(0);
                columnValueCount.resize(rowLimit, 1);
            }

            for (int index = 0; index < rowLimit; ++index) {
                valueCountPerRow[index] += columnValueCount[index];
            }
        }

        i64 rangeBegin = rows->size();
        for (i64 index = 0; index < rowLimit; ++index) {
            i64 rowIndex = currentRowIndex + index;

            auto deleteTimestamp = TimestampReader_->GetDeleteTimestamp(rowIndex);
            auto timestampIndexRange = timestampIndexRanges[index];

            bool hasWriteTimestamp = timestampIndexRange.first < timestampIndexRange.second;
            bool hasDeleteTimestamp = deleteTimestamp != NullTimestamp;
            if (!hasWriteTimestamp && !hasDeleteTimestamp) {
                if (rowIndex < safeUpperRowIndex) {
                    rows->push_back(TMutableVersionedRow());
                } else {
                    // Reserve space for key, to compare with #UpperLimit_.
                    rows->push_back(TMutableVersionedRow::Allocate(
                        &Pool_,
                        ChunkMeta_->GetKeyColumnCount(),
                        0,
                        0,
                        0));
                }
            } else {
                // Allocate according to schema.
                auto row = TMutableVersionedRow::Allocate(
                    &Pool_,
                    ChunkMeta_->GetKeyColumnCount(),
                    hasWriteTimestamp ? valueCountPerRow[index] : 0,
                    hasWriteTimestamp ? 1 : 0,
                    hasDeleteTimestamp ? 1 : 0);
                rows->push_back(row);

                if (hasDeleteTimestamp) {
                    *row.BeginDeleteTimestamps() = deleteTimestamp;
                }

                if (hasWriteTimestamp) {
                    *row.BeginWriteTimestamps() = TimestampReader_->GetWriteTimestamp(rowIndex);

                    // Value count is increased inside value column readers.
                    row.SetValueCount(0);
                }
            }
        }

        return TMutableRange<TMutableVersionedRow>(
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin),
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin + rowLimit));
    }

    void ReadValues(TMutableRange<TMutableVersionedRow> range, i64 currentRowIndex)
    {
        // Read timestamp indexes.
        auto timestampIndexRanges = TimestampReader_->GetTimestampIndexRanges(range.Size());

        for (auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValues(range, timestampIndexRanges, false);
        }

        // Read timestamps.
        for (i64 index = 0; index < range.Size(); ++index) {
            if (!range[index]) {
                continue;
            } else if (range[index].GetWriteTimestampCount() == 0 && range[index].GetDeleteTimestampCount() == 0) {
                // This row was created in order to compare with UpperLimit.
                range[index] = TMutableVersionedRow();
                continue;
            }

            for (auto* value = range[index].BeginValues(); value != range[index].EndValues(); ++value) {
                value->Timestamp = TimestampReader_->GetValueTimestamp(
                    currentRowIndex + index,
                    static_cast<ui32>(value->Timestamp));
            }
        }

        TimestampReader_->SkipPreparedRows();
    }

    void Clear()
    {
        Pool_.Clear();
    }

private:
    TScanTransactionTimestampReader* TimestampReader_ = nullptr;

    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    std::vector<IVersionedColumnReader*>& ValueColumnReaders_;

    TChunkedMemoryPool Pool_;

    const std::vector<TColumnIdMapping>& SchemaIdMapping_;

    TTimestamp Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionColumnarRowBuilder
{
public:
    TCompactionColumnarRowBuilder(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<IVersionedColumnReader*>& valueColumnReaders,
        const std::vector<TColumnIdMapping>& /* schemaIdMapping */,
        TTimestamp /* timestamp */)
        : ChunkMeta_(chunkMeta)
        , ValueColumnReaders_(valueColumnReaders)
        , Pool_(TVersionedChunkReaderPoolTag())
    { }

    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(TimestampReader_ == nullptr);
        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TCompactionTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex));

        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableRange<TMutableVersionedRow> AllocateRows(
        std::vector<TVersionedRow>* rows,
        i64 rowLimit,
        i64 currentRowIndex,
        i64 /* safeUpperRowIndex */)
    {
        TimestampReader_->PrepareRows(rowLimit);
        i64 rangeBegin = rows->size();

        std::vector<ui32> valueCountPerRow(rowLimit, 0);
        std::vector<ui32> columnValueCount(rowLimit, 0);
        for (const auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->GetValueCounts(TMutableRange<ui32>(columnValueCount));
            for (int index = 0; index < rowLimit; ++index) {
                valueCountPerRow[index] += columnValueCount[index];
            }
        }

        for (i64 index = 0; index < rowLimit; ++index) {
            i64 rowIndex = currentRowIndex + index;

            auto row = TMutableVersionedRow::Allocate(
                &Pool_,
                ChunkMeta_->GetKeyColumnCount(),
                valueCountPerRow[index],
                TimestampReader_->GetWriteTimestampCount(rowIndex),
                TimestampReader_->GetDeleteTimestampCount(rowIndex));
            rows->push_back(row);

            row.SetValueCount(0);

            for (
                ui32 timestampIndex = 0;
                timestampIndex < TimestampReader_->GetWriteTimestampCount(rowIndex);
                ++timestampIndex)
            {
                row.BeginWriteTimestamps()[timestampIndex] = TimestampReader_->GetValueTimestamp(rowIndex, timestampIndex);
            }

            for (
                ui32 timestampIndex = 0;
                timestampIndex < TimestampReader_->GetDeleteTimestampCount(rowIndex);
                ++timestampIndex)
            {
                row.BeginDeleteTimestamps()[timestampIndex] = TimestampReader_->GetDeleteTimestamp(rowIndex, timestampIndex);
            }
        }

        return TMutableRange<TMutableVersionedRow>(
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin),
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin + rowLimit));
    }

    void ReadValues(TMutableRange<TMutableVersionedRow> range, i64 currentRowIndex)
    {
        for (auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadAllValues(range);
        }

        // Read timestamps.
        for (i64 index = 0; index < range.Size(); ++index) {
            if (!range[index]) {
                continue;
            }

            for (auto* value = range[index].BeginValues(); value != range[index].EndValues(); ++value) {
                value->Timestamp = TimestampReader_->GetValueTimestamp(
                    currentRowIndex + index,
                    static_cast<ui32>(value->Timestamp));
            }
        }

        TimestampReader_->SkipPreparedRows();
    }

    void Clear()
    {
        Pool_.Clear();
    }

private:
    TCompactionTimestampReader* TimestampReader_ = nullptr;

    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    std::vector<IVersionedColumnReader*>& ValueColumnReaders_;

    TChunkedMemoryPool Pool_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowBuilder>
class TColumnarVersionedRangeChunkReader
    : public TColumnarVersionedChunkReaderBase<TColumnarRangeChunkReaderBase>
{
public:
    TColumnarVersionedRangeChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientBlockReadOptions& blockReadOptions,
        TSharedRange<TRowRange> ranges,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TColumnarVersionedChunkReaderBase(
            std::move(config),
            chunkMeta,
            std::move(underlyingReader),
            std::move(blockCache),
            blockReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            memoryManager)
        , RowBuilder_(chunkMeta, ValueColumnReaders_, SchemaIdMapping_, timestamp)
    {
        YT_VERIFY(ranges.Size() == 1);
        LowerLimit_.SetKey(TOwningKey(ranges[0].first));
        UpperLimit_.SetKey(TOwningKey(ranges[0].second));

        int timestampReaderIndex = VersionedChunkMeta_->ColumnMeta()->columns().size() - 1;
        Columns_.emplace_back(RowBuilder_.CreateTimestampReader(), timestampReaderIndex);

        // Empirical formula to determine max rows per read for better cache friendliness.
        MaxRowsPerRead_ = CacheSize / (KeyColumnReaders_.size() * sizeof(TUnversionedValue) +
            ValueColumnReaders_.size() * sizeof(TVersionedValue));
        MaxRowsPerRead_ = std::max(MaxRowsPerRead_, MinRowsPerRead);

        InitLowerRowIndex();
        InitUpperRowIndex();

        if (LowerRowIndex_ < HardUpperRowIndex_) {
            InitBlockFetcher();
            ReadyEvent_ = RequestFirstBlocks();
        } else {
            Initialized_ = true;
            Completed_ = true;
        }
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        YT_VERIFY(rows->capacity() > 0);
        rows->clear();
        RowBuilder_.Clear();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (!Initialized_) {
            ResetExhaustedColumns();
            Initialize(MakeRange(KeyColumnReaders_.data(), KeyColumnReaders_.size()));
            Initialized_ = true;
            RowIndex_ = LowerRowIndex_;
        }

        if (Completed_) {
            return false;
        }

        while (rows->size() < rows->capacity()) {
            ResetExhaustedColumns();

            // Define how many to read.
            i64 rowLimit = std::min(HardUpperRowIndex_ - RowIndex_, static_cast<i64>(rows->capacity() - rows->size()));
            for (const auto& column : Columns_) {
                rowLimit = std::min(column.ColumnReader->GetReadyUpperRowIndex() - RowIndex_, rowLimit);
            }
            rowLimit = std::min(rowLimit, MaxRowsPerRead_);
            YT_VERIFY(rowLimit > 0);

            auto range = RowBuilder_.AllocateRows(rows, rowLimit, RowIndex_, SafeUpperRowIndex_);

            // Read key values.
            for (auto& keyColumnReader : KeyColumnReaders_) {
                keyColumnReader->ReadValues(range);
            }

            YT_VERIFY(RowIndex_ + rowLimit <= HardUpperRowIndex_);
            if (RowIndex_ + rowLimit > SafeUpperRowIndex_ && UpperLimit_.HasKey()) {
                i64 index = std::max(SafeUpperRowIndex_ - RowIndex_, i64(0));
                for (; index < rowLimit; ++index) {
                    if (CompareRows(
                        range[index].BeginKeys(),
                        range[index].EndKeys(),
                        UpperLimit_.GetKey().Begin(),
                        UpperLimit_.GetKey().End()) >= 0)
                    {
                        Completed_ = true;
                        range = range.Slice(range.Begin(), range.Begin() + index);
                        rows->resize(rows->size() - rowLimit + index);
                        break;
                    }
                }
            }

            if (RowIndex_ + rowLimit == HardUpperRowIndex_) {
                Completed_ = true;
            }

            RowBuilder_.ReadValues(range, RowIndex_);

            RowIndex_ += range.Size();
            if (Completed_ || !TryFetchNextRow()) {
                break;
            }
        }

        i64 rowCount = rows->size();
        i64 dataWeight = 0;
        for (auto row : *rows) {
            dataWeight += GetDataWeight(row);
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowReadCount += rowCount;
        PerformanceCounters_->StaticChunkRowReadDataWeightCount += dataWeight;

        return true;
    }

private:
    bool Initialized_ = false;
    bool Completed_ = false;

    i64 MaxRowsPerRead_;
    i64 RowIndex_ = 0;

    TRowBuilder RowBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupSingleVersionColumnarRowBuilder
{
public:
    TLookupSingleVersionColumnarRowBuilder(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<IUnversionedColumnReader*>& keyColumnReaders,
        std::vector<IVersionedColumnReader*>& valueColumnReaders,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp)
        : ChunkMeta_(chunkMeta)
        , KeyColumnReaders_(keyColumnReaders)
        , ValueColumnReaders_(valueColumnReaders)
        , Pool_(TVersionedChunkReaderPoolTag())
        , SchemaIdMapping_(schemaIdMapping)
        , Timestamp_(timestamp)
    { }

    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(TimestampReader_ == nullptr);

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TLookupTransactionTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex),
            Timestamp_);
        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableVersionedRow ReadRow(i64 rowIndex)
    {
        auto deleteTimestamp = TimestampReader_->GetDeleteTimestamp();
        auto timestampIndexRange = TimestampReader_->GetTimestampIndexRange();

        bool hasWriteTimestamp = timestampIndexRange.first < timestampIndexRange.second;
        bool hasDeleteTimestamp = deleteTimestamp != NullTimestamp;
        if (!hasWriteTimestamp && !hasDeleteTimestamp) {
            // No record of this key at this point of time.
            return TMutableVersionedRow();
        }

        size_t valueCount = 0;
        for (int valueColumnIndex = 0; valueColumnIndex < SchemaIdMapping_.size(); ++valueColumnIndex) {
            const auto& idMapping = SchemaIdMapping_[valueColumnIndex];
            const auto& columnSchema = ChunkMeta_->ChunkSchema().Columns()[idMapping.ChunkSchemaIndex];
            ui32 columnValueCount = 1;
            if (columnSchema.Aggregate()) {
                // Possibly multiple values per column for aggregate columns.
                ValueColumnReaders_[valueColumnIndex]->GetValueCounts(TMutableRange<ui32>(&columnValueCount, 1));
            }

            valueCount += columnValueCount;
        }

        // Allocate according to schema.
        auto row = TMutableVersionedRow::Allocate(
            &Pool_,
            ChunkMeta_->GetKeyColumnCount(),
            hasWriteTimestamp ? valueCount : 0,
            hasWriteTimestamp ? 1 : 0,
            hasDeleteTimestamp ? 1 : 0);

        // Read key values.
        for (auto& keyColumnReader : KeyColumnReaders_) {
            keyColumnReader->ReadValues(TMutableRange<TMutableVersionedRow>(&row, 1));
        }

        if (hasDeleteTimestamp) {
            *row.BeginDeleteTimestamps() = deleteTimestamp;
        }

        if (!hasWriteTimestamp) {
            return row;
        }

        // Value count is increased inside value column readers.
        row.SetValueCount(0);

        // Read non-key values.
        for (const auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValues(
                TMutableRange<TMutableVersionedRow>(&row, 1),
                MakeRange(&timestampIndexRange, 1),
                false);
        }

        for (int i = 0; i < row.GetValueCount(); ++i) {
            row.BeginValues()[i].Timestamp = TimestampReader_->GetTimestamp(static_cast<i32>(row.BeginValues()[i].Timestamp));
        }

        *row.BeginWriteTimestamps() = TimestampReader_->GetWriteTimestamp();
        return row;
    }

    void Clear()
    {
        Pool_.Clear();
    }

private:
    TLookupTransactionTimestampReader* TimestampReader_ = nullptr;

    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    std::vector<IUnversionedColumnReader*>& KeyColumnReaders_;
    std::vector<IVersionedColumnReader*>& ValueColumnReaders_;

    TChunkedMemoryPool Pool_;

    const std::vector<TColumnIdMapping>& SchemaIdMapping_;

    TTimestamp Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupAllVersionsColumnarRowBuilder
{
public:
    TLookupAllVersionsColumnarRowBuilder(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<IUnversionedColumnReader*>& keyColumnReaders,
        std::vector<IVersionedColumnReader*>& valueColumnReaders,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp)
        : ChunkMeta_(chunkMeta)
        , KeyColumnReaders_(keyColumnReaders)
        , ValueColumnReaders_(valueColumnReaders)
        , Pool_(TVersionedChunkReaderPoolTag())
        , SchemaIdMapping_(schemaIdMapping)
        , Timestamp_(timestamp)
    { }

    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(TimestampReader_ == nullptr);

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TLookupTransactionAllVersionsTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex),
            Timestamp_);
        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableVersionedRow ReadRow(i64 rowIndex)
    {
        int writeTimestampCount = TimestampReader_->GetWriteTimestampCount();
        int deleteTimestampCount = TimestampReader_->GetDeleteTimestampCount();

        if (writeTimestampCount == 0 && deleteTimestampCount == 0) {
            return {};
        }

        size_t valueCount = 0;
        for (int valueColumnIndex = 0; valueColumnIndex < SchemaIdMapping_.size(); ++valueColumnIndex) {
            ui32 columnValueCount;
            ValueColumnReaders_[valueColumnIndex]->GetValueCounts(TMutableRange<ui32>(&columnValueCount, 1));
            valueCount += columnValueCount;
        }

        // Allocate according to schema.
        auto row = TMutableVersionedRow::Allocate(
            &Pool_,
            ChunkMeta_->GetKeyColumnCount(),
            writeTimestampCount > 0 ? valueCount : 0,
            writeTimestampCount,
            deleteTimestampCount);

        // Read key values.
        for (auto& keyColumnReader : KeyColumnReaders_) {
            keyColumnReader->ReadValues(TMutableRange<TMutableVersionedRow>(&row, 1));
        }

        // Read delete timestamps.
        for (ui32 timestampIndex = 0; timestampIndex < deleteTimestampCount; ++timestampIndex) {
            row.BeginDeleteTimestamps()[timestampIndex] = TimestampReader_->GetDeleteTimestamp(timestampIndex);

        }

        if (writeTimestampCount == 0) {
            return row;
        }

        // Read write timestamps.
        for (ui32 timestampIndex = 0; timestampIndex < writeTimestampCount; ++timestampIndex) {
            row.BeginWriteTimestamps()[timestampIndex] = TimestampReader_->GetValueTimestamp(timestampIndex);
        }

        // Value count is increased inside value column readers.
        row.SetValueCount(0);

        // Read non-key values.
        auto writeTimestampIndexRange = TimestampReader_->GetWriteTimestampIndexRange();
        auto* memoryTo = const_cast<char*>(row.GetMemoryEnd());
        for (const auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValues(
                TMutableRange<TMutableVersionedRow>(&row, 1),
                MakeRange(&writeTimestampIndexRange, 1),
                true);
        }

        for (int i = 0; i < row.GetValueCount(); ++i) {
            row.BeginValues()[i].Timestamp = TimestampReader_->GetValueTimestamp(
                static_cast<i32>(row.BeginValues()[i].Timestamp) - writeTimestampIndexRange.first);
        }

        auto* memoryFrom = const_cast<char*>(row.GetMemoryEnd());
        Pool_.Free(memoryFrom, memoryTo);

        return row;
    }

    void Clear()
    {
        Pool_.Clear();
    }

private:
    TLookupTransactionAllVersionsTimestampReader* TimestampReader_ = nullptr;

    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    std::vector<IUnversionedColumnReader*>& KeyColumnReaders_;
    std::vector<IVersionedColumnReader*>& ValueColumnReaders_;

    TChunkedMemoryPool Pool_;

    const std::vector<TColumnIdMapping>& SchemaIdMapping_;

    TTimestamp Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

template<class TRowBuilder>
class TColumnarVersionedLookupChunkReader
    : public TColumnarVersionedChunkReaderBase<TColumnarLookupChunkReaderBase>
{
public:
    TColumnarVersionedLookupChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientBlockReadOptions& blockReadOptions,
        const TSharedRange<TKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TColumnarVersionedChunkReaderBase(
            std::move(config),
            chunkMeta,
            std::move(underlyingReader),
            std::move(blockCache),
            blockReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            memoryManager)
        , RowBuilder_(
            chunkMeta,
            KeyColumnReaders_,
            ValueColumnReaders_,
            SchemaIdMapping_,
            timestamp)
    {
        Keys_ = keys;

        int timestampReaderIndex = VersionedChunkMeta_->ColumnMeta()->columns().size() - 1;
        Columns_.emplace_back(RowBuilder_.CreateTimestampReader(), timestampReaderIndex);

        Initialize();

        ReadyEvent_ = RequestFirstBlocks();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        rows->clear();
        RowBuilder_.Clear();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (NextKeyIndex_ == Keys_.Size()) {
            return false;
        }

        while (rows->size() < rows->capacity()) {
            ResetExhaustedColumns();

            if (RowIndexes_[NextKeyIndex_] < VersionedChunkMeta_->Misc().row_count()) {
                const auto& key = Keys_[NextKeyIndex_];
                YT_VERIFY(key.GetCount() == VersionedChunkMeta_->GetKeyColumnCount());

                // Reading row.
                i64 lowerRowIndex = KeyColumnReaders_[0]->GetCurrentRowIndex();
                i64 upperRowIndex = KeyColumnReaders_[0]->GetBlockUpperRowIndex();
                for (int i = 0; i < VersionedChunkMeta_->GetKeyColumnCount(); ++i) {
                    std::tie(lowerRowIndex, upperRowIndex) = KeyColumnReaders_[i]->GetEqualRange(
                        key[i],
                        lowerRowIndex,
                        upperRowIndex);
                }

                if (upperRowIndex == lowerRowIndex) {
                    // Key does not exist.
                    rows->push_back(TMutableVersionedRow());
                } else {
                    // Key can be present in exactly one row.
                    YT_VERIFY(upperRowIndex == lowerRowIndex + 1);
                    i64 rowIndex = lowerRowIndex;

                    rows->push_back(ReadRow(rowIndex));
                }
            } else {
                // Key oversteps chunk boundaries.
                rows->push_back(TMutableVersionedRow());
            }

            if (++NextKeyIndex_ == Keys_.Size() || !TryFetchNextRow()) {
                break;
            }
        }

        i64 rowCount = rows->size();
        i64 dataWeight = 0;
        for (auto row : *rows) {
            dataWeight += GetDataWeight(row);
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
        PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;

        return true;
    }

private:
    TLookupTransactionTimestampReader* TimestampReader_;

    TRowBuilder RowBuilder_;

    TMutableVersionedRow ReadRow(i64 rowIndex)
    {
        for (auto& column : Columns_) {
            column.ColumnReader->SkipToRowIndex(rowIndex);
        }

        return RowBuilder_.ReadRow(rowIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFilteringReader
    : public IVersionedReader
{
public:
    TFilteringReader(
        const IVersionedReaderPtr& underlyingReader,
        TSharedRange<TRowRange> ranges)
        : UnderlyingReader_(underlyingReader)
        , Ranges_(ranges)
    { }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows)
    {
        auto comparator = [] (const TVersionedRow& lhs, const TUnversionedRow& rhs) {
            return CompareRows(lhs.BeginKeys(), lhs.EndKeys(), rhs.Begin(), rhs.End()) < 0;
        };

        rows->clear();
        bool hasMoreData = true;
        while (rows->empty() && hasMoreData) {
            hasMoreData = UnderlyingReader_->Read(rows);

            if (rows->empty()) {
                break;
            }

            rows->erase(
                std::remove_if(
                    rows->begin(),
                    rows->end(),
                    [] (TVersionedRow row) {
                        return !row;
                    }),
                rows->end());

            auto finish = rows->begin();
            for (auto start = rows->begin(); start != rows->end() && RangeIndex_ < Ranges_.Size();) {
                start = std::lower_bound(start, rows->end(), Ranges_[RangeIndex_].first, comparator);

                if (start != rows->end() && !comparator(*start, Ranges_[RangeIndex_].second)) {
                    auto nextBoundIt = std::upper_bound(
                        Ranges_.begin() + RangeIndex_,
                        Ranges_.end(),
                        *start,
                        [&] (const TVersionedRow& lhs, const TRowRange& rhs) {
                            return comparator(lhs, rhs.second);
                        });
                    auto newNextBoundIndex = std::distance(Ranges_.begin(), nextBoundIt);

                    YT_VERIFY(newNextBoundIndex > RangeIndex_);

                    RangeIndex_ = std::distance(Ranges_.begin(), nextBoundIt);
                    continue;
                }

                auto end = std::lower_bound(start, rows->end(), Ranges_[RangeIndex_].second, comparator);

                finish = std::move(start, end, finish);

                if (end != rows->end()) {
                    ++RangeIndex_;
                }

                start = end;
            }
            size_t newSize = std::distance(rows->begin(), finish);

            YT_VERIFY(newSize <= rows->size());
            rows->resize(newSize);

            if (RangeIndex_ == Ranges_.Size()) {
                hasMoreData = false;
            }
        }

        return !rows->empty() || hasMoreData;
    }

private:
    IVersionedReaderPtr UnderlyingReader_;
    TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;

};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TClientBlockReadOptions& blockReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TSharedRange<TRowRange>& singletonClippingRange,
    const TChunkReaderMemoryManagerPtr& memoryManager)
{
    const auto& blockCache = chunkState->BlockCache;
    const auto& performanceCounters = chunkState->PerformanceCounters;

    auto getCappedBounds = [&ranges, &singletonClippingRange] {
        TKey lowerBound = ranges.Front().first;
        TKey upperBound = ranges.Back().second;

        YT_VERIFY(singletonClippingRange.Size() <= 1);
        if (singletonClippingRange.Size() > 0) {
            TKey clippedLowerBound = singletonClippingRange.Front().first;
            TKey clippedUpperBound = singletonClippingRange.Front().second;
            if (clippedLowerBound && clippedLowerBound > lowerBound) {
                lowerBound = clippedLowerBound;
            }
            if (clippedUpperBound && clippedUpperBound < upperBound) {
                upperBound = clippedUpperBound;
            }
        }

        SmallVector<TRowRange, 1> cappedBounds(1, TRowRange(lowerBound, upperBound));
        return MakeSharedRange(std::move(cappedBounds), ranges.GetHolder(), singletonClippingRange.GetHolder());
    };

    if (chunkState->ChunkTimestamp && timestamp < chunkState->ChunkTimestamp) {
        return CreateEmptyVersionedReader();
    }

    IVersionedReaderPtr reader;

    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::VersionedSimple:
            reader = New<TSimpleVersionedRangeChunkReader>(
                std::move(config),
                chunkMeta,
                std::move(chunkReader),
                blockCache,
                blockReadOptions,
                std::move(ranges),
                columnFilter,
                performanceCounters,
                timestamp,
                produceAllVersions,
                singletonClippingRange,
                memoryManager);
            break;

        case ETableChunkFormat::VersionedColumnar: {
            YT_VERIFY(!ranges.Empty());
            IVersionedReaderPtr unwrappedReader;

            if (timestamp == AllCommittedTimestamp) {
                unwrappedReader = New<TColumnarVersionedRangeChunkReader<TCompactionColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    blockCache,
                    blockReadOptions,
                    getCappedBounds(),
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            } else {
                unwrappedReader  = New<TColumnarVersionedRangeChunkReader<TScanColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    blockCache,
                    blockReadOptions,
                    getCappedBounds(),
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            }
            reader = New<TFilteringReader>(unwrappedReader, ranges);
            break;
        }

        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::SchemalessHorizontal: {
            // COMPAT(sandello): Fix me.
            if (produceAllVersions && timestamp != AllCommittedTimestamp) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with a particular timestamp");
            }

            auto chunkTimestamp = chunkState->ChunkTimestamp
                ? chunkState->ChunkTimestamp
                : static_cast<TTimestamp>(chunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader();
            }

            YT_VERIFY(!ranges.Empty());

            auto schemalessReaderFactory = [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                auto options = New<TTableReaderOptions>();
                options->DynamicTable = true;

                auto cappedBounds = getCappedBounds();

                TReadRange readRange(
                    TReadLimit(TOwningKey(cappedBounds.Front().first)),
                    TReadLimit(TOwningKey(cappedBounds.Front().second)));

                return CreateSchemalessChunkReader(
                    chunkState,
                    chunkMeta,
                    config,
                    options,
                    chunkReader,
                    nameTable,
                    blockReadOptions,
                    chunkMeta->Schema().GetKeyColumns(),
                    /* omittedInaccessibleColumns */ {},
                    columnFilter,
                    readRange,
                    std::nullopt,
                    memoryManager);
            };
            auto schemafulReaderFactory = [&] (const TTableSchema& schema, const TColumnFilter& columnFilter) {
                return CreateSchemafulReaderAdapter(schemalessReaderFactory, schema, columnFilter);
            };

            auto unwrappedReader = CreateVersionedReaderAdapter(
                schemafulReaderFactory,
                chunkMeta->Schema(),
                columnFilter,
                chunkTimestamp);

            return New<TFilteringReader>(unwrappedReader, ranges);
        }
        default:
            YT_ABORT();
    }

    if (chunkState->ChunkTimestamp) {
        return CreateTimestampResettingAdapter(
            reader,
            chunkState->ChunkTimestamp);
    } else {
        return reader;
    }
}

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TClientBlockReadOptions& blockReadOptions,
    TOwningKey lowerLimit,
    TOwningKey upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TChunkReaderMemoryManagerPtr& memoryManager)
{
    return CreateVersionedChunkReader(
        config,
        chunkReader,
        chunkState,
        chunkMeta,
        blockReadOptions,
        MakeSingletonRowRange(lowerLimit, upperLimit),
        columnFilter,
        timestamp,
        produceAllVersions,
        {},
        memoryManager);
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TClientBlockReadOptions& blockReadOptions,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TChunkReaderMemoryManagerPtr& memoryManager)
{
    const auto& blockCache = chunkState->BlockCache;
    const auto& performanceCounters = chunkState->PerformanceCounters;
    const auto& keyComparer = chunkState->KeyComparer;

    if (chunkState->ChunkTimestamp && timestamp < chunkState->ChunkTimestamp) {
        return CreateEmptyVersionedReader(keys.Size());
    }

    IVersionedReaderPtr reader;

    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::VersionedSimple:
            reader = New<TSimpleVersionedLookupChunkReader>(
                std::move(config),
                chunkMeta,
                std::move(chunkReader),
                blockCache,
                blockReadOptions,
                keys,
                columnFilter,
                performanceCounters,
                keyComparer,
                timestamp,
                produceAllVersions,
                memoryManager);
            break;

        case ETableChunkFormat::VersionedColumnar: {
            if (produceAllVersions) {
                YT_VERIFY(columnFilter.IsUniversal());
            }
            if (produceAllVersions) {
                reader = New<TColumnarVersionedLookupChunkReader<TLookupAllVersionsColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    blockCache,
                    blockReadOptions,
                    keys,
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            } else {
                reader = New<TColumnarVersionedLookupChunkReader<TLookupSingleVersionColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    blockCache,
                    blockReadOptions,
                    keys,
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            }
            break;
        }

        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::SchemalessHorizontal: {
            if (produceAllVersions && !columnFilter.IsUniversal()) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with non-universal column filter");
            }

            auto chunkTimestamp = chunkState->ChunkTimestamp
                ? chunkState->ChunkTimestamp
                : static_cast<TTimestamp>(chunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader(keys.Size());
            }

            auto schemalessReaderFactory = [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                auto options = New<TTableReaderOptions>();
                options->DynamicTable = true;

                return CreateSchemalessChunkReader(
                    chunkState,
                    chunkMeta,
                    config,
                    options,
                    chunkReader,
                    nameTable,
                    blockReadOptions,
                    chunkMeta->Schema().GetKeyColumns(),
                    /* omittedInaccessibleColumns */ {},
                    columnFilter,
                    keys,
                    nullptr,
                    std::nullopt,
                    memoryManager);
            };
            auto schemafulReaderFactory = [&] (const TTableSchema& schema, const TColumnFilter& columnFilter) {
                return CreateSchemafulReaderAdapter(schemalessReaderFactory, schema, columnFilter);
            };

            return CreateVersionedReaderAdapter(
                std::move(schemafulReaderFactory),
                chunkMeta->Schema(),
                columnFilter,
                chunkTimestamp);
        }

        default:
            YT_ABORT();
    }

    if (chunkState->ChunkTimestamp) {
        return CreateTimestampResettingAdapter(
            reader,
            chunkState->ChunkTimestamp);
    } else {
        return reader;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
