#include "versioned_chunk_reader.h"
#include "private.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "columnar_chunk_reader_base.h"
#include "config.h"
#include "schema.h"
#include "schemaless_chunk_reader.h"
#include "unversioned_row.h"
#include "versioned_block_reader.h"
#include "versioned_reader.h"
#include "schemaful_reader_adapter.h"
#include "versioned_reader_adapter.h"

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/block_id.h>
#include <yt/ytlib/chunk_client/cache_reader.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>
#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

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

std::vector<TColumnIdMapping> BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    std::vector<TColumnIdMapping> idMapping;
    idMapping.resize(chunkMeta->SchemaIdMapping().size(), TColumnIdMapping{-1,-1});

    if (columnFilter.All) {
        for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
            YCHECK(mapping.ChunkSchemaIndex < idMapping.size());
            idMapping[mapping.ChunkSchemaIndex].ReaderSchemaIndex = mapping.ReaderSchemaIndex;
        }
    } else {
        for (int index = 0; index < chunkMeta->GetChunkKeyColumnCount(); ++index) {
            idMapping[index].ReaderSchemaIndex = index;
        }

        for (auto index : columnFilter.Indexes) {
            if (index < chunkMeta->GetKeyColumnCount()) {
                continue;
            }

            for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
                if (mapping.ReaderSchemaIndex == index) {
                    YCHECK(mapping.ChunkSchemaIndex < idMapping.size());
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


    virtual TFuture<void> Open() override
    {
        return GetReadyEvent();
    }

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
    , SchemaIdMapping_(BuildVersionedSimpleSchemaIdMapping(columnFilter, ChunkMeta_))
    , MemoryPool_(TVersionedChunkReaderPoolTag())
    , PerformanceCounters_(std::move(performanceCounters))
{
    YCHECK(ChunkMeta_->Misc().sorted());
    YCHECK(ChunkMeta_->GetChunkType() == EChunkType::Table);
    YCHECK(ChunkMeta_->GetChunkFormat() == ETableChunkFormat::VersionedSimple);
    YCHECK(Timestamp_ != AllCommittedTimestamp || columnFilter.All);
    YCHECK(PerformanceCounters_);
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedRangeChunkReader
    : public TVersionedChunkReaderBase
{
public:
    TSimpleVersionedRangeChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        TOwningKey lowerLimit,
        TOwningKey upperLimit,
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

    virtual bool Read(std::vector<TVersionedRow>* rows) override
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
            if (CheckKeyLimit_ && KeyComparer_(BlockReader_->GetKey(), UpperLimit_) >= 0) {
                PerformanceCounters_->StaticChunkRowReadCount += rows->size();
                return !rows->empty();
            }

            auto row = BlockReader_->GetRow(&MemoryPool_);
            if (row) {
                Y_ASSERT(
                    rows->empty() ||
                    !rows->back() ||
                    CompareRows(
                        rows->back().BeginKeys(), rows->back().EndKeys(),
                        row.BeginKeys(), row.EndKeys()) < 0);
            }
            rows->push_back(row);
            ++RowCount_;

            if (!BlockReader_->NextRow()) {
                BlockEnded_ = true;
                break;
            }
        }

        PerformanceCounters_->StaticChunkRowReadCount += rows->size();
        return true;
    }

private:
    int CurrentBlockIndex_ = 0;
    TOwningKey LowerLimit_;
    TOwningKey UpperLimit_;

    std::vector<TBlockFetcher::TBlockInfo> GetBlockSequence()
    {
        const auto& blockMetaExt = ChunkMeta_->BlockMeta();
        const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

        CurrentBlockIndex_ = ApplyLowerKeyLimit(blockIndexKeys, NChunkClient::TReadLimit(LowerLimit_));
        int endBlockIndex = ApplyUpperKeyLimit(blockIndexKeys, NChunkClient::TReadLimit(UpperLimit_));

        std::vector<TBlockFetcher::TBlockInfo> blocks;
        YCHECK(endBlockIndex <= blockMetaExt.blocks_size());

        for (int blockIndex = CurrentBlockIndex_; blockIndex < endBlockIndex; ++blockIndex) {
            auto& blockMeta = blockMetaExt.blocks(blockIndex);
            blocks.push_back(TBlockFetcher::TBlockInfo(blockIndex, blockMeta.uncompressed_size(), blocks.size()));
        }

        return blocks;
    }

    virtual void InitFirstBlock() override
    {
        CheckBlockUpperLimits(
            ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
            NChunkClient::TReadLimit(UpperLimit_),
            ChunkMeta_->GetKeyColumnCount());

        YCHECK(CurrentBlock_ && CurrentBlock_.IsSet());
        BlockReader_.reset(new TSimpleVersionedBlockReader(
            CurrentBlock_.Get().ValueOrThrow(),
            ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_));

        YCHECK(BlockReader_->SkipToKey(LowerLimit_));
    }

    virtual void InitNextBlock() override
    {
        ++CurrentBlockIndex_;

        CheckBlockUpperLimits(
            ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
            NChunkClient::TReadLimit(UpperLimit_),
            ChunkMeta_->GetKeyColumnCount());
        YCHECK(CurrentBlock_ && CurrentBlock_.IsSet());

        BlockReader_.reset(new TSimpleVersionedBlockReader(
            CurrentBlock_.Get().ValueOrThrow(),
            ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedLookupChunkReader
    : public TVersionedChunkReaderBase
{
public:
    TSimpleVersionedLookupChunkReader(
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

    virtual bool Read(std::vector<TVersionedRow>* rows) override
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

private:
    const TSharedRange<TKey> Keys_;
    std::vector<bool> KeyFilterTest_;
    std::vector<int> BlockIndexes_;

    int CurrentBlockIndex_ = -1;

    std::vector<TBlockFetcher::TBlockInfo> GetBlockSequence()
    {
        const auto& blockMetaExt = ChunkMeta_->BlockMeta();
        const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

        std::vector<TBlockFetcher::TBlockInfo> blocks;
        if (Keys_.Empty()) {
            return blocks;
        }

        for (int keyIndex = 0; keyIndex < Keys_.Size(); ++keyIndex) {
            auto& key = Keys_[keyIndex];
#if 0
            //FIXME(savrus): use bloom filter here.
    if (!VersionedChunkMeta_->KeyFilter().Contains(key)) {
        KeyFilterTest_[keyIndex] = false;
        continue;
    }
#endif
            int blockIndex = GetBlockIndexByKey(
                key,
                blockIndexKeys,
                BlockIndexes_.empty() ? 0 : BlockIndexes_.back());

            if (blockIndex == blockIndexKeys.Size()) {
                break;
            }
            if (BlockIndexes_.empty() || BlockIndexes_.back() < blockIndex) {
                BlockIndexes_.push_back(blockIndex);
            }
            YCHECK(blockIndex == BlockIndexes_.back());
            YCHECK(blockIndex < blockIndexKeys.Size());
        }

        for (int blockIndex : BlockIndexes_) {
            auto& blockMeta = blockMetaExt.blocks(blockIndex);
            TBlockFetcher::TBlockInfo blockInfo;
            blockInfo.Index = blockIndex;
            blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
            blockInfo.Priority = blocks.size();
            blocks.push_back(blockInfo);
        }

        return blocks;
    }

    virtual void InitFirstBlock() override
    {
        InitNextBlock();
    }

    virtual void InitNextBlock() override
    {
        ++CurrentBlockIndex_;
        int chunkBlockIndex = BlockIndexes_[CurrentBlockIndex_];
        BlockReader_.reset(new TSimpleVersionedBlockReader(
            CurrentBlock_.Get().ValueOrThrow(),
            ChunkMeta_->BlockMeta().blocks(chunkBlockIndex),
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_));
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
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp)
        : TBase(std::move(config), std::move(underlyingReader), std::move(blockCache))
        , VersionedChunkMeta_(std::move(chunkMeta))
        , Timestamp_(timestamp)
        , SchemaIdMapping_(BuildVersionedSimpleSchemaIdMapping(columnFilter, VersionedChunkMeta_))
        , PerformanceCounters_(std::move(performanceCounters))
    {
        YCHECK(VersionedChunkMeta_->Misc().sorted());
        YCHECK(VersionedChunkMeta_->GetChunkType() == EChunkType::Table);
        YCHECK(VersionedChunkMeta_->GetChunkFormat() == ETableChunkFormat::VersionedColumnar);
        YCHECK(Timestamp_ != AllCommittedTimestamp || columnFilter.All);
        YCHECK(PerformanceCounters_);

        TBase::ChunkMeta_ = VersionedChunkMeta_;

        KeyColumnReaders_.resize(VersionedChunkMeta_->GetKeyColumnCount());
        for (int keyColumnIndex = 0;
             keyColumnIndex < VersionedChunkMeta_->GetChunkKeyColumnCount();
             ++keyColumnIndex)
        {
            auto columnReader = CreateUnversionedColumnReader(
                VersionedChunkMeta_->ChunkSchema().Columns()[keyColumnIndex],
                VersionedChunkMeta_->ColumnMeta().columns(keyColumnIndex),
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
            auto columnReader = CreateUnversionedNullColumnReader(
                keyColumnIndex,
                keyColumnIndex);
            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            TBase::Columns_.emplace_back(std::move(columnReader));
        }

        for (const auto& idMapping : SchemaIdMapping_) {
            auto columnReader = CreateVersionedColumnReader(
                VersionedChunkMeta_->ChunkSchema().Columns()[idMapping.ChunkSchemaIndex],
                VersionedChunkMeta_->ColumnMeta().columns(idMapping.ChunkSchemaIndex),
                idMapping.ReaderSchemaIndex);

            ValueColumnReaders_.push_back(columnReader.get());
            TBase::Columns_.emplace_back(std::move(columnReader), idMapping.ChunkSchemaIndex);
        }
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
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
        YCHECK(TimestampReader_ == nullptr);

        int timestampReaderIndex = ChunkMeta_->ColumnMeta().columns().size() - 1;
        TimestampReader_ = new TScanTransactionTimestampReader(
            ChunkMeta_->ColumnMeta().columns(timestampReaderIndex),
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
            if (columnSchema.Aggregate) {
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
            valueColumnReader->ReadValues(range, timestampIndexRanges);
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
        YCHECK(TimestampReader_ == nullptr);
        int timestampReaderIndex = ChunkMeta_->ColumnMeta().columns().size() - 1;
        TimestampReader_ = new TCompactionTimestampReader(
            ChunkMeta_->ColumnMeta().columns(timestampReaderIndex));

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
        TOwningKey lowerLimit,
        TOwningKey upperLimit,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp)
        : TColumnarVersionedChunkReaderBase(
            std::move(config),
            chunkMeta,
            std::move(underlyingReader),
            std::move(blockCache),
            columnFilter,
            std::move(performanceCounters),
            timestamp)
        , RowBuilder_(chunkMeta, ValueColumnReaders_, SchemaIdMapping_, timestamp)
    {
        LowerLimit_.SetKey(std::move(lowerLimit));
        UpperLimit_.SetKey(std::move(upperLimit));

        int timestampReaderIndex = VersionedChunkMeta_->ColumnMeta().columns().size() - 1;
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
        YCHECK(rows->capacity() > 0);
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
            YCHECK(rowLimit > 0);

            auto range = RowBuilder_.AllocateRows(rows, rowLimit, RowIndex_, SafeUpperRowIndex_);

            // Read key values.
            for (auto& keyColumnReader : KeyColumnReaders_) {
                keyColumnReader->ReadValues(range);
            }

            YCHECK(RowIndex_ + rowLimit <= HardUpperRowIndex_);
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

            PerformanceCounters_->StaticChunkRowReadCount += range.Size();
            RowIndex_ += range.Size();
            if (Completed_ || !TryFetchNextRow()) {
                break;
            }
        }

        return true;
    }

private:
    bool Initialized_ = false;
    bool Completed_ = false;

    i64 MaxRowsPerRead_;
    i64 RowIndex_;

    TRowBuilder RowBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

class TColumnarVersionedLookupChunkReader
    : public TColumnarVersionedChunkReaderBase<TColumnarLookupChunkReaderBase>
{
public:
    TColumnarVersionedLookupChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TSharedRange<TKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp)
        : TColumnarVersionedChunkReaderBase(
            std::move(config),
            std::move(chunkMeta),
            std::move(underlyingReader),
            std::move(blockCache),
            columnFilter,
            std::move(performanceCounters),
            timestamp)
        , Pool_(TVersionedChunkReaderPoolTag())
    {
        Keys_ = keys;

        int timestampReaderIndex = VersionedChunkMeta_->ColumnMeta().columns().size() - 1;
        TimestampReader_ = new TLookupTransactionTimestampReader(
            VersionedChunkMeta_->ColumnMeta().columns(timestampReaderIndex),
            Timestamp_);

        Columns_.emplace_back(std::unique_ptr<IColumnReaderBase>(TimestampReader_), timestampReaderIndex);

        Initialize();

        ReadyEvent_ = RequestFirstBlocks();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        rows->clear();
        Pool_.Clear();

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
                YCHECK(key.GetCount() == VersionedChunkMeta_->GetKeyColumnCount());

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
                    YCHECK(upperRowIndex == lowerRowIndex + 1);
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

        PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
        return true;
    }

private:
    TChunkedMemoryPool Pool_;

    TLookupTransactionTimestampReader* TimestampReader_;

    TMutableVersionedRow ReadRow(i64 rowIndex)
    {
        for (auto& column : Columns_) {
            column.ColumnReader->SkipToRowIndex(rowIndex);
        }

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
            const auto& columnSchema = VersionedChunkMeta_->ChunkSchema().Columns()[idMapping.ChunkSchemaIndex];
            ui32 columnValueCount = 1;
            if (columnSchema.Aggregate) {
                // Possibly multiple values per column for aggregate columns.
                ValueColumnReaders_[valueColumnIndex]->GetValueCounts(TMutableRange<ui32>(&columnValueCount, 1));
            }

            valueCount += columnValueCount;
        }

        // Allocate according to schema.
        auto row = TMutableVersionedRow::Allocate(
            &Pool_,
            VersionedChunkMeta_->GetKeyColumnCount(),
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

        // Read key values.
        for (const auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValues(
                TMutableRange<TMutableVersionedRow>(&row, 1),
                MakeRange(&timestampIndexRange, 1));
        }

        for (int i = 0; i < row.GetValueCount(); ++i) {
            row.BeginValues()[i].Timestamp = TimestampReader_->GetTimestamp(static_cast<i32>(row.BeginValues()[i].Timestamp));
        }

        *row.BeginWriteTimestamps() = TimestampReader_->GetWriteTimestamp();
        return row;
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TOwningKey lowerLimit,
    TOwningKey upperLimit,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
{
    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::VersionedSimple:
            return New<TSimpleVersionedRangeChunkReader>(
                std::move(config),
                std::move(chunkMeta),
                std::move(chunkReader),
                std::move(blockCache),
                std::move(lowerLimit),
                std::move(upperLimit),
                columnFilter,
                std::move(performanceCounters),
                timestamp);

        case ETableChunkFormat::VersionedColumnar:
            if (timestamp == AllCommittedTimestamp) {
                return New<TColumnarVersionedRangeChunkReader<TCompactionColumnarRowBuilder>>(
                    std::move(config),
                    std::move(chunkMeta),
                    std::move(chunkReader),
                    std::move(blockCache),
                    std::move(lowerLimit),
                    std::move(upperLimit),
                    columnFilter,
                    std::move(performanceCounters),
                    timestamp);
            } else {
                return New<TColumnarVersionedRangeChunkReader<TScanColumnarRowBuilder>>(
                    std::move(config),
                    std::move(chunkMeta),
                    std::move(chunkReader),
                    std::move(blockCache),
                    std::move(lowerLimit),
                    std::move(upperLimit),
                    columnFilter,
                    std::move(performanceCounters),
                    timestamp);
            }

        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::SchemalessHorizontal: {
            auto chunkTimestamp = static_cast<TTimestamp>(chunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader();
            }

            auto schemalessReaderFactory = [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                TChunkSpec chunkSpec;
                auto* protoMeta = chunkSpec.mutable_chunk_meta();
                protoMeta->set_type(static_cast<int>(chunkMeta->GetChunkType()));
                protoMeta->set_version(static_cast<int>(chunkMeta->GetChunkFormat()));
                SetProtoExtension(protoMeta->mutable_extensions(), chunkMeta->Misc());
                SetProtoExtension(protoMeta->mutable_extensions(), chunkMeta->BlockMeta());
                SetProtoExtension(protoMeta->mutable_extensions(), chunkMeta->ColumnMeta());
                SetProtoExtension(protoMeta->mutable_extensions(), ToProto<TTableSchemaExt>(chunkMeta->ChunkSchema()));

                auto options = New<TTableReaderOptions>();
                options->DynamicTable = true;

                return CreateSchemalessChunkReader(
                    MakeUnversionedDataSliceDescriptor(std::move(chunkSpec)),
                    config,
                    options,
                    chunkReader,
                    nameTable,
                    blockCache,
                    chunkMeta->Schema().GetKeyColumns(),
                    columnFilter,
                    TReadRange(TReadLimit(lowerLimit), TReadLimit(upperLimit)));
            };
            auto schemafulReaderFactory = [&] (const TTableSchema& schema) {
                return CreateSchemafulReaderAdapter(schemalessReaderFactory, schema);
            };
            return CreateVersionedReaderAdapter(
                schemafulReaderFactory,
                chunkMeta->Schema(),
                chunkTimestamp);
        }
        default:
            Y_UNREACHABLE();
    }
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
    TKeyComparer keyComparer,
    TTimestamp timestamp)
{
    // Lookup doesn't support reading all values.
    YCHECK(timestamp != AllCommittedTimestamp);

    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::VersionedSimple:
            return New<TSimpleVersionedLookupChunkReader>(
                std::move(config),
                std::move(chunkMeta),
                std::move(chunkReader),
                std::move(blockCache),
                keys,
                columnFilter,
                std::move(performanceCounters),
                std::move(keyComparer),
                timestamp);


        case ETableChunkFormat::VersionedColumnar:
            return New<TColumnarVersionedLookupChunkReader>(
                std::move(config),
                std::move(chunkMeta),
                std::move(chunkReader),
                std::move(blockCache),
                keys,
                columnFilter,
                std::move(performanceCounters),
                timestamp);

        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::SchemalessHorizontal: {
            auto chunkTimestamp = static_cast<TTimestamp>(chunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader(keys.Size());
            }

            auto schemalessReaderFactory = [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                TChunkSpec chunkSpec;
                auto* protoMeta = chunkSpec.mutable_chunk_meta();
                protoMeta->set_type(static_cast<int>(chunkMeta->GetChunkType()));
                protoMeta->set_version(static_cast<int>(chunkMeta->GetChunkFormat()));
                SetProtoExtension(protoMeta->mutable_extensions(), chunkMeta->Misc());
                SetProtoExtension(protoMeta->mutable_extensions(), chunkMeta->BlockMeta());
                SetProtoExtension(protoMeta->mutable_extensions(), chunkMeta->ColumnMeta());
                SetProtoExtension(protoMeta->mutable_extensions(), ToProto<TTableSchemaExt>(chunkMeta->ChunkSchema()));

                auto options = New<TTableReaderOptions>();
                options->DynamicTable = true;

                return CreateSchemalessChunkReader(
                    MakeUnversionedDataSliceDescriptor(std::move(chunkSpec)),
                    config,
                    options,
                    chunkReader,
                    nameTable,
                    blockCache,
                    chunkMeta->Schema().GetKeyColumns(),
                    columnFilter,
                    keys);
            };
            auto schemafulReaderFactory = [&] (const TTableSchema& schema) {
                return CreateSchemafulReaderAdapter(schemalessReaderFactory, schema);
            };
            return CreateVersionedReaderAdapter(
                std::move(schemafulReaderFactory),
                chunkMeta->Schema(),
                chunkTimestamp);
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
