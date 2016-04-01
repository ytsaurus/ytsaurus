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
#include <yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/timestamp_reader.h>
#include <yt/ytlib/table_chunk_format/null_column_reader.h>

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

private:
    int CurrentBlockIndex_ = 0;
    i64 CurrentRowIndex_ = 0;
    TReadLimit LowerLimit_;
    TReadLimit UpperLimit_;

    std::vector<TBlockFetcher::TBlockInfo> GetBlockSequence()
    {
        const auto& blockMetaExt = ChunkMeta_->BlockMeta();
        const auto& blockIndexKeys = ChunkMeta_->BlockLastKeys();

        CurrentBlockIndex_ = std::max(
            ApplyLowerRowLimit(blockMetaExt, LowerLimit_),
            ApplyLowerKeyLimit(blockIndexKeys, LowerLimit_));
        int endBlockIndex = std::min(
            ApplyUpperRowLimit(blockMetaExt, UpperLimit_),
            ApplyUpperKeyLimit(blockIndexKeys, UpperLimit_));

        std::vector<TBlockFetcher::TBlockInfo> blocks;
        if (CurrentBlockIndex_ >= blockMetaExt.blocks_size()) {
            return blocks;
        }

        auto& blockMeta = blockMetaExt.blocks(CurrentBlockIndex_);
        CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

        for (int blockIndex = CurrentBlockIndex_; blockIndex < endBlockIndex; ++blockIndex) {
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
        CheckBlockUpperLimits(
            ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
            UpperLimit_,
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

        if (LowerLimit_.HasRowIndex() && CurrentRowIndex_ < LowerLimit_.GetRowIndex()) {
            YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
            CurrentRowIndex_ = LowerLimit_.GetRowIndex();
        }

        if (LowerLimit_.HasKey()) {
            auto blockRowIndex = BlockReader_->GetRowIndex();
            YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey()));
            CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
        }
    }

    virtual void InitNextBlock() override
    {
        ++CurrentBlockIndex_;

        CheckBlockUpperLimits(
            ChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
            UpperLimit_,
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

class TColumnarVersionedChunkReaderBase
    : public IVersionedReader
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
        : Config_(std::move(config))
        , ChunkMeta_(std::move(chunkMeta))
        , UnderlyingReader_(std::move(underlyingReader))
        , BlockCache_(std::move(blockCache))
        , Timestamp_(timestamp)
        , SchemaIdMapping_(BuildSchemaIdMapping(columnFilter, ChunkMeta_))
        , Semaphore_(New<TAsyncSemaphore>(Config_->WindowSize))
        , PerformanceCounters_(std::move(performanceCounters))
    {
        YCHECK(ChunkMeta_->Misc().sorted());
        YCHECK(EChunkType(ChunkMeta_->ChunkMeta().type()) == EChunkType::Table);
        YCHECK(ETableChunkFormat(ChunkMeta_->ChunkMeta().version()) == ETableChunkFormat::VersionedColumnar);
        YCHECK(Timestamp_ != AllCommittedTimestamp || columnFilter.All);
        YCHECK(PerformanceCounters_);

        KeyColumnReaders_.resize(ChunkMeta_->GetKeyColumnCount());
        for (int keyColumnIndex = 0;
             keyColumnIndex < ChunkMeta_->GetChunkKeyColumnCount();
             ++keyColumnIndex)
        {
            auto columnReader = CreateUnversionedColumnReader(
                ChunkMeta_->ChunkSchema().Columns()[keyColumnIndex],
                ChunkMeta_->ColumnMeta().columns(keyColumnIndex),
                keyColumnIndex,
                keyColumnIndex);

            Columns_.emplace_back(columnReader.get(), keyColumnIndex);
            KeyColumnReaders_[keyColumnIndex].swap(columnReader);
        }

        // Null readers for wider keys.
        for (int keyColumnIndex = ChunkMeta_->GetChunkKeyColumnCount();
             keyColumnIndex < KeyColumnReaders_.size();
             ++keyColumnIndex)
        {
            KeyColumnReaders_[keyColumnIndex] = CreateUnversionedNullColumnReader(
                keyColumnIndex,
                keyColumnIndex);
        }

        for (const auto& idMapping : ChunkMeta_->SchemaIdMapping()) {
            auto columnReader = CreateVersionedColumnReader(
                ChunkMeta_->ChunkSchema().Columns()[idMapping.ChunkSchemaIndex],
                ChunkMeta_->ColumnMeta().columns(idMapping.ChunkSchemaIndex),
                idMapping.ReaderSchemaIndex);

            Columns_.emplace_back(columnReader.get(), idMapping.ChunkSchemaIndex);

            ValueColumnReaders_.push_back(std::move(columnReader));
        }
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        if (!BlockFetcher_) {
            return TDataStatistics();
        }

        TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_uncompressed_data_size(BlockFetcher_->GetUncompressedDataSize());
        dataStatistics.set_compressed_data_size(BlockFetcher_->GetCompressedDataSize());
        return dataStatistics;
    }

    virtual bool IsFetchingCompleted() const override
    {
        return BlockFetcher_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YUNIMPLEMENTED();
    }

    virtual TFuture<void> GetReadyEvent()
    {
        return ReadyEvent_;
    }

    virtual TFuture<void> Open() override
    {
        return VoidFuture;
    }

protected:
    TChunkReaderConfigPtr Config_;
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    IChunkReaderPtr UnderlyingReader_;
    IBlockCachePtr BlockCache_;

    const TTimestamp Timestamp_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

    i64 RowCount_ = 0;

    TAsyncSemaphorePtr Semaphore_;
    TBlockFetcherPtr BlockFetcher_;

    TChunkReaderPerformanceCountersPtr PerformanceCounters_;

    struct TColumn
    {
        TColumn(IColumnReaderBase* reader, int chunkSchemaIndex)
            : ColumnReader(reader)
            , ChunkSchemaIndex(chunkSchemaIndex)
        { }

        IColumnReaderBase* ColumnReader;
        int ChunkSchemaIndex;
        std::vector<int> BlockIndexSequence;
        int PendingBlockIndex_ = 0;
    };

    std::vector<std::unique_ptr<IUnversionedColumnReader>> KeyColumnReaders_;
    std::vector<std::unique_ptr<IVersionedColumnReader>> ValueColumnReaders_;

    std::vector<TColumn> Columns_;

    TFuture<void> ReadyEvent_ = VoidFuture;

    std::vector<TFuture<TSharedRef>> RequestedBlocks_;

    void ResetExaustedColumns()
    {
        for (int i = 0; i < RequestedBlocks_.size(); ++i) {
            if (RequestedBlocks_[i]) {
                YCHECK(RequestedBlocks_[i].IsSet());
                Columns_[i].ColumnReader->ResetBlock(
                    RequestedBlocks_[i].Get().Value(),
                    Columns_[i].PendingBlockIndex_);
            }
        }

        RequestedBlocks_.clear();
    }

    i64 GetLowerRowIndex(const TKey& key) const
    {
        auto lower = std::lower_bound(
            ChunkMeta_->BlockLastKeys().begin(),
            ChunkMeta_->BlockLastKeys().end(),
            key);

        if (lower == ChunkMeta_->BlockLastKeys().end()) {
            return ChunkMeta_->Misc().row_count();
        }

        auto upper = std::upper_bound(
            lower,
            ChunkMeta_->BlockLastKeys().end(),
            key);

        i64 rowIndex = 0;
        for (auto it = lower; it != upper + 1; ++it) {
            int blockIndex = std::distance(ChunkMeta_->BlockLastKeys().begin(), it);
            const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(blockIndex);
            rowIndex = std::max(rowIndex, blockMeta.chunk_row_count() - blockMeta.row_count());
        }
        return rowIndex;
    }

    i64 GetSegmentIndex(const TColumn& column, i64 rowIndex) const
    {
        const auto& columnMeta = ChunkMeta_->ColumnMeta().columns(column.ChunkSchemaIndex);
        auto it = std::lower_bound(
            columnMeta.segments().begin(),
            columnMeta.segments().end(),
            rowIndex,
            [](const TSegmentMeta& segmentMeta, i64 index) {
                return segmentMeta.chunk_row_count() < index + 1;
            });
        return std::distance(columnMeta.segments().begin(), it);
    }

    TBlockFetcher::TBlockInfo CreateBlockInfo(int blockIndex) const
    {
        const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(blockIndex);
        TBlockFetcher::TBlockInfo blockInfo;
        blockInfo.Index = blockIndex;
        blockInfo.Priority = blockMeta.chunk_row_count() - blockMeta.row_count();
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        return blockInfo;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TScanColumnarRowBuilder
{
public:
    TScanColumnarRowBuilder(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<std::unique_ptr<IVersionedColumnReader>>& valueColumnReaders,
        TTimestamp timestamp)
        : ChunkMeta_(chunkMeta)
        , ValueColumnReaders_(valueColumnReaders)
        , Pool_(TVersionedChunkReaderPoolTag())
    {
        int timestampReaderIndex = ChunkMeta_->ColumnMeta().columns().size() - 1;
        TimestampReader_ = std::make_unique<TScanTransactionTimestampReader>(
            ChunkMeta_->ColumnMeta().columns(timestampReaderIndex),
            timestamp);
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
        for (int valueColumnIndex = 0; valueColumnIndex < ChunkMeta_->SchemaIdMapping().size(); ++valueColumnIndex) {
            const auto& idMapping = ChunkMeta_->SchemaIdMapping()[valueColumnIndex];
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

    IColumnReaderBase* GetTimestampReader() const
    {
        return TimestampReader_.get();
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
    std::unique_ptr<TScanTransactionTimestampReader> TimestampReader_;

    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    std::vector<std::unique_ptr<IVersionedColumnReader>>& ValueColumnReaders_;

    TChunkedMemoryPool Pool_;
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionColumnarRowBuilder
{
public:
    TCompactionColumnarRowBuilder(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<std::unique_ptr<IVersionedColumnReader>>& valueColumnReaders,
        TTimestamp /* timestamp */)
        : ChunkMeta_(chunkMeta)
        , ValueColumnReaders_(valueColumnReaders)
        , Pool_(TVersionedChunkReaderPoolTag())
    {
        int timestampReaderIndex = ChunkMeta_->ColumnMeta().columns().size() - 1;
        TimestampReader_ = std::make_unique<TCompactionTimestampReader>(
            ChunkMeta_->ColumnMeta().columns(timestampReaderIndex));
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

    IColumnReaderBase* GetTimestampReader() const
    {
        return TimestampReader_.get();
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
    std::unique_ptr<TCompactionTimestampReader> TimestampReader_;

    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    std::vector<std::unique_ptr<IVersionedColumnReader>>& ValueColumnReaders_;

    TChunkedMemoryPool Pool_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowBuilder>
class TColumnarVersionedRangeChunkReader
    : public TColumnarVersionedChunkReaderBase
{
public:
    TColumnarVersionedRangeChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        TReadLimit lowerLimit,
        TReadLimit upperLimit,
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
        , LowerLimit_(std::move(lowerLimit))
        , UpperLimit_(std::move(upperLimit))
        , RowBuilder_(chunkMeta, ValueColumnReaders_, timestamp)
    {
        int timestampReaderIndex = ChunkMeta_->ColumnMeta().columns().size() - 1;
        Columns_.emplace_back(RowBuilder_.GetTimestampReader(), timestampReaderIndex);

        InitLowerRowIndex();
        InitUpperRowIndex();

        if (LowerRowIndex_ < ChunkMeta_->Misc().row_count()) {
            InitBlockFetcher();
            RequestFirstBlocks();
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

        if (!ReadyEvent_.IsSet()) {
            return true;
        }

        if (!Initialized_) {
            ResetExaustedColumns();
            Initialize();
            Initialized_ = true;
        }

        if (Completed_) {
            return false;
        }

        while (rows->size() < rows->capacity()) {
            ResetExaustedColumns();

            // Define how many to read.
            i64 rowLimit = std::min(HardUpperRowIndex_ - RowIndex_, static_cast<i64>(rows->capacity()));
            for (const auto& column : Columns_) {
                rowLimit = std::min(column.ColumnReader->GetReadyUpperRowIndex() - RowIndex_, rowLimit);
            }
            YCHECK(rowLimit > 0);
            
            auto range = RowBuilder_.AllocateRows(rows, rowLimit, RowIndex_, SafeUpperRowIndex_);

            // Read key values.
            for (auto& keyColumnReader : KeyColumnReaders_) {
                keyColumnReader->ReadValues(range);
            }

            if (RowIndex_ + rowLimit > SafeUpperRowIndex_) {
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
            } else if (RowIndex_ + rowLimit == HardUpperRowIndex_) {
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

    virtual TFuture<void> GetReadyEvent()
    {
        return ReadyEvent_;
    }

private:
    const TReadLimit LowerLimit_;
    const TReadLimit UpperLimit_;

    i64 LowerRowIndex_;
    i64 SafeUpperRowIndex_;
    i64 HardUpperRowIndex_;

    i64 RowIndex_;

    bool Initialized_ = false;
    bool Completed_ = false;

    TRowBuilder RowBuilder_;

    void InitLowerRowIndex()
    {
        LowerRowIndex_ = 0;
        if (LowerLimit_.HasRowIndex()) {
            LowerRowIndex_ = std::max(LowerRowIndex_, LowerLimit_.GetRowIndex());
        }

        if (LowerLimit_.HasKey()) {
            LowerRowIndex_ = std::max(LowerRowIndex_, GetLowerRowIndex(LowerLimit_.GetKey()));
        }
    }

    void InitUpperRowIndex()
    {
        SafeUpperRowIndex_ = HardUpperRowIndex_ = ChunkMeta_->Misc().row_count();
        if (UpperLimit_.HasRowIndex()) {
            SafeUpperRowIndex_ = HardUpperRowIndex_ = std::min(HardUpperRowIndex_, UpperLimit_.GetRowIndex());
        }

        if (UpperLimit_.HasKey()) {
            auto it = std::lower_bound(
                ChunkMeta_->BlockLastKeys().begin(),
                ChunkMeta_->BlockLastKeys().end(),
                UpperLimit_.GetKey());


            if (it == ChunkMeta_->BlockLastKeys().end()) {
                SafeUpperRowIndex_ = HardUpperRowIndex_ = ChunkMeta_->Misc().row_count();
            } else {
                int blockIndex = std::distance(ChunkMeta_->BlockLastKeys().begin(), it);
                const auto& blockMeta = ChunkMeta_->BlockMeta().blocks(blockIndex);
                SafeUpperRowIndex_ = std::min(
                    SafeUpperRowIndex_,
                    blockMeta.chunk_row_count() - blockMeta.row_count() - 1);
                HardUpperRowIndex_ = std::min(
                    HardUpperRowIndex_,
                    blockMeta.chunk_row_count());
            }
        }
    }

    void Initialize()
    {
        for (auto& column : Columns_) {
            column.ColumnReader->SkipToRowIndex(LowerRowIndex_);
        }

        RowIndex_ = LowerRowIndex_;

        if (!LowerLimit_.HasKey()) {
            return;
        }

        i64 lowerRowIndex = KeyColumnReaders_[0]->GetCurrentRowIndex();
        i64 upperRowIndex = KeyColumnReaders_[0]->GetBlockUpperRowIndex();
        for (int i = 0; i < LowerLimit_.GetKey().GetCount(); ++i) {
            std::tie(lowerRowIndex, upperRowIndex) = KeyColumnReaders_[i]->GetEqualRange(
                LowerLimit_.GetKey().Begin()[i],
                lowerRowIndex,
                upperRowIndex);
        }

        RowIndex_ = lowerRowIndex;
        YCHECK(RowIndex_ < ChunkMeta_->Misc().row_count());
        for (auto& column : Columns_) {
            column.ColumnReader->SkipToRowIndex(RowIndex_);
        }
    }

    void InitBlockFetcher()
    {
        YCHECK(LowerRowIndex_ < ChunkMeta_->Misc().row_count());

        std::vector<TBlockFetcher::TBlockInfo> blockInfos;

        for (auto& column : Columns_) {
            const auto& columnMeta = ChunkMeta_->ColumnMeta().columns(column.ChunkSchemaIndex);
            i64 segmentIndex = GetSegmentIndex(column, LowerRowIndex_);
            column.BlockIndexSequence.push_back(columnMeta.segments(segmentIndex).block_index());

            int lastBlockIndex = -1;
            for (; segmentIndex < columnMeta.segments_size(); ++segmentIndex) {
                const auto& segment = columnMeta.segments(segmentIndex);
                if (segment.block_index() != lastBlockIndex) {
                    lastBlockIndex = segment.block_index();
                    blockInfos.push_back(CreateBlockInfo(lastBlockIndex));
                }
                if (segment.chunk_row_count() > HardUpperRowIndex_) {
                    break;
                }
            }
        }

        BlockFetcher_ = New<TBlockFetcher>(
            Config_,
            std::move(blockInfos),
            Semaphore_,
            UnderlyingReader_,
            BlockCache_,
            NCompression::ECodec(ChunkMeta_->Misc().compression_codec()));
    }

    void RequestFirstBlocks()
    {
        std::vector<TFuture<void>> unfetched;
        RequestedBlocks_.clear();
        for (auto& column : Columns_) {
            RequestedBlocks_.push_back(BlockFetcher_->FetchBlock(column.BlockIndexSequence.front()));
            column.PendingBlockIndex_ = column.BlockIndexSequence.front();
            if (!RequestedBlocks_.back().IsSet()) {
                unfetched.push_back(RequestedBlocks_.back().template As<void>());
            }
        }

        if (!unfetched.empty()) {
            ReadyEvent_ = Combine(unfetched);
        }
    }

    bool TryFetchNextRow()
    {
        std::vector<TFuture<void>> unfetched;
        YASSERT(RequestedBlocks_.empty());
        for (int i = 0; i < Columns_.size(); ++i) {
            auto& column = Columns_[i];
            if (column.ColumnReader->GetCurrentRowIndex() == column.ColumnReader->GetBlockUpperRowIndex()) {
                while (RequestedBlocks_.size() < i) {
                    RequestedBlocks_.emplace_back();
                }

                auto nextBlockIndex = column.ColumnReader->GetNextBlockIndex();
                YCHECK(nextBlockIndex);
                column.PendingBlockIndex_ = *nextBlockIndex;
                RequestedBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex_));
                if (!RequestedBlocks_.back().IsSet()) {
                    unfetched.push_back(RequestedBlocks_.back().template As<void>());
                }
            }
        }

        if (!unfetched.empty()) {
            ReadyEvent_ = Combine(unfetched);
        }

        return RequestedBlocks_.empty();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TColumnarVersionedLookupChunkReader
    : public TColumnarVersionedChunkReaderBase
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
        , Keys_(keys)
        , Pool_(TVersionedChunkReaderPoolTag())
    {
        int timestampReaderIndex = ChunkMeta_->ColumnMeta().columns().size() - 1;
        TimestampReader_ = std::make_unique<TLookupTransactionTimestampReader>(
            ChunkMeta_->ColumnMeta().columns(timestampReaderIndex),
            Timestamp_);

        Columns_.emplace_back(TimestampReader_.get(), timestampReaderIndex);

        RowIndexes_.reserve(Keys_.Size());
        for (const auto& key : Keys_) {
            RowIndexes_.push_back(GetLowerRowIndex(key));
        }

        for (auto& column : Columns_) {
            for (auto rowIndex : RowIndexes_) {
                if (rowIndex < ChunkMeta_->Misc().row_count()) {
                    const auto& columnMeta = ChunkMeta_->ColumnMeta().columns(column.ChunkSchemaIndex);
                    auto segmentIndex = GetSegmentIndex(column, rowIndex);
                    const auto& segment = columnMeta.segments(segmentIndex);
                    column.BlockIndexSequence.push_back(segment.block_index());
                } else {
                    // All keys left are outside boundary keys.
                    break;
                }

            }
        }

        InitBlockFetcher();
        TryFetchNextRow();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        rows->clear();
        Pool_.Clear();

        if (!ReadyEvent_.IsSet()) {
            return true;
        }

        if (NextKeyIndex_ == Keys_.Size()) {
            return false;
        }

        while (rows->size() < rows->capacity()) {
            ResetExaustedColumns();

            if (RowIndexes_[NextKeyIndex_] < ChunkMeta_->Misc().row_count()) {
                const auto& key = Keys_[NextKeyIndex_];
                YCHECK(key.GetCount() == ChunkMeta_->GetKeyColumnCount());

                // Reading row.
                i64 lowerRowIndex = KeyColumnReaders_[0]->GetCurrentRowIndex();
                i64 upperRowIndex = KeyColumnReaders_[0]->GetBlockUpperRowIndex();
                for (int i = 0; i < ChunkMeta_->GetKeyColumnCount(); ++i) {
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

            ++PerformanceCounters_->StaticChunkRowLookupTrueNegativeCount;

            if (++NextKeyIndex_ == Keys_.Size() || !TryFetchNextRow()) {
                break;
            }
        }

        PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
        return true;
    }

private:
    const TSharedRange<TKey> Keys_;
    std::vector<i64> RowIndexes_;

    i64 NextKeyIndex_ = 0;

    TChunkedMemoryPool Pool_;

    std::unique_ptr<TLookupTransactionTimestampReader> TimestampReader_;

    void InitBlockFetcher()
    {
        std::vector<TBlockFetcher::TBlockInfo> blockInfos;
        for (auto& column : Columns_) {
            int lastBlockIndex = -1;
            for (auto blockIndex : column.BlockIndexSequence) {
                if (blockIndex != lastBlockIndex) {
                    lastBlockIndex = blockIndex;
                    blockInfos.push_back(CreateBlockInfo(lastBlockIndex));
                }
            }
        }

        if (blockInfos.empty()) {
            return;
        }

        BlockFetcher_ = New<TBlockFetcher>(
            Config_,
            std::move(blockInfos),
            Semaphore_,
            UnderlyingReader_,
            BlockCache_,
            NCompression::ECodec(ChunkMeta_->Misc().compression_codec()));
    }

    bool TryFetchNextRow()
    {
        if (RowIndexes_[NextKeyIndex_] >= ChunkMeta_->Misc().row_count()) {
            return true;
        }

        std::vector<TFuture<void>> unfetched;
        RequestedBlocks_.clear();
        for (int i = 0; i < Columns_.size(); ++i) {
            auto& column = Columns_[i];
            if (column.ColumnReader->GetCurrentBlockIndex() != column.BlockIndexSequence[NextKeyIndex_]) {
                while (RequestedBlocks_.size() < i) {
                    RequestedBlocks_.emplace_back();
                }

                column.PendingBlockIndex_ = column.BlockIndexSequence[NextKeyIndex_];
                RequestedBlocks_.push_back(BlockFetcher_->FetchBlock(column.PendingBlockIndex_));
                if (!RequestedBlocks_.back().IsSet()) {
                    unfetched.push_back(RequestedBlocks_.back().As<void>());
                }
            }
        }

        if (!unfetched.empty()) {
            ReadyEvent_ = Combine(unfetched);
        }

        return RequestedBlocks_.empty();
    }

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
        for (int valueColumnIndex = 0; valueColumnIndex < ChunkMeta_->SchemaIdMapping().size(); ++valueColumnIndex) {
            const auto& idMapping = ChunkMeta_->SchemaIdMapping()[valueColumnIndex];
            const auto& columnSchema = ChunkMeta_->ChunkSchema().Columns()[idMapping.ChunkSchemaIndex];
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
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp)
{
    auto formatVersion = ETableChunkFormat(chunkMeta->ChunkMeta().version());
    switch (formatVersion) {
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

        default:
            YUNREACHABLE();
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

    auto formatVersion = ETableChunkFormat(chunkMeta->ChunkMeta().version());
    switch (formatVersion) {
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

        default:
            YUNREACHABLE();
    }
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

class TCacheBasedSimpleVersionedLookupChunkReader
    : public TCacheBasedVersionedChunkReaderBase
{
public:
    TCacheBasedSimpleVersionedLookupChunkReader(
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

                TSimpleVersionedBlockReader blockReader(
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

        TSimpleVersionedBlockReader blockReader(
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
    return New<TCacheBasedSimpleVersionedLookupChunkReader>(
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

class TSimpleCacheBasedVersionedRangeChunkReader
    : public TCacheBasedVersionedChunkReaderBase
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
    std::unique_ptr<TSimpleVersionedBlockReader> BlockReader_;
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

        BlockReader_ = std::make_unique<TSimpleVersionedBlockReader>(
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
    switch (ETableChunkFormat(chunkMeta->ChunkMeta().version())) {
        case ETableChunkFormat::VersionedSimple:
            return New<TSimpleCacheBasedVersionedRangeChunkReader>(
                std::move(chunkMeta),
                std::move(blockCache),
                std::move(lowerBound),
                std::move(upperBound),
                columnFilter,
                std::move(performanceCounters),
                timestamp);

        case ETableChunkFormat::VersionedColumnar:
            YUNIMPLEMENTED();

        default:
            YUNREACHABLE();
    }

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
