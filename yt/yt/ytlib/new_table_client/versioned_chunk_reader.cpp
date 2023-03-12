#include "versioned_chunk_reader.h"
#include "column_block_manager.h"
#include "segment_readers.h"
#include "read_span_refiner.h"
#include "rowset_builder.h"
#include "dispatch_by_type.h"
#include "prepared_meta.h"

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_column_mapping.h>
#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/private.h>

#include <yt/yt/core/misc/tls_guard.h>


#include <yt/yt/ytlib/table_client/chunk_lookup_hash_table.h>

namespace NYT::NNewTableClient {

using NProfiling::TCpuDurationIncrementingGuard;

using NTableClient::TColumnFilter;

using NTableClient::IVersionedReader;
using NTableClient::IVersionedReaderPtr;
using NTableClient::TVersionedRow;
using NTableClient::TChunkReaderPerformanceCountersPtr;

using NChunkClient::TCodecStatistics;
using NChunkClient::TChunkId;
using NChunkClient::NProto::TDataStatistics;

using NTableClient::IVersionedRowBatchPtr;
using NTableClient::TRowBatchReadOptions;
using NTableClient::CreateEmptyVersionedRowBatch;

using NTableClient::TTableSchemaPtr;

using NTableClient::TChunkColumnMapping;
using NTableClient::TChunkColumnMappingPtr;

static const auto& Logger = NTableClient::TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

std::vector<EValueType> GetKeyTypes(const TTableSchemaPtr& tableSchema)
{
    auto keyColumnCount = tableSchema->GetKeyColumnCount();
    const auto& schemaColumns = tableSchema->Columns();

    std::vector<EValueType> keyTypes;
    keyTypes.reserve(keyColumnCount);
    for (int keyColumnIndex = 0; keyColumnIndex < keyColumnCount; ++keyColumnIndex) {
        auto type = schemaColumns[keyColumnIndex].GetWireType();
        keyTypes.push_back(type);
    }

    return keyTypes;
}

std::vector<TValueSchema> GetValuesSchema(
    const TTableSchemaPtr& tableSchema,
    TRange<TColumnIdMapping> valueIdMapping)
{
    std::vector<TValueSchema> valueSchema;
    valueSchema.reserve(std::ssize(valueIdMapping));

    const auto& schemaColumns = tableSchema->Columns();
    for (const auto& idMapping : valueIdMapping) {
        auto type = schemaColumns[idMapping.ReaderSchemaIndex].GetWireType();
        valueSchema.push_back(TValueSchema{
            type,
            ui16(idMapping.ReaderSchemaIndex),
            schemaColumns[idMapping.ReaderSchemaIndex].Aggregate().has_value()});
    }

    return valueSchema;
}

///////////////////////////////////////////////////////////////////////////////

// Rows (and theirs indexes) in chunk are partitioned by block borders.
// Block borders are block last keys and corresponding block last indexes (block.chunk_row_count).
// Window is the range of row indexes between block borders.
// TBlockWindowManager updates blocks in column block holders for each window (range of row indexes between block).

// Need to build all read windows at once to prepare block indexes for block fetcher.
template <class TItem, class TPredicate>
std::vector<TSpanMatching> DoBuildReadWindows(
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TRange<TItem> items,
    TPredicate pred)
{
    const auto& blockLastKeys = chunkMeta->BlockLastKeys();
    const auto& blockMeta = chunkMeta->DataBlockMeta();

    std::vector<TSpanMatching> readWindows;

    using NQueryClient::GroupItemsByShards;

    // Block last keys may repeat.

    // TODO(lukyan): Fix repeated block last keys other way.
    ui32 checkLastLimit = 0;

    GroupItemsByShards(items, blockLastKeys, pred, [&] (
        const TLegacyKey* shardIt,
        const TItem* itemIt,
        const TItem* itemItEnd)
    {
        if (shardIt == blockLastKeys.end()) {
            auto chunkRowCount = chunkMeta->Misc().row_count();
            TReadSpan controlSpan(itemIt - items.begin(), itemItEnd - items.begin());
            // Add window for ranges after chunk end bound. It will be used go generate sentinel rows for lookup.
            readWindows.emplace_back(TReadSpan(chunkRowCount, chunkRowCount), controlSpan);
            return;
        }

        auto shardIndex = shardIt - blockLastKeys.begin();
        auto upperRowLimit = blockMeta->data_blocks(shardIndex).chunk_row_count();

        // TODO(lukyan): Rewrite calculation of start index.
        while (shardIndex > 0 && blockMeta->data_blocks(shardIndex - 1).chunk_row_count() == upperRowLimit) {
            --shardIndex;
        }

        ui32 startIndex = shardIndex > 0 ? blockMeta->data_blocks(shardIndex - 1).chunk_row_count() : 0;

        if (startIndex < checkLastLimit) {
            return;
        }

        YT_VERIFY(itemIt != itemItEnd);

        TReadSpan controlSpan(itemIt - items.begin(), itemItEnd - items.begin());
        readWindows.emplace_back(TReadSpan(startIndex, upperRowLimit), controlSpan);

        checkLastLimit = upperRowLimit;
    });

    return readWindows;
}

std::vector<TSpanMatching> BuildReadWindows(
    TRange<TRowRange> keyRanges,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    int keyColumnCount)
{
    struct TPredicate
    {
        const int CommonKeyPrefix;
        const int KeyColumnCount;

        bool operator() (const TRowRange* itemIt, const TLegacyKey* shardIt) const
        {
            return !TestKeyWithWidening(
                ToKeyRef(*shardIt, CommonKeyPrefix),
                ToKeyBoundRef(itemIt->second, true, KeyColumnCount));
        }

        bool operator() (const TLegacyKey* shardIt, const TRowRange* itemIt) const
        {
            return !TestKeyWithWidening(
                ToKeyRef(*shardIt, CommonKeyPrefix),
                ToKeyBoundRef(itemIt->first, false, KeyColumnCount));
        }
    };

    for (auto rowRange : keyRanges) {
        YT_VERIFY(rowRange.first <= rowRange.second);
    }

    return DoBuildReadWindows(
        chunkMeta,
        keyRanges,
        TPredicate{chunkMeta->GetChunkKeyColumnCount(), keyColumnCount});
}

std::vector<TSpanMatching> BuildReadWindows(
    TRange<TLegacyKey> keys,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    int /*keyColumnCount*/)
{
    // Strong typedef.
    struct TItem
        : public TLegacyKey
    {
        using TLegacyKey::TLegacyKey;
    };

    struct TPredicate
    {
        const int CommonKeyPrefix;

        bool operator() (const TItem* itemIt, const TLegacyKey* shardIt) const
        {
            return CompareWithWidening(
                ToKeyRef(*shardIt, CommonKeyPrefix),
                ToKeyRef(*itemIt)) >= 0;
        }

        bool operator() (const TLegacyKey* shardIt, const TItem* itemIt) const
        {
            return CompareWithWidening(
                ToKeyRef(*shardIt, CommonKeyPrefix),
                ToKeyRef(*itemIt)) < 0;
        }
    };

    return DoBuildReadWindows(
        chunkMeta,
        MakeRange(static_cast<const TItem*>(keys.begin()), keys.size()),
        TPredicate{chunkMeta->GetChunkKeyColumnCount()});
}

std::vector<TSpanMatching> BuildReadWindows(
    TRange<ui32> chunkRowIndexes,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    int /*keyColumnCount*/)
{
    const auto& blockMeta = chunkMeta->DataBlockMeta();

    std::vector<TSpanMatching> readWindows;

    size_t blockIndex = 0;

    size_t dataBlocksSize = blockMeta->data_blocks_size();

    auto it = chunkRowIndexes.Begin();
    auto startIt = it;

    while (it != chunkRowIndexes.End() && *it == SentinelRowIndex) {
        ++it;
    }

    while (it != chunkRowIndexes.End()) {
        auto currentChunkRowIndex = *it;

        blockIndex = ExponentialSearch(blockIndex, dataBlocksSize, [&] (size_t index) {
            return blockMeta->data_blocks(index).chunk_row_count() <= currentChunkRowIndex;
        });

        auto upperRowLimit = blockMeta->data_blocks(blockIndex).chunk_row_count();

        auto i = blockIndex;
        while (i > 0 && blockMeta->data_blocks(i - 1).chunk_row_count() == upperRowLimit) {
            --i;
        }

        ui32 startIndex = i > 0 ? blockMeta->data_blocks(i - 1).chunk_row_count() : 0;


        while (it != chunkRowIndexes.End() && (*it == SentinelRowIndex || *it < upperRowLimit)) {
            ++it;
        }

        TReadSpan controlSpan(startIt - chunkRowIndexes.begin(), it - chunkRowIndexes.begin());
        readWindows.emplace_back(TReadSpan(startIndex, upperRowLimit), controlSpan);

        startIt = it;
    }

    if (startIt != it) {
        auto chunkRowCount = chunkMeta->Misc().row_count();
        TReadSpan controlSpan(startIt - chunkRowIndexes.begin(), it - chunkRowIndexes.begin());
        // Add window for ranges after chunk end bound. It will be used go generate sentinel rows for lookup.
        readWindows.emplace_back(TReadSpan(chunkRowCount, chunkRowCount), controlSpan);
    }

    return readWindows;
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkReader
{
public:
    TVersionedChunkReader(
        std::unique_ptr<IBlockManager> blockManager,
        std::unique_ptr<IRowsetBuilder> rowsetBuilder,
        std::vector<TSpanMatching>&& windowsList,
        TReaderStatisticsPtr readerStatistics)
        : BlockManager_(std::move(blockManager))
        , ReaderStatistics_(std::move(readerStatistics))
        , RowsetBuilder_(std::move(rowsetBuilder))
        , WindowsList_(std::move(windowsList))
    {
        std::reverse(WindowsList_.begin(), WindowsList_.end());
    }

    bool ReadRows(std::vector<TMutableVersionedRow>* rows, ui32 readCount, ui64* dataWeigth)
    {
        TCpuDurationIncrementingGuard timingGuard(&ReaderStatistics_->ReadTime);

        YT_VERIFY(rows->empty());
        rows->assign(readCount, TMutableVersionedRow());

        BlockManager_->ClearUsedBlocks();
        RowsetBuilder_->ClearBuffer();
        ui32 offset = 0;

        while (offset < readCount) {
            if (!RowsetBuilder_->IsReadListEmpty()) {
                // Read rows within window.
                offset += RowsetBuilder_->ReadRowsByList(
                    rows->data() + offset,
                    readCount - offset,
                    dataWeigth,
                    ReaderStatistics_.Get());
                // Segment limit reached, readCount reached, or read list exhausted.
            } else if (WindowsList_.empty()) {
                rows->resize(offset);
                ReaderStatistics_->RowCount += rows->size();
                return false;
            } else if (!UpdateWindow()) {
                break;
            }
        }

        rows->resize(offset);
        ReaderStatistics_->RowCount += rows->size();
        return true;
    }

    TChunkedMemoryPool* GetPool()
    {
        return RowsetBuilder_->GetPool();
    }

protected:
    const std::unique_ptr<IBlockManager> BlockManager_;
    const TReaderStatisticsPtr ReaderStatistics_;

private:
    std::unique_ptr<IRowsetBuilder> RowsetBuilder_;
    TTmpBuffers LocalBuffers_;

    // Vector is used as a stack. No need to keep ReadRangeIndex and RowIndex.
    // Also use stack for WindowsList and do not keep NextWindowIndex.
    std::vector<TSpanMatching> WindowsList_;

    // Returns false if need wait for ready event.
    bool UpdateWindow()
    {
        YT_VERIFY(!WindowsList_.empty());

        if (!BlockManager_->TryUpdateWindow(WindowsList_.back().Chunk.Lower, ReaderStatistics_.Get())) {
            return false;
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&ReaderStatistics_->BuildRangesTime);
            YT_VERIFY(RowsetBuilder_->IsReadListEmpty());

            // Update read list.
            RowsetBuilder_->BuildReadListForWindow(WindowsList_.back());
            WindowsList_.pop_back();
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TReaderStatistics& statistics)
{
    auto ticksInSecond = DurationToCpuDuration(TDuration::Seconds(1));
    auto ticksToNanoseconds = 1000000000 / static_cast<double>(ticksInSecond);

    auto cpuDurationToNs = [&] (TCpuDuration cpuDuration) {
        return static_cast<ui64>(cpuDuration * ticksToNanoseconds);
    };

    return Format(
        "RowCount: %v, "
        "Summary Init/Read Time: %vns / %vns, "
        "BuildReadWindows/GetValuesIdMapping/CreateColumnBlockHolders/GetTypesFromSchema/Logging/CreateRowsetBuilder/CreateBlockManager Times: %vns / %vns / %vns / %vns / %vns / %vns / %vns, "
        "Decode Timestamp/Key/Value Times: %vns / %vns / %vns, "
        "FetchBlocks/BuildRanges/DoRead/CollectCounts/AllocateRows/DoReadKeys/DoReadValues Times: %vns / %vns / %vns / %vns / %vns / %vns / %vns, "
        "TryUpdateWindow/SkipToBlock/FetchBlock/SetBlock/UpdateSegment/DoRead CallCounts: %v / %v / %v / %v / %v / %v",
        statistics.RowCount,
        cpuDurationToNs(statistics.InitTime),
        cpuDurationToNs(statistics.ReadTime),
        cpuDurationToNs(statistics.BuildReadWindowsTime),
        cpuDurationToNs(statistics.GetValuesIdMappingTime),
        cpuDurationToNs(statistics.CreateColumnBlockHoldersTime),
        cpuDurationToNs(statistics.GetTypesFromSchemaTime),
        cpuDurationToNs(statistics.LoggingTime),
        cpuDurationToNs(statistics.CreateRowsetBuilderTime),
        cpuDurationToNs(statistics.CreateBlockManagerTime),
        cpuDurationToNs(statistics.DecodeTimestampSegmentTime),
        cpuDurationToNs(statistics.DecodeKeySegmentTime),
        cpuDurationToNs(statistics.DecodeValueSegmentTime),
        cpuDurationToNs(statistics.FetchBlockTime),
        cpuDurationToNs(statistics.BuildRangesTime),
        cpuDurationToNs(statistics.DoReadTime),
        cpuDurationToNs(statistics.CollectCountsTime),
        cpuDurationToNs(statistics.AllocateRowsTime),
        cpuDurationToNs(statistics.DoReadKeysTime),
        cpuDurationToNs(statistics.DoReadValuesTime),
        statistics.TryUpdateWindowCallCount,
        statistics.SkipToBlockCallCount,
        statistics.FetchBlockCallCount,
        statistics.SetBlockCallCount,
        statistics.UpdateSegmentCallCount,
        statistics.DoReadCallCount);
}

// TReaderWrapper implements IVersionedReader interface.
// It calculates performance counters and provides data and decompression statistics.
class TReaderWrapper
    : public IVersionedReader
    , public TVersionedChunkReader
{
public:
    TReaderWrapper(
        TIntrusivePtr<TPreparedChunkMeta> preparedMeta,
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::unique_ptr<bool[]> columnHunkFlags,
        std::unique_ptr<IBlockManager> blockManager,
        std::unique_ptr<IRowsetBuilder> rowsetBuilder,
        std::vector<TSpanMatching>&& windowsList,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        bool lookup,
        TReaderStatisticsPtr readerStatistics)
        : TVersionedChunkReader(
            std::move(blockManager),
            std::move(rowsetBuilder),
            std::move(windowsList),
            std::move(readerStatistics))
        , PreparedMeta_(std::move(preparedMeta))
        , PerformanceCounters_(std::move(performanceCounters))
        , ChunkMeta_(std::move(chunkMeta))
        , ColumnHunkFlags_(std::move(columnHunkFlags))
        , Lookup_(lookup)
    { }

    ~TReaderWrapper()
    {
        YT_LOG_DEBUG("Reader statistics (%v)", *ReaderStatistics_);
    }

    TFuture<void> Open() override
    {
        return VoidFuture;
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        YT_VERIFY(options.MaxRowsPerRead > 0);

        std::vector<TMutableVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        bool hasMore = Read(&rows);
        if (!hasMore) {
            YT_VERIFY(rows.empty());
            return nullptr;
        }

        if (rows.empty()) {
            return CreateEmptyVersionedRowBatch();
        }

        auto* rowsData = rows.data();
        auto rowsSize = rows.size();
        return CreateBatchFromVersionedRows(MakeSharedRange(
            TRange<TVersionedRow>(rowsData, rowsSize),
            std::move(rows),
            MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return BlockManager_->GetReadyEvent();
    }

    // TODO(lukyan): Provide statistics object to BlockFetcher.
    TDataStatistics GetDataStatistics() const override
    {
        TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_uncompressed_data_size(BlockManager_->GetUncompressedDataSize());
        dataStatistics.set_compressed_data_size(BlockManager_->GetCompressedDataSize());
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return TCodecStatistics()
            .Append(BlockManager_->GetDecompressionTime());
    }

    bool IsFetchingCompleted() const override
    {
        return BlockManager_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    // Hold reference to prepared meta.
    const TIntrusivePtr<TPreparedChunkMeta> PreparedMeta_;
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;
    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    const std::unique_ptr<bool[]> ColumnHunkFlags_;

    // TODO(lukyan): Use performance counters adapter and increment counters uniformly and remove this flag.
    const bool Lookup_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    bool Read(std::vector<TMutableVersionedRow>* rows)
    {
        ui64 dataWeight = 0;

        rows->clear();
        bool hasMore = ReadRows(
            rows,
            rows->capacity(),
            &dataWeight);

        if (ColumnHunkFlags_) {
            auto* pool = GetPool();
            for (auto row : *rows) {
                GlobalizeHunkValuesAndSetHunkFlag(
                    pool,
                    ChunkMeta_,
                    ColumnHunkFlags_.get(),
                    row);
            }
        }

        i64 rowCount = 0;
        for (auto row : *rows) {
            if (row) {
                ++rowCount;
            }
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;

        if (Lookup_) {
            PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
            PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;
        } else {
            PerformanceCounters_->StaticChunkRowReadCount += rowCount;
            PerformanceCounters_->StaticChunkRowReadDataWeightCount += dataWeight;
        }

        if (hasMore) {
            return true;
        }

        // Actually does not have more but caller expect true if rows are not empty.
        return !rows->empty();
    }
};

bool IsKeys(const TSharedRange<TRowRange>&)
{
    return false;
}

bool IsKeys(const TSharedRange<TLegacyKey>&)
{
    return true;
}

bool IsKeys(const std::vector<ui32>&)
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <class TReadItems>
IVersionedReaderPtr CreateVersionedChunkReader(
    TReadItems readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TColumnFilter& columnFilter,
    const TChunkColumnMappingPtr& chunkColumnMapping,
    TBlockManagerFactory blockManagerFactory,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    bool produceAll,
    TReaderStatisticsPtr readerStatistics)
{
    if (!readerStatistics) {
        readerStatistics = New<TReaderStatistics>();
    }

    TCpuDurationIncrementingGuard timingGuard(&readerStatistics->InitTime);

    TCpuDuration lastCpuInstant = GetCpuInstant();
    auto getDurationAndReset = [&] {
        auto startCpuInstant = lastCpuInstant;
        lastCpuInstant = GetCpuInstant();
        return lastCpuInstant - startCpuInstant;
    };

    auto preparedChunkMeta = chunkMeta->GetPreparedChunkMeta();
    auto windowsList = BuildReadWindows(readItems, chunkMeta, tableSchema->GetKeyColumnCount());
    readerStatistics->BuildReadWindowsTime = getDurationAndReset();

    auto valuesIdMapping = chunkColumnMapping
        ? chunkColumnMapping->BuildVersionedSimpleSchemaIdMapping(columnFilter)
        : TChunkColumnMapping(tableSchema, chunkMeta->GetChunkSchema())
            .BuildVersionedSimpleSchemaIdMapping(columnFilter);
    readerStatistics->GetValuesIdMappingTime = getDurationAndReset();

    auto groupIds = GetGroupsIds(*preparedChunkMeta, chunkMeta->GetChunkKeyColumnCount(), valuesIdMapping);
    auto groupBlockHolders = CreateGroupBlockHolders(*preparedChunkMeta, groupIds);
    readerStatistics->CreateColumnBlockHoldersTime = getDurationAndReset();

    auto keyTypes = GetKeyTypes(tableSchema);
    auto valueSchema = GetValuesSchema(tableSchema, valuesIdMapping);
    readerStatistics->GetTypesFromSchemaTime = getDurationAndReset();

    YT_LOG_DEBUG("Creating rowset builder (ReadItemCount: %v, KeyTypes: %v, ValueTypes: %v, NewMeta: %v)",
        readItems.size(),
        keyTypes,
        MakeFormattableView(valueSchema, [] (TStringBuilderBase* builder, TValueSchema valueSchema) {
            builder->AppendFormat("%v", valueSchema.Type);
        }),
        preparedChunkMeta->FullNewMeta);

    if (timestamp == NTableClient::AllCommittedTimestamp) {
        produceAll = true;
    }

    readerStatistics->LoggingTime = getDurationAndReset();

    YT_VERIFY(produceAll || timestamp != NTableClient::AllCommittedTimestamp);

    std::vector<TColumnBase> columnInfos;
    columnInfos.reserve(std::ssize(keyTypes) + std::ssize(valuesIdMapping) + 1);

    for (int index = 0; index < std::ssize(keyTypes); ++index) {
        const TBlockRef* blockRef = nullptr;
        if (index < chunkMeta->GetChunkKeyColumnCount()) {
            auto groupId = preparedChunkMeta->ColumnIdToGroupId[index];
            auto blockHolderIndex = LowerBound(groupIds.begin(), groupIds.end(), groupId) - groupIds.begin();
            blockRef = &groupBlockHolders[blockHolderIndex];
        }

        columnInfos.emplace_back(
            blockRef,
            preparedChunkMeta->ColumnIndexInGroup[index]);
    }

    for (auto [chunkSchemaIndex, readerSchemaIndex] : valuesIdMapping) {
        auto groupId = preparedChunkMeta->ColumnIdToGroupId[chunkSchemaIndex];
        auto blockHolderIndex = LowerBound(groupIds.begin(), groupIds.end(), groupId) - groupIds.begin();
        const auto* blockRef = &groupBlockHolders[blockHolderIndex];
        columnInfos.emplace_back(
            blockRef,
            preparedChunkMeta->ColumnIndexInGroup[chunkSchemaIndex]);
    }

    {
        // Timestamp column info.
        auto groupId = preparedChunkMeta->ColumnIdToGroupId.back();
        auto blockHolderIndex = LowerBound(groupIds.begin(), groupIds.end(), groupId) - groupIds.begin();
        const auto* blockRef = &groupBlockHolders[blockHolderIndex];
        columnInfos.emplace_back(
            blockRef,
            preparedChunkMeta->ColumnIndexInGroup.back());
    }

    auto rowsetBuilder = CreateRowsetBuilder(
        std::move(readItems),
        keyTypes,
        valueSchema,
        columnInfos,
        timestamp,
        produceAll,
        preparedChunkMeta->FullNewMeta);

    readerStatistics->CreateRowsetBuilderTime = getDurationAndReset();

    auto blockManager = blockManagerFactory(std::move(groupBlockHolders), windowsList);
    readerStatistics->CreateBlockManagerTime = getDurationAndReset();

    std::unique_ptr<bool[]> columnHunkFlags;
    const auto& chunkSchema = chunkMeta->GetChunkSchema();
    if (chunkSchema->HasHunkColumns()) {
        columnHunkFlags.reset(new bool[tableSchema->GetColumnCount()]());
        for (auto [chunkColumnId, tableColumnId] : valuesIdMapping) {
            columnHunkFlags[tableColumnId] = chunkSchema->Columns()[chunkColumnId].MaxInlineHunkSize().has_value();
        }
    }

    return New<TReaderWrapper>(
        std::move(preparedChunkMeta),
        std::move(chunkMeta),
        std::move(columnHunkFlags),
        std::move(blockManager),
        std::move(rowsetBuilder),
        std::move(windowsList),
        std::move(performanceCounters),
        IsKeys(readItems),
        readerStatistics);
}

template
IVersionedReaderPtr CreateVersionedChunkReader<TSharedRange<TRowRange>>(
    TSharedRange<TRowRange> readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TColumnFilter& columnFilter,
    const TChunkColumnMappingPtr& chunkColumnMapping,
    TBlockManagerFactory blockManagerFactory,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    bool produceAll,
    TReaderStatisticsPtr readerStatistics);

template
IVersionedReaderPtr CreateVersionedChunkReader<TSharedRange<TLegacyKey>>(
    TSharedRange<TLegacyKey> readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TColumnFilter& columnFilter,
    const TChunkColumnMappingPtr& chunkColumnMapping,
    TBlockManagerFactory blockManagerFactory,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    bool produceAll,
    TReaderStatisticsPtr readerStatistics);

template
IVersionedReaderPtr CreateVersionedChunkReader<std::vector<ui32>>(
    std::vector<ui32> readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TColumnFilter& columnFilter,
    const TChunkColumnMappingPtr& chunkColumnMapping,
    TBlockManagerFactory blockManagerFactory,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    bool produceAll,
    TReaderStatisticsPtr readerStatistics);

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> ClipRanges(
    TSharedRange<TRowRange> ranges,
    TUnversionedRow lower,
    TUnversionedRow upper,
    THolderPtr holder)
{
    auto startIt = ranges.begin();
    auto endIt = ranges.end();

    if (lower) {
        startIt = BinarySearch(startIt, endIt, [&] (auto it) {
            return it->second <= lower;
        });
    }

    if (upper) {
        endIt = BinarySearch(startIt, endIt, [&] (auto it) {
            return it->first < upper;
        });
    }

    if (startIt != endIt) {
        std::vector<TRowRange> items(startIt, endIt);
        if (lower && lower > items.front().first) {
            items.front().first = lower;
        }

        if (upper && upper < items.back().second) {
            items.back().second = upper;
        }

        return MakeSharedRange(
            std::move(items),
            std::move(ranges.ReleaseHolder()),
            std::move(holder));
    } else {
        return TSharedRange<TRowRange>(); // Empty ranges.
    }
}

TSharedRange<TRowRange> ConvertLegacyRanges(
    NTableClient::TLegacyOwningKey lowerLimit,
    NTableClient::TLegacyOwningKey upperLimit)
{
    // Workaround for case with invalid read limits (lower is greater than upper: [0#1, 1#<Min>] .. [0#1])
    // Test: test_traverse_table_with_alter_and_ranges_stress
    return MakeSingletonRowRange(lowerLimit, std::max(lowerLimit, upperLimit));
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type>
struct TRefineColumn
{
    static void Do(
        const std::vector<std::pair<ui32, ui32>>& indexList,
        std::vector<std::pair<ui32, ui32>>* nextIndexList,
        TRange<TLegacyKey> keys,
        TChunkId chunkId,
        int columnId,
        int chunkKeyColumnCount,
        const NNewTableClient::TPreparedChunkMeta& preparedChunkMeta,
        NChunkClient::IBlockCache* blockCache)
    {
        auto groupId = preparedChunkMeta.ColumnIdToGroupId[columnId];
        auto columnIndexInGroup = preparedChunkMeta.ColumnIndexInGroup[columnId];

        ui32 chunkRowIndex = SentinelRowIndex;

        TGroupBlockHolder groupBlockolder(
            preparedChunkMeta.ColumnGroups[groupId].BlockIds,
            preparedChunkMeta.ColumnGroups[groupId].BlockChunkRowCounts,
            preparedChunkMeta.ColumnGroups[groupId].MergedMetas);

        TColumnBase columnBase(&groupBlockolder, columnIndexInGroup);

        ui32 position = 0;
        // Index of segment is used for exponential search.
        ui32 segmentIndex = 0;

        TUnversionedValue value;
        TKeySegmentReader<Type, false> columnReader;

        // By default TKeySegmentReader is initialized to null column.
        if (columnId < chunkKeyColumnCount) {
            columnReader.Reset();
        }

        for (auto [rowIndex, keyIndex] : indexList) {
            if (rowIndex != chunkRowIndex) {
                chunkRowIndex = rowIndex;

                if (chunkRowIndex >= columnReader.GetSegmentRowLimit()) {
                    if (auto blockIndex = groupBlockolder.SkipToBlock(chunkRowIndex)) {
                        NChunkClient::TBlockId blockId(chunkId, *blockIndex);
                        auto cachedBlock = blockCache->FindBlock(blockId, NChunkClient::EBlockType::UncompressedData).Block;

                        if (!cachedBlock) {
                            THROW_ERROR_EXCEPTION("Using lookup hash table with compressed in memory mode is not supported");
                        }

                        groupBlockolder.SetBlock(std::move(cachedBlock.Data));
                        segmentIndex = 0;
                    }

                    auto segmentsMetas = columnBase.GetSegmentMetas<TKeyMeta<Type>>();

                    // Search segment
                    auto segmentIt = ExponentialSearch(
                        segmentsMetas.begin() + segmentIndex,
                        segmentsMetas.end(),
                        [&] (auto segmentMetaIt) {
                            return segmentMetaIt->ChunkRowCount <= chunkRowIndex;
                        });

                    YT_VERIFY(segmentIt != segmentsMetas.end());
                    segmentIndex = std::distance(segmentsMetas.begin(), segmentIt);

                    const auto* segmentMeta = &segmentsMetas[segmentIndex];
                    const ui64* data = reinterpret_cast<const ui64*>(columnBase.GetBlock().Begin() + segmentMeta->DataOffset);

                    if (preparedChunkMeta.FullNewMeta) {
                        DoInitLookupKeySegment</*NewMeta*/ true>(&columnReader, segmentMeta, data);
                    } else {
                        DoInitLookupKeySegment</*NewMeta*/ false>(&columnReader, segmentMeta, data);
                    }

                    position = 0;
                }

                position = columnReader.SkipTo(chunkRowIndex, position);
                columnReader.Extract(&value, position);
            }

            if (keys[keyIndex][columnId] == value) {
                nextIndexList->emplace_back(rowIndex, keyIndex);
            }
        }
    }
};

std::vector<ui32> BuildChunkRowIndexesUsingLookupTable(
    const NTableClient::TChunkLookupHashTable& lookupHashTable,
    TRange<TLegacyKey> keys,
    const TTableSchemaPtr& tableSchema,
    const NTableClient::TCachedVersionedChunkMetaPtr& chunkMeta,
    TChunkId chunkId,
    NChunkClient::IBlockCache* blockCache)
{
    auto lookupInHashTableStart = GetCpuInstant();
    auto keyTypes = GetKeyTypes(tableSchema);
    TCompactVector<TLinearProbeHashTable::TValue, 1> foundChunkRowIndexes;

    // Build list of chunkRowIndexes and corresponding keyIndexesCandidates.
    std::vector<std::pair<ui32, ui32>> indexList;
    indexList.reserve(std::ssize(keys));
    for (int keyIndex = 0; keyIndex < std::ssize(keys); ++keyIndex) {
        lookupHashTable.Find(GetFarmFingerprint(keys[keyIndex]), &foundChunkRowIndexes);

        for (auto chunkRowIndex : foundChunkRowIndexes) {
            indexList.emplace_back(chunkRowIndex, keyIndex);
        }

        foundChunkRowIndexes.clear();
    }

    TCpuDuration lookupInHashTableTime = GetCpuInstant() - lookupInHashTableStart;

    auto sortStart = GetCpuInstant();
    std::sort(indexList.begin(), indexList.end());
    TCpuDuration sortTime = GetCpuInstant() - sortStart;

    std::vector<std::pair<ui32, ui32>> nextIndexList;
    nextIndexList.reserve(std::ssize(indexList));

    auto refineStart = GetCpuInstant();

    auto preparedChunkMeta = chunkMeta->GetPreparedChunkMeta();

    // Refine indexList.
    for (ui32 columnId = 0; columnId < keyTypes.size(); ++columnId) {
        DispatchByDataType<TRefineColumn>(
            keyTypes[columnId],
            indexList,
            &nextIndexList,
            keys,
            chunkId,
            columnId,
            chunkMeta->GetChunkKeyColumnCount(),
            *preparedChunkMeta,
            blockCache);

        indexList.clear();
        nextIndexList.swap(indexList);
    }

    std::vector<ui32> chunkRowIndexes(std::ssize(keys), SentinelRowIndex);
    for (auto [rowIndex, keyIndex] : indexList) {
        YT_VERIFY(chunkRowIndexes[keyIndex] == SentinelRowIndex);
        chunkRowIndexes[keyIndex] = rowIndex;
    }

    TCpuDuration refineTime = GetCpuInstant() - refineStart;

    YT_LOG_DEBUG("BuildChunkRowIndexesUsingLookupTable (Lookup/Sort/Refine Time: %v / %v / %v)",
        CpuDurationToDuration(lookupInHashTableTime),
        CpuDurationToDuration(sortTime),
        CpuDurationToDuration(refineTime));

    return chunkRowIndexes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
