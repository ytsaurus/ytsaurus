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
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/chunk_lookup_hash_table.h>

#include <yt/yt/client/table_client/private.h>

#include <yt/yt/library/query/base/coordination_helpers.h>

namespace NYT::NColumnarChunkFormat {

using NProfiling::TCpuDurationIncrementingGuard;

using NTableClient::TColumnFilter;

using NTableClient::IVersionedReader;
using NTableClient::IVersionedReaderPtr;
using NTableClient::TVersionedRow;

using NChunkClient::TCodecStatistics;
using NChunkClient::TChunkId;
using NChunkClient::NProto::TDataStatistics;

using NTableClient::IVersionedRowBatchPtr;
using NTableClient::TRowBatchReadOptions;
using NTableClient::CreateEmptyVersionedRowBatch;

using NTableClient::TTableSchemaPtr;
using NTableClient::TTableSchema;

using NTableClient::TChunkColumnMapping;
using NTableClient::TChunkColumnMappingPtr;

constinit const auto Logger = NTableClient::TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TCompactVector<EValueType, 16> GetKeyTypes(const TTableSchemaPtr& tableSchema)
{
    auto keyColumnCount = tableSchema->GetKeyColumnCount();
    const auto& schemaColumns = tableSchema->Columns();

    TCompactVector<EValueType, 16> keyTypes;
    keyTypes.resize(keyColumnCount);
    // Use raw data pointer because TCompactVector has branch in index operator.
    auto* keyTypesData = keyTypes.data();
    for (int keyColumnIndex = 0; keyColumnIndex < keyColumnCount; ++keyColumnIndex) {
        auto type = schemaColumns[keyColumnIndex].GetWireType();
        keyTypesData[keyColumnIndex] = type;
    }

    return keyTypes;
}

TCompactVector<TValueSchema, 32> GetValuesSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& chunkSchema,
    TRange<TColumnIdMapping> valueIdMapping)
{
    TCompactVector<TValueSchema, 32> valueSchema;
    valueSchema.resize(std::ssize(valueIdMapping));
    // Use raw data pointer because TCompactVector has branch in index operator.
    auto* valueSchemaData = valueSchema.data();

    const auto& schemaColumns = tableSchema.Columns();
    for (const auto& idMapping : valueIdMapping) {
        auto type = schemaColumns[idMapping.ReaderSchemaIndex].GetWireType();
        *valueSchemaData++ = TValueSchema{
            ui16(idMapping.ReaderSchemaIndex),
            type,
            chunkSchema.Columns()[idMapping.ChunkSchemaIndex].Aggregate().has_value(),
        };
    }

    return valueSchema;
}

////////////////////////////////////////////////////////////////////////////////

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
        TRange(static_cast<const TItem*>(keys.begin()), keys.size()),
        TPredicate{chunkMeta->GetChunkKeyColumnCount()});
}

std::vector<TSpanMatching> BuildReadWindows(
    const TKeysWithHints& keysWithHints,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    int /*keyColumnCount*/)
{
    const auto& blockMeta = chunkMeta->DataBlockMeta();

    std::vector<TSpanMatching> readWindows;

    size_t blockIndex = 0;

    size_t dataBlocksSize = blockMeta->data_blocks_size();

    auto it = keysWithHints.RowIndexesToKeysIndexes.begin();
    auto startIt = it;

    while (it != keysWithHints.RowIndexesToKeysIndexes.end() && it->first == SentinelRowIndex) {
        ++it;
    }

    while (it != keysWithHints.RowIndexesToKeysIndexes.end()) {
        auto currentChunkRowIndex = it->first;

        blockIndex = ExponentialSearch(blockIndex, dataBlocksSize, [&] (size_t index) {
            return blockMeta->data_blocks(index).chunk_row_count() <= currentChunkRowIndex;
        });

        auto upperRowLimit = blockMeta->data_blocks(blockIndex).chunk_row_count();

        auto i = blockIndex;
        while (i > 0 && blockMeta->data_blocks(i - 1).chunk_row_count() == upperRowLimit) {
            --i;
        }

        ui32 startIndex = i > 0 ? blockMeta->data_blocks(i - 1).chunk_row_count() : 0;


        while (it != keysWithHints.RowIndexesToKeysIndexes.end() && (it->first == SentinelRowIndex || it->first < upperRowLimit)) {
            ++it;
        }

        TReadSpan controlSpan(startIt - keysWithHints.RowIndexesToKeysIndexes.begin(), it - keysWithHints.RowIndexesToKeysIndexes.begin());
        readWindows.emplace_back(TReadSpan(startIndex, upperRowLimit), controlSpan);

        startIt = it;
    }

    if (startIt != it) {
        auto chunkRowCount = chunkMeta->Misc().row_count();
        TReadSpan controlSpan(startIt - keysWithHints.RowIndexesToKeysIndexes.begin(), it - keysWithHints.RowIndexesToKeysIndexes.begin());
        // Add window for ranges after chunk end bound. It will be used go generate sentinel rows for lookup.
        readWindows.emplace_back(TReadSpan(chunkRowCount, chunkRowCount), controlSpan);
    }

    return readWindows;
}

////////////////////////////////////////////////////////////////////////////////

ui32 GetReadItemWidth(TRange<TRowRange> keyRanges, int /*keyColumnCount*/)
{
    ui32 maxCount = 0;
    for (auto [lower, upper] : keyRanges) {
        maxCount = std::max(maxCount, std::max(lower.GetCount(), upper.GetCount()));
    }
    return maxCount;
}

ui32 GetReadItemWidth(TRange<TLegacyKey> /*keyRanges*/, int keyColumnCount)
{
    return keyColumnCount;
}

ui32 GetReadItemWidth(const TKeysWithHints& /*keysWithHints*/, int keyColumnCount)
{
    return keyColumnCount;
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkReader
{
public:
    TVersionedChunkReader(
        std::unique_ptr<IBlockManager> blockManager,
        std::unique_ptr<IRowsetBuilder> rowsetBuilder,
        std::vector<TSpanMatching>&& windowsList,
        TReaderStatisticsPtr readerStatistics,
        NTableClient::TKeyFilterStatisticsPtr keyFilterStatistics)
        : BlockManager_(std::move(blockManager))
        , ReaderStatistics_(std::move(readerStatistics))
        , KeyFilterStatistics_(std::move(keyFilterStatistics))
        , RowsetBuilder_(std::move(rowsetBuilder))
        , WindowsList_(std::move(windowsList))
    {
        std::reverse(WindowsList_.begin(), WindowsList_.end());
    }

    bool ReadRows(std::vector<TMutableVersionedRow>* rows, ui32 readCount, ui64* dataWeight)
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
                    dataWeight,
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
    const NTableClient::TKeyFilterStatisticsPtr KeyFilterStatistics_;

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
            RowsetBuilder_->BuildReadListForWindow(WindowsList_.back(), KeyFilterStatistics_);
            WindowsList_.pop_back();
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TReaderStatistics& statistics, TStringBuf /*spec*/)
{
    auto ticksInSecond = DurationToCpuDuration(TDuration::Seconds(1));
    auto ticksToNanoseconds = 1000000000 / static_cast<double>(ticksInSecond);

    auto cpuDurationToNs = [&] (TCpuDuration cpuDuration) {
        return static_cast<ui64>(cpuDuration * ticksToNanoseconds);
    };

    Format(
        builder,
        "RowCount: %v, "
        "Summary Init/Read Time: %vns / %vns, "
        "BuildReadWindows/GetValuesIdMapping/CreateColumnBlockHolders/GetTypesFromSchema/BuildColumnInfos/CreateRowsetBuilder/CreateBlockManager Times: %vns / %vns / %vns / %vns / %vns / %vns / %vns, "
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
        cpuDurationToNs(statistics.BuildColumnInfosTime),
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
        bool lookup,
        TReaderStatisticsPtr readerStatistics,
        NTableClient::TKeyFilterStatisticsPtr keyFilterStatistics,
        int lookupKeyCount)
        : TVersionedChunkReader(
            std::move(blockManager),
            std::move(rowsetBuilder),
            std::move(windowsList),
            std::move(readerStatistics),
            std::move(keyFilterStatistics))
        , PreparedMeta_(std::move(preparedMeta))
        , ChunkMeta_(std::move(chunkMeta))
        , ColumnHunkFlags_(std::move(columnHunkFlags))
        , Lookup_(lookup)
        , LookupKeyCount_(lookupKeyCount)
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
        // Do not reserve too much if there are few lookup keys.
        auto lookupKeyCount = Lookup_ ? LookupKeyCount_ : std::numeric_limits<int>::max();
        rows.reserve(std::min(options.MaxRowsPerRead, i64(lookupKeyCount)));
        YT_VERIFY(rows.capacity() > 0);

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
    const TCachedVersionedChunkMetaPtr ChunkMeta_;

    const std::unique_ptr<bool[]> ColumnHunkFlags_;

    const bool Lookup_;
    const int LookupKeyCount_;

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

        DataWeight_ += dataWeight;

        for (auto row : *rows) {
            RowCount_ += static_cast<bool>(row);
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

bool IsKeys(const TKeysWithHints&)
{
    return true;
}

size_t GetReadItemCount(const TSharedRange<TRowRange>& readItems)
{
    return readItems.size();
}

size_t GetReadItemCount(const TSharedRange<TLegacyKey>& readItems)
{
    return readItems.size();
}

size_t GetReadItemCount(const TKeysWithHints& readItems)
{
    return readItems.Keys.size();
}

////////////////////////////////////////////////////////////////////////////////

TCompactVector<ui16, 8> ExtractKeyColumnIndexes(
    const TColumnFilter& columnFilter,
    int tableKeyColumnCount,
    bool forceAllKeyColumns)
{
    TCompactVector<ui16, 8> keyColumnIndexes;
    if (columnFilter.IsUniversal() || forceAllKeyColumns) {
        keyColumnIndexes.resize(tableKeyColumnCount);
        for (int index = 0; index < tableKeyColumnCount; ++index) {
            keyColumnIndexes[index] = index;
        }
    } else {
        auto indexes = TRange(columnFilter.GetIndexes());
        for (auto index : indexes) {
            if (index < tableKeyColumnCount) {
                keyColumnIndexes.push_back(index);
            }
        }
    }
    return keyColumnIndexes;
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
    bool produceAll,
    TReaderStatisticsPtr readerStatistics,
    NTableClient::TKeyFilterStatisticsPtr keyFilterStatistics,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    if (!readerStatistics) {
        readerStatistics = New<TReaderStatistics>();
    }

    if (timestamp == NTableClient::AllCommittedTimestamp) {
        produceAll = true;
    }

    YT_VERIFY(produceAll || timestamp != NTableClient::AllCommittedTimestamp);

    auto readItemCount = GetReadItemCount(readItems);

    TCpuDurationIncrementingGuard timingGuard(&readerStatistics->InitTime);

    auto tableKeyColumnCount = tableSchema->GetKeyColumnCount();
    auto chunkKeyColumnCount = chunkMeta->GetChunkKeyColumnCount();
    const auto& chunkSchema = chunkMeta->ChunkSchema();

    // Lightweight duration counter.
    TCpuDuration lastCpuInstant = GetCpuInstant();
    auto getDurationAndReset = [&] {
        auto startCpuInstant = lastCpuInstant;
        lastCpuInstant = GetCpuInstant();
        return lastCpuInstant - startCpuInstant;
    };

    // Read item width is used to infer read spans.
    int readItemWidth = GetReadItemWidth(readItems, tableKeyColumnCount);
    readItemWidth = std::min(readItemWidth, tableKeyColumnCount);

    auto keyColumnIndexes = ExtractKeyColumnIndexes(columnFilter, tableKeyColumnCount, IsKeys(readItems));

    auto preparedChunkMeta = chunkMeta->GetPreparedChunkMeta();
    auto windowsList = BuildReadWindows(readItems, chunkMeta, tableKeyColumnCount);
    readerStatistics->BuildReadWindowsTime = getDurationAndReset();

    auto valuesIdMapping = chunkColumnMapping
        ? chunkColumnMapping->BuildVersionedSimpleSchemaIdMapping(columnFilter)
        : TChunkColumnMapping(tableSchema, chunkSchema)
            .BuildVersionedSimpleSchemaIdMapping(columnFilter);
    readerStatistics->GetValuesIdMappingTime = getDurationAndReset();

    auto groupIds = GetGroupsIds(
        *preparedChunkMeta,
        chunkKeyColumnCount,
        readItemWidth,
        keyColumnIndexes,
        valuesIdMapping);

    auto groupBlockHolders = CreateGroupBlockHolders(*preparedChunkMeta, groupIds);
    readerStatistics->CreateColumnBlockHoldersTime = getDurationAndReset();

    auto keyTypes = GetKeyTypes(tableSchema);
    auto valueSchema = GetValuesSchema(*tableSchema, *chunkSchema, valuesIdMapping);
    readerStatistics->GetTypesFromSchemaTime = getDurationAndReset();

    TCompactVector<TColumnBase, 32> columnInfos;
    columnInfos.resize(std::ssize(keyTypes) + std::ssize(valuesIdMapping) + 1);
    // Use raw data pointer because TCompactVector has branch in index operator.
    auto* columnInfosData = columnInfos.data();

    auto makeColumnBase = [&] (const TPreparedChunkMeta::TColumnInfo& columnInfo, ui16 columnId) {
        auto groupId = columnInfo.GroupId;
        auto blockHolderIndex = LowerBound(groupIds.begin(), groupIds.end(), groupId) - groupIds.begin();
        YT_VERIFY(blockHolderIndex < std::ssize(groupIds) && groupIds[blockHolderIndex] == groupId);
        const auto* blockRef = &groupBlockHolders[blockHolderIndex];

        *columnInfosData++ = {blockRef, columnInfo.IndexInGroup, columnId};
    };

    auto makeKeyColumnBase = [&] (int keyColumnIndex) {
        if (keyColumnIndex < chunkKeyColumnCount) {
            makeColumnBase(preparedChunkMeta->ColumnInfos[keyColumnIndex], keyColumnIndex);
        } else {
            *columnInfosData++ = {nullptr, 0, ui16(keyColumnIndex)};
        }
    };

    for (int keyColumnIndex = 0; keyColumnIndex < readItemWidth; ++keyColumnIndex) {
        makeKeyColumnBase(keyColumnIndex);
    }

    for (auto keyColumnIndex : keyColumnIndexes) {
        if (keyColumnIndex < readItemWidth) {
            continue;
        }

        makeKeyColumnBase(keyColumnIndex);
    }

    for (auto [chunkSchemaIndex, readerSchemaIndex] : valuesIdMapping) {
        makeColumnBase(preparedChunkMeta->ColumnInfos[chunkSchemaIndex], readerSchemaIndex);
    }

    {
        // Timestamp column info.
        makeColumnBase(preparedChunkMeta->ColumnInfos.back(), 0);
    }

    columnInfos.resize(columnInfosData - columnInfos.data());

    readerStatistics->BuildColumnInfosTime = getDurationAndReset();

    auto blockManager = blockManagerFactory(std::move(groupBlockHolders), windowsList);
    readerStatistics->CreateBlockManagerTime = getDurationAndReset();

    // Do not log in case of reading from memory.
    YT_LOG_DEBUG_IF(
        !blockManager->IsFetchingCompleted(),
        "Creating rowset builder (ReadItemCount: %v, GroupIds: %v, KeyTypes: %v, "
        "ReadItemWidth: %v, KeyColumnIndexes: %v, ValueTypes: %v, NewMeta: %v)",
        readItemCount,
        groupIds,
        keyTypes,
        readItemWidth,
        keyColumnIndexes,
        MakeFormattableView(valueSchema, [] (TStringBuilderBase* builder, const TValueSchema& valueSchema) {
            builder->AppendFormat("%v", valueSchema.Type);
        }),
        preparedChunkMeta->FullNewMeta);

    auto rowsetBuilder = CreateRowsetBuilder(std::move(readItems), {
        .KeyTypes = keyTypes,
        .ReadItemWidth = static_cast<ui16>(readItemWidth),
        .ProduceAll = produceAll,
        .NewMeta = preparedChunkMeta->FullNewMeta,
        .KeyColumnIndexes = std::move(keyColumnIndexes),
        .ValueSchema = valueSchema,
        .ColumnInfos = columnInfos,
        .Timestamp = timestamp,
        .MemoryUsageTracker = std::move(memoryUsageTracker),
    });

    readerStatistics->CreateRowsetBuilderTime = getDurationAndReset();

    std::unique_ptr<bool[]> columnHunkFlags;
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
        IsKeys(readItems),
        readerStatistics,
        std::move(keyFilterStatistics),
        readItemCount);
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
    bool produceAll,
    TReaderStatisticsPtr readerStatistics,
    NTableClient::TKeyFilterStatisticsPtr keyFilterStatistics,
    IMemoryUsageTrackerPtr memoryUsageTracker);

template
IVersionedReaderPtr CreateVersionedChunkReader<TSharedRange<TLegacyKey>>(
    TSharedRange<TLegacyKey> readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TColumnFilter& columnFilter,
    const TChunkColumnMappingPtr& chunkColumnMapping,
    TBlockManagerFactory blockManagerFactory,
    bool produceAll,
    TReaderStatisticsPtr readerStatistics,
    NTableClient::TKeyFilterStatisticsPtr keyFilterStatistics,
    IMemoryUsageTrackerPtr memoryUsageTracker);

template
IVersionedReaderPtr CreateVersionedChunkReader<TKeysWithHints>(
    TKeysWithHints readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TColumnFilter& columnFilter,
    const TChunkColumnMappingPtr& chunkColumnMapping,
    TBlockManagerFactory blockManagerFactory,
    bool produceAll,
    TReaderStatisticsPtr readerStatistics,
    NTableClient::TKeyFilterStatisticsPtr keyFilterStatistics,
    IMemoryUsageTrackerPtr memoryUsageTracker);

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
        if (lower && lower <= startIt->first) {
            lower = {};
        }

        if (upper && upper >= (endIt - 1)->second) {
            upper = {};
        }

        if (!lower && !upper) {
            return ranges.Slice(startIt, endIt);
        }

        std::vector<TRowRange> items(startIt, endIt);
        if (lower) {
            items.front().first = lower;
        }

        if (upper) {
            items.back().second = upper;
        }

        return MakeSharedRange(
            std::move(items),
            std::move(ranges.ReleaseHolder()),
            std::move(holder));
    } else {
        return {}; // Empty ranges.
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

TKeysWithHints BuildKeyHintsUsingLookupTable(
    const NTableClient::TChunkLookupHashTable& lookupHashTable,
    TSharedRange<TLegacyKey> keys)
{
    auto lookupInHashTableStart = GetCpuInstant();
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

    YT_LOG_DEBUG("BuildKeyHintsUsingLookupTable (Lookup/Sort Time: %v / %v )",
        CpuDurationToDuration(lookupInHashTableTime),
        CpuDurationToDuration(sortTime));

    if (!keys.empty()) {
        indexList.emplace_back(SentinelRowIndex, keys.size() - 1);
    }

    return {indexList, keys};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat
