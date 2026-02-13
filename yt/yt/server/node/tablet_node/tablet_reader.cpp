#include "tablet_reader.h"

#include "bootstrap.h"
#include "config.h"
#include "failing_on_rotation_reader.h"
#include "partition.h"
#include "private.h"
#include "sorted_chunk_store.h"
#include "store.h"
#include "tablet.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/schemaful_concatencaing_reader.h>

#include <yt/yt/library/row_merger/overlapping_reader.h>
#include <yt/yt/library/row_merger/row_merger.h>
#include <yt/yt/library/row_merger/versioned_row_merger.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/timestamped_schema_helpers.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>
#include <library/cpp/yt/memory/range.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NConcurrency;
using namespace NRowMerger;

using NTransactionClient::TReadTimestampRange;

////////////////////////////////////////////////////////////////////////////////

struct TTabletReaderPoolTag { };

constinit const auto Logger = TabletNodeLogger;

static constexpr TDuration DefaultMaxOverdraftDuration = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

struct TStoreRangeFormatter
{
    void operator()(TStringBuilderBase* builder, const ISortedStorePtr& store) const
    {
        builder->AppendFormat("<%v:%v>",
            store->GetMinKey(),
            store->GetUpperBoundKey());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnversifyingReader
    : public ISchemafulUnversionedReader
{
public:
    static std::vector<int> GetColumnIdsFromFilter(const TColumnFilter& columnFilter, int columnCount)
    {
        std::vector<int> columnIds;

        if (columnFilter.IsUniversal()) {
            columnIds.resize(columnCount);
            std::iota(columnIds.begin(), columnIds.end(), 0);
        } else {
            columnIds.assign(columnFilter.GetIndexes().begin(), columnFilter.GetIndexes().end());
        }

        return columnIds;
    }

    TUnversifyingReader(
        IVersionedReaderPtr versionedReader,
        TRowBufferPtr rowBuffer,
        TTableSchema* schema,
        const TColumnFilter& columnFilter,
        TNestedColumnsSchema nestedColumnsSchema = {})
        : VersionedReader_(std::move(versionedReader))
        , RowBuffer_(std::move(rowBuffer))
        , ColumnIds_(GetColumnIdsFromFilter(columnFilter, schema->GetColumnCount()))
        , NestedColumnsSchema_(FilterNestedColumnsSchema(nestedColumnsSchema, ColumnIds_))
    {
        YT_UNUSED_FUTURE(VersionedReader_->Open());

        ColumnIdToIndex_.assign(static_cast<size_t>(schema->GetColumnCount()), -1);
        NestedIdToIndex_.assign(static_cast<size_t>(schema->GetColumnCount()), -1);

        AggregateFunctions_.assign(ColumnIds_.size(), nullptr);

        int nestedColumnCount = 0;
        auto fillNestedColumnIndexes = [&] {
            if (nestedColumnCount != 0) {
                return;
            }

            for (auto [nestedColumnId, _] : NestedColumnsSchema_.KeyColumns) {
                NestedIdToIndex_[nestedColumnId] = nestedColumnCount++;
            }
        };

        for (int columnIndex = 0; columnIndex < std::ssize(ColumnIds_); ++columnIndex) {
            auto columnId = ColumnIds_[columnIndex];
            ColumnIdToIndex_[columnId] = columnIndex;

            if (columnId < schema->GetKeyColumnCount()) {
                continue;
            }

            if (FindNestedColumnById(NestedColumnsSchema_.KeyColumns, columnId)) {
                fillNestedColumnIndexes();
                continue;
            }

            if (FindNestedColumnById(NestedColumnsSchema_.ValueColumns, columnId)) {
                fillNestedColumnIndexes();
                NestedIdToIndex_[columnId] = nestedColumnCount++;
                continue;
            }

            const auto& maybeAggregate = schema->Columns()[columnId].Aggregate();

            if (!maybeAggregate) {
                THROW_ERROR_EXCEPTION("Reading without merge is supported for aggregating schemas only");
            }

            auto wireType = GetWireType(schema->Columns()[columnId].LogicalType());

            AggregateFunctions_[columnIndex] = GetSimpleAggregateFunction(*maybeAggregate, wireType);
        }

        NestedColumns_.resize(nestedColumnCount);
    }

    TUnversionedRow BuildMergedRow(TVersionedRow versionedRow)
    {
        if (Y_UNLIKELY(!versionedRow.DeleteTimestamps().Empty())) {
            THROW_ERROR_EXCEPTION("Delete timestamp are not supported");
        }

        auto* pool = RowBuffer_->GetPool();
        auto resultRow = TMutableUnversionedRow::Allocate(pool, ColumnIds_.size());

        for (int index = 0; index < std::ssize(ColumnIds_); ++index) {
            resultRow[index] = MakeUnversionedNullValue(ColumnIds_[index]);
        }

        for (auto key : versionedRow.Keys()) {
            int columnIndex = ColumnIdToIndex_[key.Id];
            if (Y_UNLIKELY(columnIndex == -1)) {
                continue;
            }

            resultRow[columnIndex] = key;
        }

        auto valueIt = const_cast<TVersionedValue*>(versionedRow.BeginValues());
        auto valueItEnd = const_cast<TVersionedValue*>(versionedRow.EndValues());

        while (valueIt != valueItEnd) {
            auto columnId = valueIt->Id;

            auto valueItNext = valueIt;
            do {
                ++valueItNext;
            } while (valueItNext != valueItEnd && valueItNext->Id == columnId);

            int nestedColumnIndex = NestedIdToIndex_[columnId];
            int columnIndex = ColumnIdToIndex_[columnId];

            // Timestamps in reverse order.
            std::reverse(valueIt, valueItNext);

            // Consider overrides.
            for (auto next = valueIt; next != valueItNext; ++next) {
                if (None(next->Flags & EValueFlags::Aggregate)) {
                    // Skip older aggregate values.
                    valueIt = next;
                }
            }

            if (nestedColumnIndex != -1) {
                NestedColumns_[nestedColumnIndex] = {valueIt, valueItNext};
                valueIt = valueItNext;
            } else if (Y_LIKELY(columnIndex != -1)) {
                auto* aggregateFunction = AggregateFunctions_[columnIndex];
                auto* state = &resultRow[columnIndex];

                while (valueIt != valueItNext) {
                    (*aggregateFunction)(state, *valueIt++);
                }
            } else {
                valueIt = valueItNext;
            }
        }

        auto nestedKeySchema = TRange(NestedColumnsSchema_.KeyColumns);
        auto nestedValueSchema = TRange(NestedColumnsSchema_.ValueColumns);

        NestedMerger_.UnpackKeyColumns(
            TRange(NestedColumns_).Slice(0, nestedKeySchema.size()),
            nestedKeySchema);

        // NB(sabdenovch): only here to signal that no discard is needed.
        // Normally this is called after all UnpackValueColumn.
        NestedMerger_.DiscardZeroes(/*nestedRowDiscardPolicy*/ nullptr);

        for (int index = 0; index < std::ssize(nestedKeySchema); ++index) {
            if (NestedColumns_[index].Empty()) {
                continue;
            }

            auto [columnId, type] = nestedKeySchema[index];

            auto state = NestedMerger_.GetPackedKeyColumn(index, type, pool);
            state.Id = columnId;

            auto columnIndex = ColumnIdToIndex_[columnId];
            // Nested key columns are added to enriched column filter.
            if (columnIndex != -1) {
                resultRow[columnIndex] = state;
            }
        }

        for (int index = 0; index < std::ssize(nestedValueSchema); ++index) {
            auto valueRange = NestedColumns_[index + std::ssize(nestedKeySchema)];

            auto [columnId, type, aggregateFunction] = nestedValueSchema[index];

            NestedMerger_.UnpackValueColumn(
                valueRange,
                type,
                aggregateFunction);

            if (valueRange.Empty()) {
                continue;
            }

            // For nested value columns requested and enriched column filters are matched.
            auto state = NestedMerger_.GetPackedValueColumn(index, type, pool);
            state.Id = columnId;

            auto columnIndex = ColumnIdToIndex_[columnId];
            YT_VERIFY(columnIndex != -1);
            resultRow[columnIndex] = state;
        }

        return resultRow;
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options = {}) override
    {
        auto batch = VersionedReader_->Read(options);
        if (!batch) {
            return nullptr;
        }

        RowBuffer_->Clear();
        auto rowsRange = batch->MaterializeRows();
        Rows_.reserve(rowsRange.Size());

        for (auto versionedRow : rowsRange) {
            Rows_.push_back(BuildMergedRow(versionedRow));

            DataWeight_ += GetDataWeight(Rows_.back());
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(Rows_), MakeStrong(this)));
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = VersionedReader_->GetDataStatistics();

        dataStatistics.set_unmerged_row_count(dataStatistics.row_count());
        dataStatistics.set_unmerged_data_weight(dataStatistics.data_weight());

        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return VersionedReader_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return VersionedReader_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return VersionedReader_->GetFailedChunkIds();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VersionedReader_->GetReadyEvent();
    }

private:
    const IVersionedReaderPtr VersionedReader_;
    const TRowBufferPtr RowBuffer_;
    const std::vector<int> ColumnIds_;
    const TNestedColumnsSchema NestedColumnsSchema_;

    std::vector<TAggregateFunction*> AggregateFunctions_;

    std::vector<int> ColumnIdToIndex_;
    std::vector<int> NestedIdToIndex_;

    TNestedTableMerger NestedMerger_{/*orderNestedRows*/ false, /*useFastYsonRoutines*/ true};
    // Key and value columns.
    std::vector<TRange<TVersionedValue>> NestedColumns_;

    std::vector<TUnversionedRow> Rows_;

    i64 DataWeight_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

void ThrowUponDistributedThrottlerOverdraft(
    ETabletDistributedThrottlerKind tabletThrottlerKind,
    const TTabletSnapshotPtr& tabletSnapshot,
    const TClientChunkReadOptions& chunkReadOptions)
{
    const auto& distributedThrottler = tabletSnapshot->DistributedThrottlers[tabletThrottlerKind];
    if (distributedThrottler && distributedThrottler->IsOverdraft()) {
        tabletSnapshot->TableProfiler->GetThrottlerCounter(tabletThrottlerKind)->Increment();
        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::RequestThrottled,
            "Read request is throttled due to %Qlv throttler overdraft",
            tabletThrottlerKind)
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("read_session_id", chunkReadOptions.ReadSessionId)
            << TErrorAttribute("queue_total_count", distributedThrottler->GetQueueTotalAmount());
    }
}

void ThrowUponNodeThrottlerOverdraft(
    std::optional<TInstant> requestStartTime,
    std::optional<TDuration> requestTimeout,
    const TClientChunkReadOptions& chunkReadOptions,
    IBootstrap* bootstrap)
{
    TDuration maxOverdraftDuration = DefaultMaxOverdraftDuration;
    if (requestStartTime && requestTimeout) {
        maxOverdraftDuration = *requestStartTime + *requestTimeout - NProfiling::GetInstant();
    }

    const auto& nodeThrottler = bootstrap->GetInThrottler(chunkReadOptions.WorkloadDescriptor.Category);
    if (nodeThrottler->GetEstimatedOverdraftDuration() > maxOverdraftDuration) {
        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::RequestThrottled,
            "Read request is throttled due to node throttler overdraft")
            << TErrorAttribute("read_session_id", chunkReadOptions.ReadSessionId)
            << TErrorAttribute("queue_total_count", nodeThrottler->GetQueueTotalAmount())
            << TErrorAttribute("max_overdraft_duration", maxOverdraftDuration);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class IReader, class TRow>
class TThrottlerAwareReaderBase
    : public IReader
{
public:
    using IReaderPtr = TIntrusivePtr<IReader>;

    TThrottlerAwareReaderBase(
        IReaderPtr underlying,
        IThroughputThrottlerPtr throttler)
        : Underlying_(std::move(underlying))
        , Throttler_(std::move(throttler))
    { }

    typename TRowBatchTrait<TRow>::IRowBatchPtr Read(const TRowBatchReadOptions& options = {}) override
    {
        auto rawBatch = Underlying_->Read(options);

        auto currentDataWeight = Underlying_->GetDataStatistics().data_weight();
        YT_VERIFY(currentDataWeight >= ThrottledDataWeight_);
        Throttler_->Acquire(currentDataWeight - ThrottledDataWeight_);
        ThrottledDataWeight_ = currentDataWeight;

        return rawBatch;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return Underlying_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return Underlying_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Underlying_->GetFailedChunkIds();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return Underlying_->GetReadyEvent();
    }

protected:
    const IReaderPtr Underlying_;
    const IThroughputThrottlerPtr Throttler_;

    i64 ThrottledDataWeight_ = 0;
};

class TThrottlerAwareVersionedReader
    : public TThrottlerAwareReaderBase<IVersionedReader, TVersionedRow>
{
public:
    TThrottlerAwareVersionedReader(
        IVersionedReaderPtr underlying,
        IThroughputThrottlerPtr throttler)
        : TThrottlerAwareReaderBase(std::move(underlying), std::move(throttler))
    { }

    TFuture<void> Open() override
    {
        return Underlying_->Open();
    }
};

class TThrottlerAwareSchemafulUnversionedReader
    : public TThrottlerAwareReaderBase<ISchemafulUnversionedReader, TUnversionedRow>
{
public:
    TThrottlerAwareSchemafulUnversionedReader(
        ISchemafulUnversionedReaderPtr underlying,
        IThroughputThrottlerPtr throttler)
        : TThrottlerAwareReaderBase(std::move(underlying), std::move(throttler))
    { }
};

template <class TThrottlerAwareReader, class IReaderPtr>
IReaderPtr MaybeWrapWithThrottlerAwareReader(
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    const TTabletSnapshotPtr& tabletSnapshot,
    IReaderPtr underlyingReader)
{
    const auto& throttler = tabletThrottlerKind
        ? tabletSnapshot->DistributedThrottlers[*tabletThrottlerKind]
        : nullptr;

    if (throttler) {
        return New<TThrottlerAwareReader>(std::move(underlyingReader), throttler);
    } else {
        return underlyingReader;
    }
}

ISchemafulUnversionedReaderPtr WrapSchemafulTabletReader(
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    const TTabletSnapshotPtr& tabletSnapshot,
    const TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    ISchemafulUnversionedReaderPtr reader)
{
    reader = MaybeWrapWithThrottlerAwareReader<TThrottlerAwareSchemafulUnversionedReader>(
        tabletThrottlerKind,
        tabletSnapshot,
        std::move(reader));

    reader = CreateHunkDecodingSchemafulReader(
        tabletSnapshot->QuerySchema,
        columnFilter,
        tabletSnapshot->Settings.HunkReaderConfig,
        std::move(reader),
        tabletSnapshot->ChunkFragmentReader,
        tabletSnapshot->DictionaryCompressionFactory,
        chunkReadOptions,
        tabletSnapshot->PerformanceCounters);

    return reader;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStoresAndBounds GetStoresAndBounds(
    const TTabletSnapshotPtr& tabletSnapshot,
    int partitionIndex,
    TSharedRange<TRowRange> bounds)
{
    auto holder = bounds.GetHolder();

    std::vector<ISortedStorePtr> stores;
    std::vector<TSharedRange<TRowRange>> boundsPerStore;
    std::vector<TRowRange> edenStoreBoundsVector;

    const auto& partition = tabletSnapshot->PartitionList[partitionIndex];

    YT_VERIFY(bounds.size() > 0);

    auto lowerBound = std::max<TLegacyKey>(bounds.Front().first, partition->PivotKey);
    auto upperBound = std::min<TLegacyKey>(bounds.Back().second, partition->NextPivotKey);

    // Enrich bounds for eden stores with partition bounds.
    NQueryClient::ForEachRange(TRange(bounds), TRowRange(lowerBound, upperBound), [&] (auto item) {
        auto [lower, upper] = item;
        edenStoreBoundsVector.emplace_back(lower, upper);
    });

    auto partitionStoreBounds = MakeSharedRange(bounds, holder);
    for (const auto& store : partition->Stores) {
        stores.push_back(store);
        boundsPerStore.push_back(partitionStoreBounds);
    }

    auto edenStoreBounds = MakeSharedRange(edenStoreBoundsVector, holder);
    for (const auto& store : tabletSnapshot->GetEdenStores()) {
        stores.push_back(store);
        boundsPerStore.push_back(edenStoreBounds);
    }

    if (std::ssize(stores) > tabletSnapshot->Settings.MountConfig->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", std::ssize(stores))
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Settings.MountConfig->MaxReadFanIn);
    }

    return {std::move(stores), std::move(boundsPerStore)};
}

ISchemafulUnversionedReaderPtr DoCreateScanReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TStoresAndBounds& storesAndBounds,
    TReadTimestampRange timestampRange,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory,
    TTimestampReadOptions timestampReadOptions,
    bool mergeVersionedRows)
{
    auto timestamp = timestampRange.Timestamp;
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);

    tabletSnapshot->WaitOnLocks(timestamp);

    if (tabletThrottlerKind) {
        ThrowUponDistributedThrottlerOverdraft(*tabletThrottlerKind, tabletSnapshot, chunkReadOptions);
    }

    const auto& stores = storesAndBounds.Stores;
    const auto& boundsPerStore = storesAndBounds.BoundsPerStore;

    ISchemafulUnversionedReaderPtr reader;

    auto nestedSchema = NRowMerger::GetNestedColumnsSchema(*tabletSnapshot->QuerySchema);
    auto enrichedColumnFilter = EnrichColumnFilter(
        columnFilter,
        nestedSchema,
        (mergeVersionedRows
            ? tabletSnapshot->QuerySchema->GetKeyColumnCount()
            : 0));

    auto overlyingColumnFilter = mergeVersionedRows
        ? ToLatestTimestampColumnFilter(
            columnFilter,
            timestampReadOptions,
            tabletSnapshot->QuerySchema->GetColumnCount())
        : columnFilter;

    if (mergeVersionedRows) {
        std::vector<TLegacyOwningKey> startStoreBounds;
        startStoreBounds.reserve(std::ssize(stores));
        for (const auto& store : stores) {
            startStoreBounds.push_back(store->GetMinKey());
        }

        auto rowMerger = CreateQueryLatestTimestampRowMerger(
            New<TRowBuffer>(TTabletReaderPoolTag()),
            tabletSnapshot,
            overlyingColumnFilter,
            timestampRange.RetentionTimestamp,
            timestampReadOptions);

        reader = NRowMerger::CreateSchemafulOverlappingRangeReader(
            startStoreBounds,
            std::move(rowMerger),
            [
                =,
                stores = std::move(stores),
                boundsPerStore = std::move(boundsPerStore)
            ] (int index) {
                YT_VERIFY(index < std::ssize(stores));

                return stores[index]->CreateReader(
                    tabletSnapshot,
                    boundsPerStore[index],
                    timestamp,
                    false,
                    enrichedColumnFilter,
                    chunkReadOptions,
                    workloadCategory);
            },
            [keyComparer = tabletSnapshot->RowKeyComparer] (TUnversionedValueRange lhs, TUnversionedValueRange rhs) {
                return keyComparer(lhs, rhs);
            });
    } else {
        auto storesCount = stores.size();
        auto getNextReader = [
            =,
            stores = std::move(stores),
            boundsPerStore = std::move(boundsPerStore),
            index = 0
        ] () mutable -> ISchemafulUnversionedReaderPtr {
            if (index == std::ssize(stores)) {
                return nullptr;
            }

            auto underlyingReader = stores[index]->CreateReader(
                tabletSnapshot,
                boundsPerStore[index],
                timestamp,
                /*produceAllVersions*/ false,
                enrichedColumnFilter,
                chunkReadOptions,
                workloadCategory);
            index++;

            return New<TUnversifyingReader>(
                std::move(underlyingReader),
                New<TRowBuffer>(TTabletReaderPoolTag()),
                tabletSnapshot->QuerySchema.get(),
                columnFilter,
                GetNestedColumnsSchema(*tabletSnapshot->QuerySchema));
        };

        reader = CreateUnorderedSchemafulReader(getNextReader, storesCount);
    }

    return WrapSchemafulTabletReader(
        tabletThrottlerKind,
        tabletSnapshot,
        chunkReadOptions,
        overlyingColumnFilter,
        std::move(reader));
}

ISchemafulUnversionedReaderPtr CreatePartitionScanReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TPartitionBounds>& partitionBounds,
    TReadTimestampRange timestampRange,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory,
    const TTimestampReadOptions& timestampReadOptions,
    bool mergeVersionedRows)
{
    auto timestamp = timestampRange.Timestamp;

    auto holder = partitionBounds.GetHolder();

    std::vector<ISortedStorePtr> stores;
    std::vector<TSharedRange<TRowRange>> boundsPerStore;

    std::vector<TRowRange> edenStoreBoundsVector;
    for (const auto& [bounds, partitionIndex] : partitionBounds) {
        const auto& partition = tabletSnapshot->PartitionList[partitionIndex];

        YT_VERIFY(bounds.size() > 0);

        auto lowerBound = std::max<TLegacyKey>(bounds.front().first, partition->PivotKey);
        auto upperBound = std::min<TLegacyKey>(bounds.back().second, partition->NextPivotKey);

        // Enrich bounds for eden stores with partition bounds.
        NQueryClient::ForEachRange(TRange(bounds), TRowRange(lowerBound, upperBound), [&] (auto item) {
            auto [lower, upper] = item;
            edenStoreBoundsVector.emplace_back(lower, upper);
        });

        auto partitionStoreBounds = MakeSharedRange(bounds, holder);

        for (const auto& store : partition->Stores) {
            stores.push_back(store);
            boundsPerStore.push_back(partitionStoreBounds);
        }
    }

    auto edenStoreBounds = MakeSharedRange(edenStoreBoundsVector, holder);

    for (const auto& store : tabletSnapshot->GetEdenStores()) {
        stores.push_back(store);
        boundsPerStore.push_back(edenStoreBounds);
    }

    if (std::ssize(stores) > tabletSnapshot->Settings.MountConfig->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", std::ssize(stores))
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Settings.MountConfig->MaxReadFanIn);
    }

    TUnversionedRow lowerBound;
    TUnversionedRow upperBound;

    if (!partitionBounds.Empty()) {
        lowerBound = partitionBounds.Front().Bounds.front().first;
        upperBound = partitionBounds.Back().Bounds.back().second;
    }

    YT_LOG_DEBUG("Creating schemaful sorted tablet reader (TabletId: %v, CellId: %v, "
        "WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v, StoreRanges: %v, "
        "Timestamp: %v, BoundCount: %v, LowerBound: %kv, UpperBound: %kv, MergeVersionedRows: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        chunkReadOptions.WorkloadDescriptor,
        chunkReadOptions.ReadSessionId,
        MakeFormattableView(stores, TStoreIdFormatter()),
        MakeFormattableView(stores, TStoreRangeFormatter()),
        timestamp,
        std::ssize(partitionBounds),
        lowerBound,
        upperBound,
        mergeVersionedRows);

    return DoCreateScanReader(
        tabletSnapshot,
        columnFilter,
        {std::move(stores), std::move(boundsPerStore)},
        timestampRange,
        chunkReadOptions,
        tabletThrottlerKind,
        workloadCategory,
        timestampReadOptions,
        mergeVersionedRows);
}

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateSchemafulSortedTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TRowRange>& bounds,
    TReadTimestampRange timestampRange,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory,
    TTimestampReadOptions timestampReadOptions,
    bool mergeVersionedRows)
{
    auto timestamp = timestampRange.Timestamp;
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);

    YT_VERIFY(bounds.Size() > 0);
    auto lowerBound = bounds[0].first;
    auto upperBound = bounds[bounds.Size() - 1].second;

    std::vector<ISortedStorePtr> stores;
    std::vector<TSharedRange<TRowRange>> boundsPerStore;

    tabletSnapshot->WaitOnLocks(timestamp);

    if (tabletThrottlerKind) {
        ThrowUponDistributedThrottlerOverdraft(*tabletThrottlerKind, tabletSnapshot, chunkReadOptions);
    }

    // Pick stores which intersect [lowerBound, upperBound) (excluding upperBound).
    auto takePartition = [&] (const std::vector<ISortedStorePtr>& candidateStores) {
        for (const auto& store : candidateStores) {
            const auto* begin = std::upper_bound(
                bounds.begin(),
                bounds.end(),
                store->GetMinKey().Get(),
                [] (TUnversionedRow lhs, const TRowRange& rhs) {
                    return lhs < rhs.second;
                });

            const auto* end = std::lower_bound(
                bounds.begin(),
                bounds.end(),
                store->GetUpperBoundKey().Get(),
                [] (const TRowRange& lhs, TUnversionedRow rhs) {
                    return lhs.first < rhs;
                });

            if (begin != end) {
                auto offsetBegin = std::distance(bounds.begin(), begin);
                auto offsetEnd = std::distance(bounds.begin(), end);

                stores.push_back(store);
                boundsPerStore.push_back(bounds.Slice(offsetBegin, offsetEnd));
            }
        }
    };

    takePartition(tabletSnapshot->GetEdenStores());

    auto range = tabletSnapshot->GetIntersectingPartitions(lowerBound, upperBound);
    for (const auto* it = range.first; it != range.second; ++it) {
        takePartition((*it)->Stores);
    }

    if (std::ssize(stores) > tabletSnapshot->Settings.MountConfig->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", stores.size())
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Settings.MountConfig->MaxReadFanIn);
    }

    YT_LOG_DEBUG("Creating schemaful sorted tablet reader (TabletId: %v, CellId: %v, Timestamp: %v, "
        "LowerBound: %v, UpperBound: %v, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v, "
        "StoreRanges: %v, BoundCount: %v, MergeVersionedRows: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        timestamp,
        lowerBound,
        upperBound,
        chunkReadOptions.WorkloadDescriptor,
        chunkReadOptions.ReadSessionId,
        MakeFormattableView(stores, TStoreIdFormatter()),
        MakeFormattableView(stores, TStoreRangeFormatter()),
        bounds.Size(),
        mergeVersionedRows);

    std::vector<TLegacyOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());

        if (Y_UNLIKELY(!mergeVersionedRows)) {
            auto type = store->GetType();

            THROW_ERROR_EXCEPTION_IF(type != EStoreType::SortedDynamic && type != EStoreType::SortedChunk,
                "Expected a sorted table when not merging versioned rows");

            if (type == EStoreType::SortedChunk) {
                auto format = FromProto<EChunkFormat>(store->AsSortedChunk()->GetChunkMeta().format());
                THROW_ERROR_EXCEPTION_IF(format != EChunkFormat::TableVersionedColumnar,
                    "Expected chunks with %Qlv format when not merging versioned rows",
                    EChunkFormat::TableVersionedColumnar);
            }
        }
    }

    ISchemafulUnversionedReaderPtr reader;

    auto nestedSchema = NRowMerger::GetNestedColumnsSchema(*tabletSnapshot->QuerySchema);
    auto enrichedColumnFilter = EnrichColumnFilter(
        columnFilter,
        nestedSchema,
        (mergeVersionedRows
            ? tabletSnapshot->QuerySchema->GetKeyColumnCount()
            : 0));

    auto overlyingColumnFilter = mergeVersionedRows
        ? ToLatestTimestampColumnFilter(
            columnFilter,
            timestampReadOptions,
            tabletSnapshot->QuerySchema->GetColumnCount())
        : columnFilter;

    if (mergeVersionedRows) {
        auto rowMerger = CreateQueryLatestTimestampRowMerger(
            New<TRowBuffer>(TTabletReaderPoolTag()),
            tabletSnapshot,
            overlyingColumnFilter,
            timestampRange.RetentionTimestamp,
            timestampReadOptions);

        reader = NRowMerger::CreateSchemafulOverlappingRangeReader(
            boundaries,
            std::move(rowMerger),
            [
                =,
                stores = std::move(stores),
                boundsPerStore = std::move(boundsPerStore)
            ] (int index) {
                YT_ASSERT(index < std::ssize(stores));

                return stores[index]->CreateReader(
                    tabletSnapshot,
                    boundsPerStore[index],
                    timestamp,
                    false,
                    enrichedColumnFilter,
                    chunkReadOptions,
                    workloadCategory);
            },
            [keyComparer = tabletSnapshot->RowKeyComparer] (TUnversionedValueRange lhs, TUnversionedValueRange rhs) {
                return keyComparer(lhs, rhs);
            });
    } else {
        auto getNextReader = [
            =,
            timestampReadOptions = std::move(timestampReadOptions),
            stores = std::move(stores),
            boundsPerStore = std::move(boundsPerStore),
            index = 0
        ] () mutable -> ISchemafulUnversionedReaderPtr {
            if (index == std::ssize(stores)) {
                return nullptr;
            }

            auto underlyingReader = stores[index]->CreateReader(
                tabletSnapshot,
                boundsPerStore[index],
                timestamp,
                /*produceAllVersions*/ false,
                enrichedColumnFilter,
                chunkReadOptions,
                workloadCategory);
            index++;

            return New<TUnversifyingReader>(
                std::move(underlyingReader),
                New<TRowBuffer>(TTabletReaderPoolTag()),
                tabletSnapshot->QuerySchema.get(),
                columnFilter,
                GetNestedColumnsSchema(*tabletSnapshot->QuerySchema));
        };

        reader = CreateUnorderedSchemafulReader(getNextReader, boundaries.size());
    }

    return WrapSchemafulTabletReader(
        tabletThrottlerKind,
        tabletSnapshot,
        chunkReadOptions,
        overlyingColumnFilter,
        std::move(reader));
}

ISchemafulUnversionedReaderPtr CreateSchemafulOrderedTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TReadTimestampRange timestampRange,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory)
{
    // Deduce tablet index and row range from lower and upper bound.
    YT_VERIFY(lowerBound.GetCount() >= 1);
    YT_VERIFY(upperBound.GetCount() >= 1);

    if (tabletThrottlerKind) {
        ThrowUponDistributedThrottlerOverdraft(*tabletThrottlerKind, tabletSnapshot, chunkReadOptions);
    }

    static constexpr i64 Infinity = std::numeric_limits<i64>::max() / 2;

    auto valueToInt = [] (const TUnversionedValue& value) {
        switch (value.Type) {
            case EValueType::Int64:
                return std::clamp(value.Data.Int64, -Infinity, +Infinity);
            case EValueType::Min:
                return -Infinity;
            case EValueType::Max:
                return +Infinity;
            default:
                YT_ABORT();
        }
    };

    int tabletIndex = 0;
    i64 lowerRowIndex = 0;
    i64 upperRowIndex = Infinity;
    if (lowerBound < upperBound) {
        if (lowerBound[0].Type == EValueType::Min) {
            tabletIndex = 0;
        } else {
            YT_VERIFY(lowerBound[0].Type == EValueType::Int64);
            tabletIndex = static_cast<int>(lowerBound[0].Data.Int64);
        }

        YT_VERIFY(upperBound[0].Type == EValueType::Int64 ||
            upperBound[0].Type == EValueType::Max);
        YT_VERIFY(upperBound[0].Type != EValueType::Int64 ||
            tabletIndex == upperBound[0].Data.Int64 ||
            tabletIndex + 1 == upperBound[0].Data.Int64);

        if (lowerBound.GetCount() >= 2) {
            lowerRowIndex = valueToInt(lowerBound[1]);
            if (lowerBound.GetCount() >= 3) {
                ++lowerRowIndex;
            }
        }

        if (upperBound.GetCount() >= 2) {
            upperRowIndex = valueToInt(upperBound[1]);
            if (upperBound.GetCount() >= 3) {
                ++upperRowIndex;
            }
        }
    }

    i64 trimmedRowCount = tabletSnapshot->TabletRuntimeData->TrimmedRowCount;
    lowerRowIndex = std::max(lowerRowIndex, trimmedRowCount);

    std::vector<int> storeIndices;
    const auto& allStores = tabletSnapshot->OrderedStores;
    if (lowerRowIndex < upperRowIndex && !allStores.empty()) {
        const auto* lowerIt = std::upper_bound(
            allStores.begin(),
            allStores.end(),
            lowerRowIndex,
            [] (i64 lhs, const IOrderedStorePtr& rhs) {
                return lhs < rhs->GetStartingRowIndex();
            }) - 1;
        const auto* it = lowerIt;
        while (it != allStores.end()) {
            const auto& store = *it;
            if (store->GetStartingRowIndex() >= upperRowIndex) {
                break;
            }
            storeIndices.push_back(std::distance(allStores.begin(), it));
            ++it;
        }
    }

    YT_LOG_DEBUG("Creating schemaful ordered tablet reader (TabletId: %v, CellId: %v, "
        "LowerRowIndex: %v, UpperRowIndex: %v, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        lowerRowIndex,
        upperRowIndex,
        chunkReadOptions.WorkloadDescriptor,
        chunkReadOptions.ReadSessionId,
        MakeFormattableView(storeIndices, [&] (auto* builder, int storeIndex) {
            FormatValue(builder, allStores[storeIndex]->GetId(), TStringBuf());
        }));

    std::vector<std::function<ISchemafulUnversionedReaderPtr()>> readers;
    for (int storeIndex : storeIndices) {
        auto store = allStores[storeIndex];
        readers.emplace_back([=, store = std::move(store)] {
            return store->CreateReader(
                tabletSnapshot,
                tabletIndex,
                lowerRowIndex,
                upperRowIndex,
                timestampRange.Timestamp,
                columnFilter,
                chunkReadOptions,
                workloadCategory);
        });
    }

    auto reader = CreateSchemafulConcatenatingReader(std::move(readers));

    if (tabletSnapshot->CommitOrdering == NTransactionClient::ECommitOrdering::Strong &&
        tabletSnapshot->Settings.MountConfig->RetryReadOnOrderedStoreRotation)
    {
        reader = CreateFailingOnRotationReader(std::move(reader), tabletSnapshot);
    }

    return WrapSchemafulTabletReader(
        tabletThrottlerKind,
        tabletSnapshot,
        chunkReadOptions,
        columnFilter,
        std::move(reader));
}

ISchemafulUnversionedReaderPtr CreateSchemafulRangeTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TReadTimestampRange timestampRange,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory)
{
    if (tabletSnapshot->PhysicalSchema->IsSorted()) {
        return CreateSchemafulSortedTabletReader(
            tabletSnapshot,
            columnFilter,
            MakeSingletonRowRange(lowerBound, upperBound),
            timestampRange,
            chunkReadOptions,
            tabletThrottlerKind,
            workloadCategory,
            /*timestampReadOptions*/ {});
    } else {
        return CreateSchemafulOrderedTabletReader(
            tabletSnapshot,
            columnFilter,
            std::move(lowerBound),
            std::move(upperBound),
            timestampRange,
            chunkReadOptions,
            tabletThrottlerKind,
            workloadCategory);
    }
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateCompactionTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const TClientChunkReadOptions& chunkReadOptions,
    int minConcurrency,
    ETabletDistributedThrottlerKind tabletThrottlerKind,
    IThroughputThrottlerPtr perTabletThrottler,
    std::optional<EWorkloadCategory> workloadCategory,
    IMemoryUsageTrackerPtr rowMergerMemoryTracker)
{
    if (!tabletSnapshot->PhysicalSchema->IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted",
            tabletSnapshot->TableId);
    }

    tabletSnapshot->WaitOnLocks(majorTimestamp);

    auto throttler = perTabletThrottler;

    if (const auto& distributedThrottler = tabletSnapshot->DistributedThrottlers[tabletThrottlerKind]) {
        throttler = NConcurrency::CreateCombinedThrottler({
            perTabletThrottler,
            distributedThrottler,
        });
    }

    auto asyncResult = throttler->Throttle(1);
    if (asyncResult.IsSet()) {
        asyncResult.Get().ThrowOnError();
    } else {
        YT_LOG_DEBUG("Started waiting for compaction inbound throughput throttler");
        WaitFor(asyncResult)
            .ThrowOnError();
        YT_LOG_DEBUG("Finished waiting for compaction inbound throughput throttler");
    }

    YT_LOG_DEBUG(
        "Creating versioned tablet reader (TabletId: %v, CellId: %v, LowerBound: %v, UpperBound: %v, "
        "CurrentTimestamp: %v, MajorTimestamp: %v, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v, StoreRanges: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        lowerBound,
        upperBound,
        currentTimestamp,
        majorTimestamp,
        chunkReadOptions.WorkloadDescriptor,
        chunkReadOptions.ReadSessionId,
        MakeFormattableView(stores, TStoreIdFormatter()),
        MakeFormattableView(stores, TStoreRangeFormatter()));

    const auto& mountConfig = tabletSnapshot->Settings.MountConfig;

    auto rowMerger = NRowMerger::CreateVersionedRowMerger(
        mountConfig->RowMergerType,
        New<TRowBuffer>(TTabletReaderPoolTag()),
        tabletSnapshot->QuerySchema,
        TColumnFilter(),
        mountConfig,
        currentTimestamp,
        majorTimestamp,
        tabletSnapshot->ColumnEvaluator,
        tabletSnapshot->CustomRuntimeData,
        /*mergeRowsOnFlush*/ false,
        /*useTtlColumn*/ true,
        /*mergeDeletionsOnFlush*/ false,
        mountConfig->NestedRowDiscardPolicy,
        std::move(rowMergerMemoryTracker));

    std::vector<TLegacyOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    auto reader = NRowMerger::CreateVersionedOverlappingRangeReader(
        boundaries,
        std::move(rowMerger),
        [
            =,
            stores = std::move(stores),
            lowerBound = std::move(lowerBound),
            upperBound = std::move(upperBound)
        ] (int index) {
            YT_VERIFY(index < std::ssize(stores));
            const auto& store = stores[index];
            return store->CreateReader(
                tabletSnapshot,
                MakeSingletonRowRange(lowerBound, upperBound),
                AllCommittedTimestamp,
                true,
                TColumnFilter(),
                chunkReadOptions,
                workloadCategory);
        },
        [keyComparer = tabletSnapshot->RowKeyComparer] (TUnversionedValueRange lhs, TUnversionedValueRange rhs) {
            return keyComparer(lhs, rhs);
        },
        minConcurrency);

    if (throttler) {
        return New<TThrottlerAwareVersionedReader>(
            std::move(reader),
            std::move(throttler));
    } else {
        return reader;
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NRowMerger::TSchemafulRowMerger> CreateQueryLatestTimestampRowMerger(
    TRowBufferPtr rowBuffer,
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& latestTimestampColumnFilter,
    TTimestamp retentionTimestamp,
    const TTimestampReadOptions& timestampReadOptions)
{
    auto createRowMerger = [&] (int columnCount, const TColumnFilter& rowMergerColumnFilter) {
        return std::make_unique<NRowMerger::TSchemafulRowMerger>(
            std::move(rowBuffer),
            columnCount,
            tabletSnapshot->QuerySchema->GetKeyColumnCount(),
            rowMergerColumnFilter,
            tabletSnapshot->ColumnEvaluator,
            retentionTimestamp,
            timestampReadOptions.TimestampColumnMapping,
            NRowMerger::GetNestedColumnsSchema(*tabletSnapshot->QuerySchema));
    };

    if (timestampReadOptions.TimestampColumnMapping.empty()) {
        return createRowMerger(tabletSnapshot->QuerySchema->GetColumnCount(), latestTimestampColumnFilter);
    } else {
        return createRowMerger(
            // Add timestamp column for every value column.
            tabletSnapshot->QuerySchema->GetColumnCount() + tabletSnapshot->QuerySchema->GetValueColumnCount(),
            latestTimestampColumnFilter);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
