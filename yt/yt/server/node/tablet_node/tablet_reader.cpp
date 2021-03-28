#include "tablet_reader.h"
#include "private.h"
#include "partition.h"
#include "store.h"
#include "tablet.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/table_client/overlapping_reader.h>
#include <yt/yt/ytlib/table_client/row_merger.h>
#include <yt/yt/ytlib/table_client/schemaful_concatencaing_reader.h>

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/chunked_memory_pool.h>
#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/range.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TTabletReaderPoolTag { };

static const auto& Logger = TabletNodeLogger;

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

void ThrottleUponOverdraft(
    ETabletDistributedThrottlerKind tabletThrottlerKind,
    const TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions)
{
    const auto& tabletThrottler = tabletSnapshot->DistributedThrottlers[tabletThrottlerKind];
    if (!tabletThrottler || !tabletThrottler->IsOverdraft()) {
        return;
    }

    YT_LOG_DEBUG("Started waiting for tablet throttler (TabletId: %v, ReadSessionId: %v, ThrottlerKind: %v)",
        tabletSnapshot->TabletId,
        chunkReadOptions.ReadSessionId,
        tabletThrottlerKind);

    NProfiling::TWallTimer throttlerWaitTimer;
    WaitFor(tabletThrottler->Throttle(1))
        .ThrowOnError();
    auto elapsedTime = throttlerWaitTimer.GetElapsedTime();

    YT_LOG_DEBUG("Finished waiting for tablet throttler (TabletId: %v, ReadSessionId: %v, ThrottlerKind: %v, ElapsedTime: %v)",
        tabletSnapshot->TabletId,
        chunkReadOptions.ReadSessionId,
        tabletThrottlerKind,
        elapsedTime);

    tabletSnapshot->TableProfiler->GetTabletCounters()->ThrottlerWaitTimers[tabletThrottlerKind]
        .Record(elapsedTime);
}

////////////////////////////////////////////////////////////////////////////////

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

    virtual typename TRowBatchTrait<TRow>::IRowBatchPtr Read(const TRowBatchReadOptions& options = {}) override
    {
        auto rawBatch = Underlying_->Read(options);

        auto currentDataWeight = Underlying_->GetDataStatistics().data_weight();
        YT_VERIFY(currentDataWeight >= ThrottledDataWeight_);
        Throttler_->Acquire(currentDataWeight - ThrottledDataWeight_);
        ThrottledDataWeight_ = currentDataWeight;

        return rawBatch;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return Underlying_->GetDecompressionStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return Underlying_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Underlying_->GetFailedChunkIds();
    }

    virtual TFuture<void> GetReadyEvent() const override
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

    virtual TFuture<void> Open() override
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

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateSchemafulSortedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TRowRange>& bounds,
    TTimestamp timestamp,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    IThroughputThrottlerPtr bandwidthThrottler)
{
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);
    YT_VERIFY(bounds.Size() > 0);
    auto lowerBound = bounds[0].first;
    auto upperBound = bounds[bounds.Size() - 1].second;

    std::vector<ISortedStorePtr> stores;
    std::vector<TSharedRange<TRowRange>> boundsPerStore;

    tabletSnapshot->WaitOnLocks(timestamp);

    if (tabletThrottlerKind) {
        ThrottleUponOverdraft(*tabletThrottlerKind, tabletSnapshot, chunkReadOptions);
    }

    // Pick stores which intersect [lowerBound, upperBound) (excluding upperBound).
    auto takePartition = [&] (const std::vector<ISortedStorePtr>& candidateStores) {
        for (const auto& store : candidateStores) {
            auto begin = std::upper_bound(
                bounds.begin(),
                bounds.end(),
                store->GetMinKey().Get(),
                [] (TUnversionedRow lhs, const TRowRange& rhs) {
                    return lhs < rhs.second;
                });

            auto end = std::lower_bound(
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
    for (auto it = range.first; it != range.second; ++it) {
        takePartition((*it)->Stores);
    }

    if (stores.size() > tabletSnapshot->MountConfig->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", stores.size())
            << TErrorAttribute("fan_in_limit", tabletSnapshot->MountConfig->MaxReadFanIn);
    }

    YT_LOG_DEBUG("Creating schemaful sorted tablet reader (TabletId: %v, CellId: %v, Timestamp: %llx, "
        "LowerBound: %v, UpperBound: %v, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v, StoreRanges: %v, BoundCount: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        timestamp,
        lowerBound,
        upperBound,
        chunkReadOptions.WorkloadDescriptor,
        chunkReadOptions.ReadSessionId,
        MakeFormattableView(stores, TStoreIdFormatter()),
        MakeFormattableView(stores, TStoreRangeFormatter()),
        bounds.Size());

    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        New<TRowBuffer>(TTabletReaderPoolTag()),
        tabletSnapshot->QuerySchema->Columns().size(),
        tabletSnapshot->QuerySchema->GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    std::vector<TLegacyOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    auto reader = CreateSchemafulOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        [
            =,
            stores = std::move(stores),
            boundsPerStore = std::move(boundsPerStore),
            bandwidthThrottler = std::move(bandwidthThrottler)
        ] (int index) {
            YT_ASSERT(index < stores.size());

            return stores[index]->CreateReader(
                tabletSnapshot,
                boundsPerStore[index],
                timestamp,
                false,
                columnFilter,
                chunkReadOptions,
                bandwidthThrottler);
        },
        [keyComparer = tabletSnapshot->RowKeyComparer] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd) {
            return keyComparer(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        });

    return MaybeWrapWithThrottlerAwareReader<TThrottlerAwareSchemafulUnversionedReader>(
        tabletThrottlerKind,
        tabletSnapshot,
        std::move(reader));
}

ISchemafulUnversionedReaderPtr CreateSchemafulOrderedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp /*timestamp*/,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    IThroughputThrottlerPtr bandwidthThrottler)
{
    // Deduce tablet index and row range from lower and upper bound.
    YT_VERIFY(lowerBound.GetCount() >= 1);
    YT_VERIFY(upperBound.GetCount() >= 1);

    if (tabletThrottlerKind) {
        ThrottleUponOverdraft(*tabletThrottlerKind, tabletSnapshot, chunkReadOptions);
    }

    const i64 infinity = std::numeric_limits<i64>::max() / 2;

    auto valueToInt = [] (const TUnversionedValue& value) {
        switch (value.Type) {
            case EValueType::Int64:
                return std::max(std::min(value.Data.Int64, +infinity), -infinity);
            case EValueType::Min:
                return -infinity;
            case EValueType::Max:
                return +infinity;
            default:
                YT_ABORT();
        }
    };

    int tabletIndex = 0;
    i64 lowerRowIndex = 0;
    i64 upperRowIndex = std::numeric_limits<i64>::max();
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
    if (lowerRowIndex < trimmedRowCount) {
        lowerRowIndex = trimmedRowCount;
    }

    i64 totalRowCount = tabletSnapshot->TabletRuntimeData->TotalRowCount;
    if (upperRowIndex > totalRowCount) {
        upperRowIndex = totalRowCount;
    }

    std::vector<int> storeIndices;
    const auto& allStores = tabletSnapshot->OrderedStores;
    if (lowerRowIndex < upperRowIndex && !allStores.empty()) {
        auto lowerIt = std::upper_bound(
            allStores.begin(),
            allStores.end(),
            lowerRowIndex,
            [] (i64 lhs, const IOrderedStorePtr& rhs) {
                return lhs < rhs->GetStartingRowIndex();
            }) - 1;
        auto it = lowerIt;
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
    for (auto storeIndex : storeIndices) {
        auto store = allStores[storeIndex];
        readers.emplace_back([=, store = std::move(store)] () {
            return store->CreateReader(
                tabletSnapshot,
                tabletIndex,
                lowerRowIndex,
                upperRowIndex,
                columnFilter,
                chunkReadOptions,
                bandwidthThrottler);
        });
    }

    auto reader = CreateSchemafulConcatenatingReader(std::move(readers));

    return MaybeWrapWithThrottlerAwareReader<TThrottlerAwareSchemafulUnversionedReader>(
        tabletThrottlerKind,
        tabletSnapshot,
        std::move(reader));
}

ISchemafulUnversionedReaderPtr CreateSchemafulRangeTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp timestamp,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    IThroughputThrottlerPtr bandwidthThrottler)
{
    if (tabletSnapshot->PhysicalSchema->IsSorted()) {
        return CreateSchemafulSortedTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            MakeSingletonRowRange(lowerBound, upperBound),
            timestamp,
            chunkReadOptions,
            tabletThrottlerKind,
            std::move(bandwidthThrottler));
    } else {
        return CreateSchemafulOrderedTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            std::move(lowerBound),
            std::move(upperBound),
            timestamp,
            chunkReadOptions,
            tabletThrottlerKind,
            std::move(bandwidthThrottler));
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

ISchemafulUnversionedReaderPtr CreateSchemafulPartitionReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TPartitionSnapshotPtr& partitionSnapshot,
    const TSharedRange<TLegacyKey>& keys,
    TTimestamp timestamp,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TRowBufferPtr rowBuffer,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    IThroughputThrottlerPtr bandwidthThrottler)
{
    auto minKey = *keys.Begin();
    auto maxKey = *(keys.End() - 1);
    std::vector<ISortedStorePtr> stores;

    // Pick stores which intersect [minKey, maxKey] (including maxKey).
    auto takeStores = [&] (const std::vector<ISortedStorePtr>& candidateStores) {
        for (const auto& store : candidateStores) {
            if (store->GetMinKey() <= maxKey && store->GetUpperBoundKey() > minKey) {
                stores.push_back(store);
            }
        }
    };

    takeStores(tabletSnapshot->GetEdenStores());
    takeStores(partitionSnapshot->Stores);

    YT_LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %llx, WorkloadDescriptor: %v, "
        " ReadSessionId: %v, StoreIds: %v, StoreRanges: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        timestamp,
        chunkReadOptions.WorkloadDescriptor,
        chunkReadOptions.ReadSessionId,
        MakeFormattableView(stores, TStoreIdFormatter()),
        MakeFormattableView(stores, TStoreRangeFormatter()));

    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        std::move(rowBuffer),
        tabletSnapshot->QuerySchema->Columns().size(),
        tabletSnapshot->QuerySchema->GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    return CreateSchemafulOverlappingLookupReader(
        std::move(rowMerger),
        [
            =,
            stores = std::move(stores),
            tabletSnapshot = std::move(tabletSnapshot),
            bandwidthThrottler = std::move(bandwidthThrottler),
            index = 0
        ] () mutable -> IVersionedReaderPtr {
            if (index < stores.size()) {
                return stores[index++]->CreateReader(
                    tabletSnapshot,
                    keys,
                    timestamp,
                    false,
                    columnFilter,
                    chunkReadOptions,
                    bandwidthThrottler);
            } else {
                return nullptr;
            }
        });
}

} // namespace

ISchemafulUnversionedReaderPtr CreateSchemafulLookupTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
    TTimestamp timestamp,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    IThroughputThrottlerPtr bandwidthThrottler)
{
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);

    tabletSnapshot->WaitOnLocks(timestamp);

    if (tabletThrottlerKind) {
        ThrottleUponOverdraft(*tabletThrottlerKind, tabletSnapshot, chunkReadOptions);
    }

    if (!tabletSnapshot->PhysicalSchema->IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted",
            tabletSnapshot->TableId);
    }

    std::vector<TPartitionSnapshotPtr> partitions;
    std::vector<TSharedRange<TLegacyKey>> partitionedKeys;
    auto currentIt = keys.Begin();
    while (currentIt != keys.End()) {
        auto nextPartitionIt = std::upper_bound(
            tabletSnapshot->PartitionList.begin(),
            tabletSnapshot->PartitionList.end(),
            *currentIt,
            [] (TLegacyKey lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            });
        YT_VERIFY(nextPartitionIt != tabletSnapshot->PartitionList.begin());
        auto nextIt = nextPartitionIt == tabletSnapshot->PartitionList.end()
            ? keys.End()
            : std::lower_bound(currentIt, keys.End(), (*nextPartitionIt)->PivotKey);
        partitions.push_back(*(nextPartitionIt - 1));
        partitionedKeys.push_back(keys.Slice(currentIt, nextIt));
        currentIt = nextIt;
    }

    auto rowBuffer = New<TRowBuffer>(TTabletReaderPoolTag());

    auto readerFactory = [
        =,
        columnFilter = std::move(columnFilter),
        partitions = std::move(partitions),
        partitionedKeys = std::move(partitionedKeys),
        rowBuffer = std::move(rowBuffer),
        index = 0,
        bandwidthThrottler = std::move(bandwidthThrottler)
    ] () mutable -> ISchemafulUnversionedReaderPtr {
        if (index < partitionedKeys.size()) {
            auto reader = CreateSchemafulPartitionReader(
                tabletSnapshot,
                columnFilter,
                partitions[index],
                partitionedKeys[index],
                timestamp,
                chunkReadOptions,
                rowBuffer,
                tabletThrottlerKind,
                bandwidthThrottler);
            ++index;
            return reader;
        } else {
            return nullptr;
        }
    };

    auto reader = CreatePrefetchingOrderedSchemafulReader(std::move(readerFactory));

    return MaybeWrapWithThrottlerAwareReader<TThrottlerAwareSchemafulUnversionedReader>(
        tabletThrottlerKind,
        tabletSnapshot,
        std::move(reader));
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    int minConcurrency,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    IThroughputThrottlerPtr bandwidthThrottler)
{
    if (!tabletSnapshot->PhysicalSchema->IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted",
            tabletSnapshot->TableId);
    }

    // XXX will this work?
    tabletSnapshot->WaitOnLocks(majorTimestamp);

    if (tabletThrottlerKind) {
        ThrottleUponOverdraft(*tabletThrottlerKind, tabletSnapshot, chunkReadOptions);
    }

    YT_LOG_DEBUG(
        "Creating versioned tablet reader (TabletId: %v, CellId: %v, LowerBound: %v, UpperBound: %v, "
        "CurrentTimestamp: %llx, MajorTimestamp: %llx, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v, StoreRanges: %v)",
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

    auto rowMerger = std::make_unique<TVersionedRowMerger>(
        New<TRowBuffer>(TTabletReaderPoolTag()),
        tabletSnapshot->QuerySchema->GetColumnCount(),
        tabletSnapshot->QuerySchema->GetKeyColumnCount(),
        TColumnFilter(),
        tabletSnapshot->MountConfig,
        currentTimestamp,
        majorTimestamp,
        tabletSnapshot->ColumnEvaluator,
        false,
        false);

    std::vector<TLegacyOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    auto reader = CreateVersionedOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        [
            =,
            stores = std::move(stores),
            lowerBound = std::move(lowerBound),
            upperBound = std::move(upperBound),
            bandwidthThrottler = std::move(bandwidthThrottler)
        ] (int index) {
            YT_ASSERT(index < stores.size());
            return stores[index]->CreateReader(
                tabletSnapshot,
                MakeSingletonRowRange(lowerBound, upperBound),
                AllCommittedTimestamp,
                true,
                TColumnFilter(),
                chunkReadOptions,
                bandwidthThrottler);
        },
        [keyComparer = tabletSnapshot->RowKeyComparer] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd)
        {
            return keyComparer(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        },
        minConcurrency);

    return MaybeWrapWithThrottlerAwareReader<TThrottlerAwareVersionedReader>(
        tabletThrottlerKind,
        tabletSnapshot,
        std::move(reader));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

