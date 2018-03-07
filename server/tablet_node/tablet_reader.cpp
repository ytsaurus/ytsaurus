#include "tablet_reader.h"
#include "private.h"
#include "config.h"
#include "partition.h"
#include "store.h"
#include "tablet.h"
#include "tablet_slot.h"

#include <yt/ytlib/table_client/overlapping_reader.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/row_merger.h>
#include <yt/ytlib/table_client/schemaful_concatencaing_reader.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/unordered_schemaful_reader.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/heap.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/range.h>

namespace NYT {
namespace NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TTabletReaderPoolTag { };

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

struct TStoreRangeFormatter
{
    void operator()(TStringBuilder* builder, const ISortedStorePtr& store) const
    {
        builder->AppendFormat("<%v:%v>",
            store->GetMinKey(),
            store->GetMaxKey());
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulSortedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TRowRange>& bounds,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    const TReadSessionId& sessionId)
{
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);
    YCHECK(bounds.Size() > 0);
    auto lowerBound = bounds[0].first;
    auto upperBound = bounds[bounds.Size() - 1].second;

    std::vector<ISortedStorePtr> stores;
    std::vector<TSharedRange<TRowRange>> boundsPerStore;

    // Pick stores which intersect [lowerBound, upperBound) (excluding upperBound).
    auto takePartition = [&] (const std::vector<ISortedStorePtr>& candidateStores) {
        for (const auto& store : candidateStores) {
            auto begin = std::lower_bound(
                bounds.begin(),
                bounds.end(),
                store->GetMinKey().Get(),
                [] (const TRowRange& lhs, TUnversionedRow rhs) {
                    return lhs.second < rhs;
                });

            auto end = std::upper_bound(
                bounds.begin(),
                bounds.end(),
                store->GetMaxKey().Get(),
                [] (TUnversionedRow lhs, const TRowRange& rhs) {
                    return lhs < rhs.first;
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

    if (stores.size() > tabletSnapshot->Config->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", stores.size())
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Config->MaxReadFanIn);
    }

    LOG_DEBUG("Creating schemaful sorted tablet reader (TabletId: %v, CellId: %v, Timestamp: %llx, "
        "LowerBound: %v, UpperBound: %v, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v, StoreRanges: %v, BoundCount: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        timestamp,
        lowerBound,
        upperBound,
        workloadDescriptor,
        sessionId,
        MakeFormattableRange(stores, TStoreIdFormatter()),
        MakeFormattableRange(stores, TStoreRangeFormatter()),
        bounds.Size());

    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->QuerySchema.Columns().size(),
        tabletSnapshot->QuerySchema.GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    return CreateSchemafulOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        [=, stores = std::move(stores)] (int index) {
            Y_ASSERT(index < stores.size());

            return stores[index]->CreateReader(
                tabletSnapshot,
                boundsPerStore[index],
                timestamp,
                false,
                columnFilter,
                workloadDescriptor,
                sessionId);
        },
        [keyComparer = tabletSnapshot->RowKeyComparer] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd) {
            return keyComparer(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        });
}

ISchemafulReaderPtr CreateSchemafulOrderedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp /*timestamp*/,
    const TWorkloadDescriptor& workloadDescriptor,
    const TReadSessionId& sessionId)
{
    // Deduce tablet index and row range from lower and upper bound.
    YCHECK(lowerBound.GetCount() >= 1);
    YCHECK(upperBound.GetCount() >= 1);

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
                Y_UNREACHABLE();
        }
    };

    int tabletIndex = 0;
    i64 lowerRowIndex = 0;
    i64 upperRowIndex = std::numeric_limits<i64>::max();
    if (lowerBound < upperBound) {
        if (lowerBound[0].Type == EValueType::Min) {
            tabletIndex = 0;
        } else {
            YCHECK(lowerBound[0].Type == EValueType::Int64);
            tabletIndex = static_cast<int>(lowerBound[0].Data.Int64);
        }

        YCHECK(upperBound[0].Type == EValueType::Int64 ||
               upperBound[0].Type == EValueType::Max);
        YCHECK(upperBound[0].Type != EValueType::Int64 ||
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

    i64 trimmedRowCount = tabletSnapshot->RuntimeData->TrimmedRowCount;
    if (lowerRowIndex < trimmedRowCount) {
        lowerRowIndex = trimmedRowCount;
    }

    i64 totalRowCount = tabletSnapshot->RuntimeData->TotalRowCount;
    if (upperRowIndex > totalRowCount) {
        upperRowIndex = totalRowCount;
    }

    std::vector<IOrderedStorePtr> stores;
    if (lowerRowIndex < upperRowIndex && !tabletSnapshot->OrderedStores.empty()) {
        auto lowerIt = std::upper_bound(
            tabletSnapshot->OrderedStores.begin(),
            tabletSnapshot->OrderedStores.end(),
            lowerRowIndex,
            [] (i64 lhs, const IOrderedStorePtr& rhs) {
                return lhs < rhs->GetStartingRowIndex();
            }) - 1;
        auto it = lowerIt;
        while (it != tabletSnapshot->OrderedStores.end()) {
            const auto& store = *it;
            if (store->GetStartingRowIndex() >= upperRowIndex) {
                break;
            }
            stores.push_back(store);
            ++it;
        }
    }

    LOG_DEBUG("Creating schemaful ordered tablet reader (TabletId: %v, CellId: %v, "
        "LowerRowIndex: %v, UpperRowIndex: %v, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        lowerRowIndex,
        upperRowIndex,
        workloadDescriptor,
        sessionId,
        MakeFormattableRange(stores, TStoreIdFormatter()));

    std::vector<std::function<ISchemafulReaderPtr()>> readers;
    for (const auto& store : stores) {
        readers.emplace_back([=] () {
            return store->CreateReader(
                tabletSnapshot,
                tabletIndex,
                lowerRowIndex,
                upperRowIndex,
                columnFilter,
                workloadDescriptor,
                sessionId);
        });
    }

    return CreateSchemafulConcatenatingReader(readers);
}

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    const TReadSessionId& sessionId)
{
    if (tabletSnapshot->PhysicalSchema.IsSorted()) {
        return CreateSchemafulSortedTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            MakeSingletonRowRange(lowerBound, upperBound),
            timestamp,
            workloadDescriptor,
            sessionId);
    } else {
        return CreateSchemafulOrderedTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            std::move(lowerBound),
            std::move(upperBound),
            timestamp,
            workloadDescriptor,
            sessionId);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

ISchemafulReaderPtr CreateSchemafulPartitionReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TPartitionSnapshotPtr paritionSnapshot,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    const TReadSessionId& sessionId,
    TRowBufferPtr rowBuffer)
{
    auto minKey = *keys.Begin();
    auto maxKey = *(keys.End() - 1);
    std::vector<ISortedStorePtr> stores;

    // Pick stores which intersect [minKey, maxKey] (including maxKey).
    auto takeStores = [&] (const std::vector<ISortedStorePtr>& candidateStores) {
        for (const auto& store : candidateStores) {
            if (store->GetMinKey() <= maxKey && store->GetMaxKey() >= minKey) {
                stores.push_back(store);
            }
        }
    };

    takeStores(tabletSnapshot->GetEdenStores());
    takeStores(paritionSnapshot->Stores);

    LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %llx, WorkloadDescriptor: %v, "
        " ReadSessionId: %v, StoreIds: %v, StoreRanges: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        timestamp,
        workloadDescriptor,
        sessionId,
        MakeFormattableRange(stores, TStoreIdFormatter()),
        MakeFormattableRange(stores, TStoreRangeFormatter()));

    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        rowBuffer,
        tabletSnapshot->QuerySchema.Columns().size(),
        tabletSnapshot->QuerySchema.GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    return CreateSchemafulOverlappingLookupReader(
        std::move(rowMerger),
        [=, stores = std::move(stores), index = 0] () mutable -> IVersionedReaderPtr {
            if (index < stores.size()) {
                return stores[index++]->CreateReader(
                    tabletSnapshot,
                    keys,
                    timestamp,
                    false,
                    columnFilter,
                    workloadDescriptor,
                    sessionId);
            } else {
                return nullptr;
            }
        });
}

} // namespace

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    const TReadSessionId& sessionId)
{
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);

    if (!tabletSnapshot->PhysicalSchema.IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted",
            tabletSnapshot->TableId);
    }

    std::vector<TPartitionSnapshotPtr> partitions;
    std::vector<TSharedRange<TKey>> partitionedKeys;
    auto currentIt = keys.Begin();
    while (currentIt != keys.End()) {
        auto nextPartitionIt = std::upper_bound(
            tabletSnapshot->PartitionList.begin(),
            tabletSnapshot->PartitionList.end(),
            *currentIt,
            [] (TKey lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            });
        YCHECK(nextPartitionIt != tabletSnapshot->PartitionList.begin());
        auto nextIt = nextPartitionIt == tabletSnapshot->PartitionList.end()
            ? keys.End()
            : std::lower_bound(currentIt, keys.End(), (*nextPartitionIt)->PivotKey);
        partitions.push_back(*(nextPartitionIt - 1));
        partitionedKeys.push_back(keys.Slice(currentIt, nextIt));
        currentIt = nextIt;
    }

    auto rowBuffer = New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>());

    auto readerFactory = [
        =,
        tabletSnapshot = std::move(tabletSnapshot),
        columnFilter = std::move(columnFilter),
        partitions = std::move(partitions),
        partitionedKeys = std::move(partitionedKeys),
        rowBuffer = std::move(rowBuffer),
        index = 0
    ] () mutable -> ISchemafulReaderPtr {
        if (index < partitionedKeys.size()) {
            auto reader = CreateSchemafulPartitionReader(
                tabletSnapshot,
                columnFilter,
                partitions[index],
                partitionedKeys[index],
                timestamp,
                workloadDescriptor,
                sessionId,
                rowBuffer);
            ++index;
            return reader;
        } else {
            return nullptr;
        }
    };

    return CreatePrefetchingOrderedSchemafulReader(std::move(readerFactory));
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    const TReadSessionId& sessionId,
    int minConcurrency)
{
    if (!tabletSnapshot->PhysicalSchema.IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted",
            tabletSnapshot->TableId);
    }

    LOG_DEBUG(
        "Creating versioned tablet reader (TabletId: %v, CellId: %v, LowerBound: %v, UpperBound: %v, "
        "CurrentTimestamp: %llx, MajorTimestamp: %llx, WorkloadDescriptor: %v, ReadSessionId: %v, StoreIds: %v, StoreRanges: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        lowerBound,
        upperBound,
        currentTimestamp,
        majorTimestamp,
        workloadDescriptor,
        sessionId,
        MakeFormattableRange(stores, TStoreIdFormatter()),
        MakeFormattableRange(stores, TStoreRangeFormatter()));

    auto rowMerger = std::make_unique<TVersionedRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->QuerySchema.GetColumnCount(),
        tabletSnapshot->QuerySchema.GetKeyColumnCount(),
        TColumnFilter(),
        tabletSnapshot->Config,
        currentTimestamp,
        majorTimestamp,
        tabletSnapshot->ColumnEvaluator,
        false);

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    return CreateVersionedOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        [=, stores = std::move(stores)] (int index) {
            Y_ASSERT(index < stores.size());
            return stores[index]->CreateReader(
                tabletSnapshot,
                MakeSingletonRowRange(lowerBound, upperBound),
                AllCommittedTimestamp,
                true,
                TColumnFilter(),
                workloadDescriptor,
                sessionId);
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

