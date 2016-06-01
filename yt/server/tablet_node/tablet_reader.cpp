#include "tablet_reader.h"
#include "private.h"
#include "config.h"
#include "partition.h"
#include "store.h"
#include "tablet.h"
#include "tablet_slot.h"

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/row_merger.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>
#include <yt/ytlib/table_client/schemaful_overlapping_chunk_reader.h>
#include <yt/ytlib/table_client/unordered_schemaful_reader.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/heap.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/range.h>

namespace NYT {
namespace NTabletNode {

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

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor)
{
    std::vector<ISortedStorePtr> stores;

    // Pick stores which intersect [lowerBound, upperBound) (excluding upperBound).
    auto takePartition = [&] (const TPartitionSnapshotPtr& partitionSnapshot) {
        for (const auto& store : partitionSnapshot->Stores) {
            if (store->GetMinKey() < upperBound && store->GetMaxKey() >= lowerBound) {
                stores.push_back(store);
            }
        }
    };

    takePartition(tabletSnapshot->Eden);

    auto range = tabletSnapshot->GetIntersectingPartitions(lowerBound, upperBound);
    for (auto it = range.first; it != range.second; ++it) {
        takePartition(*it);
    }

    LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %v, "
        "LowerBound: %v, UpperBound: %v, WorkloadDescriptor: %v, StoreIds: %v, StoreRanges: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        timestamp,
        lowerBound,
        upperBound,
        workloadDescriptor,
        MakeFormattableRange(stores, TStoreIdFormatter()),
        MakeFormattableRange(stores, TStoreRangeFormatter()));

    if (stores.size() > tabletSnapshot->Config->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", stores.size())
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Config->MaxReadFanIn);
    }

    auto rowMerger = New<TSchemafulRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->Schema.Columns().size(),
        tabletSnapshot->KeyColumns.size(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    return CreateSchemafulOverlappingRangeChunkReader(
        std::move(boundaries),
        std::move(rowMerger),
        [=, stores = std::move(stores)] (int index) {
            Y_ASSERT(index < stores.size());
            return stores[index]->CreateReader(
                lowerBound,
                upperBound,
                timestamp,
                columnFilter,
                workloadDescriptor);
        },
        [keyComparer = tabletSnapshot->RowKeyComparer] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd)
        {
            return keyComparer(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        });
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
    TRowBufferPtr rowBuffer)
{
    auto minKey = *keys.Begin();
    auto maxKey = *(keys.End() - 1);
    std::vector<ISortedStorePtr> stores;

    // Pick stores which intersect [minKey, maxKey] (including maxKey).
    auto takePartition = [&] (const TPartitionSnapshotPtr& partitionSnapshot) {
        Y_ASSERT(partitionSnapshot);
        for (const auto& store : partitionSnapshot->Stores) {
            if (store->GetMinKey() <= maxKey && store->GetMaxKey() >= minKey) {
                stores.push_back(store);
            }
        }
    };

    takePartition(tabletSnapshot->Eden);
    takePartition(paritionSnapshot);

    LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %v, WorkloadDescriptor: %v, "
        "StoreIds: %v, StoreRanges: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        timestamp,
        workloadDescriptor,
        MakeFormattableRange(stores, TStoreIdFormatter()),
        MakeFormattableRange(stores, TStoreRangeFormatter()));

    auto rowMerger = New<TSchemafulRowMerger>(
        rowBuffer
            ? std::move(rowBuffer)
            : New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->Schema.Columns().size(),
        tabletSnapshot->KeyColumns.size(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    return CreateSchemafulOverlappingLookupChunkReader(
        std::move(rowMerger),
        [=, stores = std::move(stores), index = 0] () mutable -> IVersionedReaderPtr {
            if (index < stores.size()) {
                return stores[index++]->CreateReader(
                    keys,
                    timestamp,
                    columnFilter,
                    workloadDescriptor);
            } else {
                return nullptr;
            }
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    int concurrency,
    TRowBufferPtr rowBuffer)
{
    YCHECK(!rowBuffer || concurrency == 1);

    std::vector<TPartitionSnapshotPtr> partitions;
    std::vector<TSharedRange<TKey>> partitionedKeys;
    auto currentIt = keys.Begin();
    while (currentIt != keys.End()) {
        auto nextPartitionIt = std::upper_bound(
            tabletSnapshot->Partitions.begin(),
            tabletSnapshot->Partitions.end(),
            *currentIt,
            [] (TKey lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            });
        YCHECK(nextPartitionIt != tabletSnapshot->Partitions.begin());
        auto nextIt = nextPartitionIt == tabletSnapshot->Partitions.end()
            ? keys.End()
            : std::lower_bound(currentIt, keys.End(), (*nextPartitionIt)->PivotKey);
        partitions.push_back(*(nextPartitionIt - 1));
        partitionedKeys.push_back(keys.Slice(currentIt, nextIt));
        currentIt = nextIt;
    }

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
                rowBuffer);
            ++index;
            return reader;
        } else {
            return nullptr;
        }
    };

    return CreateUnorderedSchemafulReader(std::move(readerFactory), concurrency);
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedTabletReader(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const TWorkloadDescriptor& workloadDescriptor)
{
    LOG_DEBUG(
        "Creating versioned tablet reader (TabletId: %v, CellId: %v, LowerBound: %v, UpperBound: %v, "
        "CurrentTimestamp: %v, MajorTimestamp: %v, WorkloadDescriptor: %v, StoreIds: %v, StoreRanges: %v)",
        tabletSnapshot->TabletId,
        tabletSnapshot->CellId,
        lowerBound,
        upperBound,
        currentTimestamp,
        majorTimestamp,
        workloadDescriptor,
        MakeFormattableRange(stores, TStoreIdFormatter()),
        MakeFormattableRange(stores, TStoreRangeFormatter()));

    auto rowMerger = New<TVersionedRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->KeyColumns.size(),
        tabletSnapshot->Config,
        currentTimestamp,
        majorTimestamp,
        tabletSnapshot->ColumnEvaluator);

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    return CreateVersionedOverlappingRangeChunkReader(
        std::move(boundaries),
        std::move(rowMerger),
        [=, stores = std::move(stores)] (int index) {
            Y_ASSERT(index < stores.size());
            return stores[index]->CreateReader(
                lowerBound,
                upperBound,
                AllCommittedTimestamp,
                TColumnFilter(),
                workloadDescriptor);
        },
        [keyComparer = tabletSnapshot->RowKeyComparer] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd)
        {
            return keyComparer(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

