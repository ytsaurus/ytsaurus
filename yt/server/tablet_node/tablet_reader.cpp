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

namespace {

void StoreRangeFormatter(TStringBuilder* builder, const IStorePtr& store)
{
    builder->AppendFormat("<%v:%v>",
        store->GetMinKey(),
        store->GetMaxKey());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp)
{
    std::vector<IStorePtr> stores;
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
        "LowerBound: {%v}, UpperBound: {%v}, StoreIds: [%v], StoreRanges: {%v})",
        tabletSnapshot->TabletId,
        tabletSnapshot->Slot->GetCellId(),
        timestamp,
        lowerBound,
        upperBound,
        JoinToString(stores, TStoreIdFormatter()),
        JoinToString(stores, StoreRangeFormatter));

    if (stores.size() > tabletSnapshot->Config->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", stores.size())
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Config->MaxReadFanIn);
    }

    auto rowMerger = New<TSchemafulRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
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
            YASSERT(index < stores.size());
            return stores[index]->CreateReader(
                lowerBound,
                upperBound,
                timestamp,
                columnFilter);
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

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    int concurrency,
    TRowBufferPtr rowBuffer)
{
    YCHECK(!rowBuffer || concurrency == 1);

    auto takePartition = [&] (
        const TPartitionSnapshotPtr& partitionSnapshot,
        TKey minKey,
        TKey maxKey,
        std::vector<IStorePtr>* stores)
    {
        YASSERT(partitionSnapshot);
        for (const auto& store : partitionSnapshot->Stores) {
            if (store->GetMinKey() <= maxKey && store->GetMaxKey() >= minKey) {
                stores->push_back(store);
            }
        }
    };

    auto createPartitionReader = [=] (
        TPartitionSnapshotPtr partition,
        TSharedRange<TKey> keys) -> ISchemafulReaderPtr
    {
        TKey minKey = *keys.Begin();
        TKey maxKey = *(keys.End() - 1);

        std::vector<IStorePtr> stores;
        takePartition(tabletSnapshot->Eden, minKey, maxKey, &stores);
        takePartition(partition, minKey, maxKey, &stores);

        LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %v, StoreIds: [%v])",
            tabletSnapshot->TabletId,
            tabletSnapshot->Slot->GetCellId(),
            timestamp,
            JoinToString(stores, TStoreIdFormatter()));

        auto rowMerger = New<TSchemafulRowMerger>(
            rowBuffer
                ? std::move(rowBuffer)
                : New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
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
                        columnFilter);
                } else {
                    return nullptr;
                }
            });
    };

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
        partitions = std::move(partitions),
        partitionedKeys = std::move(partitionedKeys),
        createPartitionReader = std::move(createPartitionReader),
        index = 0
    ] () mutable -> ISchemafulReaderPtr {
        if (index < partitionedKeys.size()) {
            auto reader = createPartitionReader(partitions[index], partitionedKeys[index]);
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
    std::vector<IStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp)
{
    LOG_DEBUG(
        "Creating versioned tablet reader (TabletId: %v, CellId: %v, LowerBound: {%v}, UpperBound: {%v}, "
        "CurrentTimestamp: %v, MajorTimestamp: %v, StoreIds: [%v], StoreRanges: {%v})",
        tabletSnapshot->TabletId,
        tabletSnapshot->Slot->GetCellId(),
        lowerBound,
        upperBound,
        currentTimestamp,
        majorTimestamp,
        JoinToString(stores, TStoreIdFormatter()),
        JoinToString(stores, StoreRangeFormatter));

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
        [stores = std::move(stores), lowerBound, upperBound] (int index) {
            YASSERT(index < stores.size());
            return stores[index]->CreateReader(
                lowerBound,
                upperBound,
                AllCommittedTimestamp,
                TColumnFilter());
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

