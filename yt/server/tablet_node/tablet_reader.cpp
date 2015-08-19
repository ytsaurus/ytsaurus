#include "stdafx.h"
#include "tablet_reader.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "partition.h"
#include "store.h"
#include "config.h"
#include "private.h"

#include <ytlib/table_client/row_merger.h>
#include <ytlib/table_client/row_buffer.h>
#include <ytlib/table_client/schemaful_overlapping_chunk_reader.h>

#include <core/misc/error.h>

namespace NYT {
namespace NTabletNode {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TTabletReaderPoolTag { };

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TColumnFilter GetColumnFilter(const TTableSchema& schema, const TTableSchema& tabletSchema)
{
    // Infer column filter.
    TColumnFilter columnFilter;
    columnFilter.All = false;
    for (const auto& column : schema.Columns()) {
        const auto& tabletColumn = tabletSchema.GetColumnOrThrow(column.Name);
        if (tabletColumn.Type != column.Type) {
            THROW_ERROR_EXCEPTION("Invalid type of schema column %Qv: expected %Qlv, actual %Qlv",
                column.Name,
                tabletColumn.Type,
                column.Type);
        }
        columnFilter.Indexes.push_back(tabletSchema.GetColumnIndex(tabletColumn));
    }

    return columnFilter;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TTableSchema& schema,
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
        JoinToString(stores, [] (const IStorePtr& store) {
            return Stroka("<") + ToString(store->GetMinKey()) + ":" + ToString(store->GetMaxKey()) + ">";
        }));

    if (stores.size() > tabletSnapshot->Config->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", stores.size())
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Config->MaxReadFanIn);
    }

    auto columnFilter = GetColumnFilter(schema, tabletSnapshot->Schema);

    auto rowMerger = New<TSchemafulRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->Schema.Columns().size(),
        tabletSnapshot->KeyColumns.size(),
        columnFilter);

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    return CreateSchemafulOverlappingChunkReader(
        boundaries,
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
    const TTableSchema& schema,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp)
{
    TKey minKey;
    TKey maxKey;

    for (const auto& key : keys) {
        minKey = !minKey || key < minKey ? key : minKey;
        maxKey = !maxKey || key > maxKey ? key : maxKey;
    }

    std::vector<IStorePtr> stores;
    auto takePartition = [&] (const TPartitionSnapshotPtr& partitionSnapshot) {
        for (const auto& store : partitionSnapshot->Stores) {
            if (store->GetMinKey() <= maxKey && store->GetMaxKey() >= minKey) {
                stores.push_back(store);
            }
        }
    };

    takePartition(tabletSnapshot->Eden);

    std::vector<TPartitionSnapshotPtr> snapshots;

    for (auto key : keys) {
        snapshots.push_back(tabletSnapshot->FindContainingPartition(key));
    }

    std::sort(snapshots.begin(), snapshots.end());
    snapshots.erase(std::unique(snapshots.begin(), snapshots.end()), snapshots.end());

    for (const auto& snapshot : snapshots) {
        takePartition(snapshot);
    }

    LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %v, StoreIds: [%v])",
        tabletSnapshot->TabletId,
        tabletSnapshot->Slot->GetCellId(),
        timestamp,
        JoinToString(stores, TStoreIdFormatter()));

    if (stores.size() > tabletSnapshot->Config->MaxReadFanIn) {
        THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
            << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
            << TErrorAttribute("fan_in", stores.size())
            << TErrorAttribute("fan_in_limit", tabletSnapshot->Config->MaxReadFanIn);
    }

    auto columnFilter = GetColumnFilter(schema, tabletSnapshot->Schema);

    auto rowMerger = New<TSchemafulRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->Schema.Columns().size(),
        tabletSnapshot->KeyColumns.size(),
        columnFilter);

    return CreateSchemafulOverlappingChunkLookupReader(
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
        JoinToString(stores, [] (const IStorePtr& store) {
            return Stroka("<") + ToString(store->GetMinKey()) + ":" + ToString(store->GetMaxKey()) + ">";
        }));

    auto rowMerger = New<TVersionedRowMerger>(
        New<TRowBuffer>(TRefCountedTypeTag<TTabletReaderPoolTag>()),
        tabletSnapshot->KeyColumns.size(),
        tabletSnapshot->Config,
        currentTimestamp,
        majorTimestamp);

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(stores.size());
    for (const auto& store : stores) {
        boundaries.push_back(store->GetMinKey());
    }

    return CreateVersionedOverlappingChunkReader(
        boundaries,
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

