#include "row_cache.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/row_merger.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TRowCacheMemoryTracker
    : public IMemoryUsageTracker
{
public:
    explicit TRowCacheMemoryTracker(IMemoryUsageTrackerPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    TError TryAcquire(i64 size) override
    {
        auto result = Underlying_->TryAcquire(size);
        if (result.IsOK()) {
            Size_ += size;
        }
        return result;
    }

    TError TryChange(i64 size) override
    {
        auto result = Underlying_->TryChange(size);
        if (result.IsOK()) {
            Size_ += size;
        }
        return result;
    }

    bool Acquire(i64 size) override
    {
        bool result = Underlying_->Acquire(size);
        Size_ += size;

        return result;
    }

    void Release(i64 size) override
    {
        Underlying_->Release(size);
        Size_ -= size;
    }

    void SetLimit(i64 size) override
    {
        Underlying_->SetLimit(size);
    }

    i64 GetUsedBytesCount()
    {
        return Size_.load();
    }

private:
    const IMemoryUsageTrackerPtr Underlying_;
    std::atomic<i64> Size_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TRowCacheMemoryTracker)

////////////////////////////////////////////////////////////////////////////////

TRowCache::TRowCache(
    size_t elementCount,
    const NProfiling::TProfiler& profiler,
    IMemoryUsageTrackerPtr memoryTracker)
    : MemoryTracker_(New<TRowCacheMemoryTracker>(std::move(memoryTracker)))
    , Allocator_(profiler.WithPrefix("/slab_allocator"), MemoryTracker_)
    , Cache_(elementCount)
{ }

TConcurrentCache<TCachedRow>* TRowCache::GetCache()
{
    return &Cache_;
}

TSlabAllocator* TRowCache::GetAllocator()
{
    return &Allocator_;
}

ui32 TRowCache::GetFlushIndex() const
{
    return FlushIndex_.load(std::memory_order::acquire);
}

void TRowCache::SetFlushIndex(ui32 storeFlushIndex)
{
    auto currentFlushIndex = FlushIndex_.load(std::memory_order::acquire);
    // Check that stores are flushed in proper order.
    // Revisions are equal if retrying flush.
    YT_VERIFY(currentFlushIndex <= storeFlushIndex);
    FlushIndex_.store(storeFlushIndex, std::memory_order::release);
}

void TRowCache::UpdateItems(
    TRange<NTableClient::TVersionedRow> rows,
    NTableClient::TTimestamp retainedTimestamp,
    NTableClient::TVersionedRowMerger* compactionRowMerger,
    ui32 storeFlushIndex,
    const NLogging::TLogger& Logger)
{
    auto lookuper = Cache_.GetLookuper();
    auto secondaryLookuper = Cache_.GetSecondaryLookuper();

    for (auto row : rows) {
        auto foundItemRef = lookuper(row);

        if (auto foundItem = foundItemRef.Get()) {
            foundItem = GetLatestRow(std::move(foundItem));

            YT_VERIFY(foundItem->GetVersionedRow().GetKeyCount() > 0);

            // Row is inserted in lookup thread in two steps.
            // Initially it is inserted with last known flush revision of passive dynamic stores.
            //
            // Cached row revision is updated to maximum value after insertion
            // if RowCache->FlushIndex is still not greater than cached row initial revision.
            // Otherwise the second step of insertion is failed and inserted row becomes outdated.
            // Its revision is also checked when reading it in lookup thread.
            //
            // If updating revision to maximum value takes too long time it can be canceled by
            // the following logic.

            // Normally this condition is rare.
            if (foundItem->Revision.load(std::memory_order::acquire) < storeFlushIndex) {
                // No way to update row and preserve revision.
                // Discard its revision.
                // In lookup use CAS to update revision to Max.

                YT_LOG_TRACE("Discarding row (Row: %v, Revision: %v, StoreFlushIndex: %v)",
                    foundItem->GetVersionedRow(),
                    foundItem->Revision.load(),
                    storeFlushIndex);

                foundItem->Revision.store(std::numeric_limits<ui32>::min(), std::memory_order::release);
                continue;
            }

            compactionRowMerger->AddPartialRow(foundItem->GetVersionedRow());
            compactionRowMerger->AddPartialRow(row);
            auto mergedRow = compactionRowMerger->BuildMergedRow();

            YT_VERIFY(mergedRow);
            YT_VERIFY(mergedRow.GetKeyCount() > 0);

            auto updatedItem = CachedRowFromVersionedRow(
                &Allocator_,
                mergedRow,
                retainedTimestamp);

            if (!updatedItem) {
                // Not enough memory to allocate new item.
                // Make current item outdated.
                foundItem->Revision.store(std::numeric_limits<ui32>::min(), std::memory_order::release);
                continue;
            }

            YT_LOG_TRACE("Updating cache (Row: %v, Revision: %v, StoreFlushIndex: %v)",
                updatedItem->GetVersionedRow(),
                foundItem->Revision.load(),
                storeFlushIndex);

            updatedItem->Revision.store(std::numeric_limits<ui32>::max(), std::memory_order::release);

            YT_VERIFY(!foundItem->Updated.Exchange(updatedItem));

            foundItemRef.Update(updatedItem);

            if (secondaryLookuper.GetPrimary() && secondaryLookuper.GetPrimary() != foundItemRef.Origin) {
                if (auto foundItemRef = secondaryLookuper(row)) {
                    foundItemRef.Update(updatedItem);
                }
            }
        }
    }
}

void TRowCache::ReallocateItems(const NLogging::TLogger& Logger)
{
    THazardPtrReclaimGuard reclaimGuard;

    bool hasReallocatedArenas = Allocator_.ReallocateArenasIfNeeded();

    if (hasReallocatedArenas) {
        YT_LOG_DEBUG("Lookup cache reallocation started");

        int reallocatedRows = 0;
        auto onItem = [&] (auto itemRef) {
            auto head = itemRef.Get();
            auto item = GetLatestRow(head);
            auto memoryBegin = GetRefCounter(item.Get());

            if (TSlabAllocator::IsReallocationNeeded(memoryBegin)) {
                ++reallocatedRows;
                if (auto newItem = CopyCachedRow(&Allocator_, item.Get())) {
                    newItem->Revision.store(item->Revision.load(std::memory_order::acquire), std::memory_order::release);
                    YT_VERIFY(!item->Updated.Exchange(newItem));
                    itemRef.Update(newItem);
                }
            } else {
                itemRef.Update(std::move(item), head.Get());
            }
        };

        auto lookuper = Cache_.GetLookuper();

        // Reallocate secondary hash table at first because
        // rows can be concurrently moved into primary during lookup.
        if (auto secondaryHashTable = lookuper.GetSecondary()) {
            secondaryHashTable->ForEach(onItem);
        }

        auto hashTable = lookuper.GetPrimary();
        hashTable->ForEach(onItem);

        YT_LOG_DEBUG("Lookup cache reallocation finished (ReallocatedRows: %v)", reallocatedRows);
    }
}

i64 TRowCache::GetUsedBytesCount() const
{
    return MemoryTracker_->GetUsedBytesCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
