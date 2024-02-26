#include "row_cache.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/versioned_row_merger.h>

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
    FlushIndex_.store(storeFlushIndex);
}

TUpdateCacheStatistics TRowCache::UpdateItems(
    TRange<NTableClient::TVersionedRow> rows,
    NTableClient::TTimestamp retainedTimestamp,
    NTableClient::IVersionedRowMerger* compactionRowMerger,
    ui32 storeFlushIndex,
    const NLogging::TLogger& Logger)
{
    auto lookuper = Cache_.GetLookuper();
    auto secondaryLookuper = Cache_.GetSecondaryLookuper();

    TUpdateCacheStatistics statistics;

    auto currentTime = GetInstant();

    for (auto row : rows) {
        auto foundItemRef = lookuper(row);

        if (auto foundItem = foundItemRef.Get()) {
            foundItem = GetLatestRow(std::move(foundItem));

            ++statistics.FoundRows;
            YT_VERIFY(foundItem->GetVersionedRow().GetKeyCount() > 0);

            bool sealed = foundItemRef.IsSealed();

            if (foundItem->Outdated) {
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
                foundItem->Outdated.store(true);
                ++statistics.FailedByMemoryRows;
                continue;
            }

            updatedItem->UpdatedInFlush = foundItem->UpdatedInFlush + 1;
            updatedItem->InsertTime = foundItem->InsertTime;
            updatedItem->UpdateTime = currentTime;

            YT_LOG_TRACE("Updating cache (Row: %v, Outdated: %v, StoreFlushIndex: %v)",
                updatedItem->GetVersionedRow(),
                foundItem->Outdated.load(),
                storeFlushIndex);

            YT_VERIFY(!foundItem->Updated.Exchange(updatedItem));

            // Otherwise will be updated after seal.
            if (sealed) {
                foundItemRef.Replace(updatedItem, true);

                if (secondaryLookuper.GetPrimary() && secondaryLookuper.GetPrimary() != foundItemRef.Origin) {
                    if (auto foundItemRef = secondaryLookuper(row)) {
                        foundItemRef.Replace(updatedItem);
                    }
                }
            }
        }
    }

    return statistics;
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
                    YT_VERIFY(!item->Updated.Exchange(newItem));
                    ++newItem->Reallocated;
                    itemRef.Replace(newItem, itemRef.IsSealed());
                }
            } else {
                itemRef.Replace(std::move(item), head.Get(), itemRef.IsSealed());
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
