#pragma once

#include "cached_row.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/slab_allocator.h>
#include <yt/yt/core/misc/concurrent_cache.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRowCacheMemoryTracker)

struct TUpdateCacheStatistics
{
    int FoundRows = 0;
    int DiscardedRows = 0;
    int FailedByMemoryRows = 0;
};

class TRowCache
    : public TRefCounted
{
public:
    TRowCache(
        size_t elementCount,
        const NProfiling::TProfiler& profiler,
        IMemoryUsageTrackerPtr memoryTracker);

    TConcurrentCache<TCachedRow>* GetCache();

    TSlabAllocator* GetAllocator();

    ui32 GetFlushIndex() const;
    void SetFlushIndex(ui32 storeFlushIndex);

    TUpdateCacheStatistics UpdateItems(
        TRange<NTableClient::TVersionedRow> rows,
        NTableClient::TTimestamp retainedTimestamp,
        NTableClient::IVersionedRowMerger* compactionRowMerger,
        ui32 storeFlushIndex,
        const NLogging::TLogger& Logger);

    void ReallocateItems(const NLogging::TLogger& Logger);

    DEFINE_BYVAL_RW_PROPERTY(bool, ReallocatingItems, false);

    i64 GetUsedBytesCount() const;

private:
    const TIntrusivePtr<TRowCacheMemoryTracker> MemoryTracker_;

    THazardPtrReclaimGuard HazardPtrReclaimGuard_;

    TSlabAllocator Allocator_;
    TConcurrentCache<TCachedRow> Cache_;

    // Rows with revision less than FlushIndex are considered outdated.
    std::atomic<ui32> FlushIndex_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TRowCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
