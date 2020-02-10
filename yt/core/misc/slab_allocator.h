#pragma once

#include <yt/core/misc/common.h>
#include <yt/core/misc/lock_free_stack.h>
#include <yt/core/misc/format.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/memory_usage_tracker.h>

#include <library/ytalloc/api/ytalloc.h>

#include <array>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

class TArenaPool
{
public:
    struct TFreeListItem
        : public TIntrusiveStackItem<TFreeListItem>
    { };

    using TFreeList = TIntrusiveLockFreeStack<TFreeListItem>;

    TArenaPool(
        size_t rank,
        size_t batchSize,
        IMemoryUsageTrackerPtr memoryTracker);

    ~TArenaPool();

    void* Allocate();

    void Free(void* obj);

    size_t Unref();

private:
    const size_t ChunkSize_;
    const size_t BatchSize_;
    const IMemoryUsageTrackerPtr MemoryTracker_;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    const TRefCountedTypeCookie Cookie_;
#endif

    TFreeList FreeList_;
    TFreeList Segments_;
    std::atomic<size_t> RefCount_ = {1};
    std::atomic<size_t> SegmentsCount_ = {0};

    void AllocateMore();
};

/////////////////////////////////////////////////////////////////////////////

class TSlabAllocator
{
public:
    explicit TSlabAllocator(IMemoryUsageTrackerPtr memoryTracker = nullptr);

    void* Allocate(size_t size);

    static void Free(void* ptr);

private:
    struct TArenaDeleter
    {
        void operator() (TArenaPool* arena)
        {
            arena->Unref();
        }
    };

    using TArenaPoolPtr = std::unique_ptr<TArenaPool, TArenaDeleter>;

    TArenaPoolPtr SmallArenas_[NYTAlloc::SmallRankCount];
    static constexpr size_t SegmentSize = 128_KB;
};

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
