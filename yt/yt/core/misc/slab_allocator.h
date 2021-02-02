#pragma once

#include "common.h"
#include "free_list.h"
#include "format.h"
#include "error.h"
#include "memory_usage_tracker.h"

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <array>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

class TArenaPool
    : public TRefTracked<TArenaPool>
{
public:
    struct TFreeListItem
        : public TFreeListItemBase<TFreeListItem>
    { };

    using TSimpleFreeList = TFreeList<TFreeListItem>;

    TArenaPool(
        size_t rank,
        size_t segmentSize,
        IMemoryUsageTrackerPtr memoryTracker);

    ~TArenaPool();

    void* Allocate();

    void Free(void* obj);

    size_t Unref();

private:
    const size_t ChunkSize_;
    const size_t BatchSize_;
    const IMemoryUsageTrackerPtr MemoryTracker_;

    TSimpleFreeList FreeList_;
    TSimpleFreeList Segments_;
    std::atomic<size_t> RefCount_ = {1};

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
