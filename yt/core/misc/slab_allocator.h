#pragma once

#include "common.h"
#include "lock_free_stack.h"
#include "format.h"
#include "error.h"
#include "memory_usage_tracker.h"
#include "allocator_traits.h"

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <array>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

class TArenaPool
    : public TDeleterBase
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

    static void Deallocate(TDeleterBase* deleter, void* obj)
    {
        auto self = static_cast<TArenaPool*>(deleter);
        self->Free(obj);
    }

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

    TDeleterBase* GetDeleter(size_t size);

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
