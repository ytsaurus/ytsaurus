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

class TSmallArena;
class TLargeArena;

/////////////////////////////////////////////////////////////////////////////

class TSlabAllocator
{
public:
    explicit TSlabAllocator(IMemoryUsageTrackerPtr memoryTracker = nullptr);

    void* Allocate(size_t size);
    static void Free(void* ptr);

private:
    struct TSmallArenaDeleter
    {
        void operator() (TSmallArena* arena);
    };

    using TSmallArenaPtr = std::unique_ptr<TSmallArena, TSmallArenaDeleter>;

    struct TLargeArenaDeleter
    {
        void operator() (TLargeArena* arena);
    };

    using TLargeArenaPtr = std::unique_ptr<TLargeArena, TLargeArenaDeleter>;

    TSmallArenaPtr SmallArenas_[NYTAlloc::SmallRankCount];
    TLargeArenaPtr LargeArena_;
};

bool IsReallocationNeeded(const void* ptr);

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT

