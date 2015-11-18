#include "lfalloc_helpers.h"

#include <thread>
#include <mutex>

#include <dlfcn.h>

namespace NYT {
namespace NLFAlloc {

////////////////////////////////////////////////////////////////////////////////

static std::once_flag BindWithLFAllocFlag;
static i64 (*DynamicGetLFAllocCounterFast)(int) = nullptr;
static i64 (*DynamicGetLFAllocCounterFull)(int) = nullptr;

i64 DummyGetLFAllocCounter(int)
{
    return 0;
}

void BindWithLFAllocImpl()
{
#ifdef _unix_
    DynamicGetLFAllocCounterFast = reinterpret_cast<i64(*)(int)>(dlsym(nullptr, "GetLFAllocCounterFast"));
    DynamicGetLFAllocCounterFull = reinterpret_cast<i64(*)(int)>(dlsym(nullptr, "GetLFAllocCounterFull"));
#endif
    if (!DynamicGetLFAllocCounterFast) {
        DynamicGetLFAllocCounterFast = &DummyGetLFAllocCounter;
    }
    if (!DynamicGetLFAllocCounterFull) {
        DynamicGetLFAllocCounterFull = &DummyGetLFAllocCounter;
    }
}

void BindWithLFAllocOnce()
{
    std::call_once(BindWithLFAllocFlag, &BindWithLFAllocImpl);
}

// Copied from library/lfalloc.
enum {
    CT_USER_ALLOC,      // accumulated size requested by user code
    CT_MMAP,            // accumulated mmapped size
    CT_MMAP_CNT,        // number of mmapped regions
    CT_MUNMAP,          // accumulated unmmapped size
    CT_MUNMAP_CNT,      // number of munmaped regions
    CT_SYSTEM_ALLOC,    // accumulated allocated size for internal lfalloc needs
    CT_SYSTEM_FREE,     // accumulated deallocated size for internal lfalloc needs
    CT_SMALL_ALLOC,     // accumulated allocated size for fixed-size blocks
    CT_SMALL_FREE,      // accumulated deallocated size for fixed-size blocks
    CT_LARGE_ALLOC,     // accumulated allocated size for large blocks
    CT_LARGE_FREE,      // accumulated deallocated size for large blocks
    CT_MAX
};

i64 GetCurrentUsed()
{
    BindWithLFAllocOnce();
    return GetCurrentLargeBlocks() + GetCurrentSmallBlocks() + GetCurrentSystem();
}

i64 GetCurrentMmapped()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_MMAP) - DynamicGetLFAllocCounterFull(CT_MUNMAP);
}

i64 GetCurrentMmappedCount()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_MMAP_CNT) - DynamicGetLFAllocCounterFull(CT_MUNMAP_CNT);
}

i64 GetCurrentLargeBlocks()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_LARGE_ALLOC) - DynamicGetLFAllocCounterFull(CT_LARGE_FREE);
}

i64 GetCurrentSmallBlocks()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_SMALL_ALLOC) - DynamicGetLFAllocCounterFull(CT_SMALL_FREE);
}

i64 GetCurrentSystem()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_SYSTEM_ALLOC) - DynamicGetLFAllocCounterFull(CT_SYSTEM_FREE);
}

#define IMPL(fn, ct) \
i64 fn() { BindWithLFAllocOnce(); return DynamicGetLFAllocCounterFull(ct); }
IMPL(GetUserAllocated, CT_USER_ALLOC);
IMPL(GetMmapped, CT_MMAP);
IMPL(GetMmappedCount, CT_MMAP_CNT);
IMPL(GetMunmapped, CT_MUNMAP);
IMPL(GetMunmappedCount, CT_MUNMAP_CNT);
IMPL(GetSystemAllocated, CT_SYSTEM_ALLOC);
IMPL(GetSystemFreed, CT_SYSTEM_FREE);
IMPL(GetSmallBlocksAllocated, CT_SMALL_ALLOC);
IMPL(GetSmallBlocksFreed, CT_SMALL_FREE);
IMPL(GetLargeBlocksAllocated, CT_LARGE_ALLOC);
IMPL(GetLargeBlocksFreed, CT_LARGE_FREE);
#undef IMPL

////////////////////////////////////////////////////////////////////////////////

} // namespace NLFAlloc
} // namespace NYT
