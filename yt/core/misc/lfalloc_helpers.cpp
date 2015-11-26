#include "lfalloc_helpers.h"

namespace NYT {
namespace NLFAlloc {

////////////////////////////////////////////////////////////////////////////////

i64 GetCurrentUsed()
{
    return GetCurrentLargeBlocks() + GetCurrentSmallBlocks() + GetCurrentSystem();
}

i64 GetCurrentMmapped()
{
    return GetLFAllocCounterFull(CT_MMAP) - GetLFAllocCounterFull(CT_MUNMAP);
}

i64 GetCurrentMmappedCount()
{
    return GetLFAllocCounterFull(CT_MMAP_CNT) - GetLFAllocCounterFull(CT_MUNMAP_CNT);
}

i64 GetCurrentLargeBlocks()
{
    return GetLFAllocCounterFull(CT_LARGE_ALLOC) - GetLFAllocCounterFull(CT_LARGE_FREE);
}

i64 GetCurrentSmallBlocks()
{
    return GetLFAllocCounterFull(CT_SMALL_ALLOC) - GetLFAllocCounterFull(CT_SMALL_FREE);
}

i64 GetCurrentSystem()
{
    return GetLFAllocCounterFull(CT_SYSTEM_ALLOC) - GetLFAllocCounterFull(CT_SYSTEM_FREE);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLFAlloc
} // namespace NYT
