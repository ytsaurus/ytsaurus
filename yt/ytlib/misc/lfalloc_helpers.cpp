#include "lfalloc_helpers.h"

#include <util/private/lfalloc/helpers.h>

namespace NYT {
namespace NLfAlloc {

////////////////////////////////////////////////////////////////////////////////

i64 GetCurrentUsed()
{
    return GetCurrentLargeBlocks() + GetCurrentSmallBlocks() + GetCurrentSystem();
}

i64 GetCurrentMmaped()
{
    return GetLFAllocCounterFull(CT_MMAP) - GetLFAllocCounterFull(CT_MUNMAP);
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

} // NLfAlloc
} // NYT
