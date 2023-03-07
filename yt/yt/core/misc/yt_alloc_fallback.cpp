// This file contains the fallback implementations of YTAlloc-specific stuff.
// These implementations are annotated with Y_WEAK to ensure that if the actual YTAlloc
// is available at the link time, the latter is preferred over the fallback.
#include "yt_alloc.h"

#include <util/system/compiler.h>
#include <util/system/yassert.h>

#include <cstdlib>

#include <yt/core/misc/assert.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void* Allocate(size_t size)
{
    return ::malloc(size);
}

Y_WEAK void* AllocateSmall(size_t untaggedRank, size_t /*taggedRank*/)
{
    return ::malloc(SmallRankToSize[untaggedRank]);
}

Y_WEAK void Free(void* ptr)
{
    ::free(ptr);
}

Y_WEAK void FreeNonNull(void* ptr)
{
    YT_ASSERT(ptr);
    ::free(ptr);
}

Y_WEAK size_t GetAllocationSize(void* /*ptr*/)
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc

