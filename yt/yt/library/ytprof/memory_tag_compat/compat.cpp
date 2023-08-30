#include <yt/yt/library/ytprof/heap_profiler.h>

#include <library/cpp/yt/memory/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

size_t GetMemoryUsageForTag(TMemoryTag tag)
{
    return NYTProf::GetEstimatedMemoryUsage(static_cast<NYTProf::TMemoryTag>(tag));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
