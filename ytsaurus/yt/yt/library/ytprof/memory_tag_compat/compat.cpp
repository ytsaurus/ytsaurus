#include <yt/yt/library/ytprof/heap_profiler.h>

#include <library/cpp/yt/memory/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMemoryTag GetCurrentMemoryTag()
{
    return static_cast<TMemoryTag>(NYTProf::GetMemoryTag());
}

void SetCurrentMemoryTag(TMemoryTag tag)
{
    NYTProf::SetMemoryTag(static_cast<NYTProf::TMemoryTag>(tag));
}

size_t GetMemoryUsageForTag(TMemoryTag tag)
{
    return NYTProf::GetEstimatedMemoryUsage(static_cast<NYTProf::TMemoryTag>(tag));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
