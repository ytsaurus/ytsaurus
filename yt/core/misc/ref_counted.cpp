#include "ref_counted.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRefCountedTypeCookie TRefCountedTrackerFacade::GetCookie(
    TRefCountedTypeKey typeKey,
    size_t instanceSize,
    const TSourceLocation& location)
{
    return TRefCountedTracker::Get()->GetCookie(
        typeKey,
        instanceSize,
        location);
}

void TRefCountedTrackerFacade::AllocateInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::Get()->AllocateInstance(cookie);
}

void TRefCountedTrackerFacade::FreeInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::Get()->FreeInstance(cookie);
}

void TRefCountedTrackerFacade::AllocateTagInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::Get()->AllocateTagInstance(cookie);
}

void TRefCountedTrackerFacade::FreeTagInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::Get()->FreeTagInstance(cookie);
}

void TRefCountedTrackerFacade::AllocateSpace(TRefCountedTypeCookie cookie, size_t size)
{
    TRefCountedTracker::Get()->AllocateSpace(cookie, size);
}

void TRefCountedTrackerFacade::FreeSpace(TRefCountedTypeCookie cookie, size_t size)
{
    TRefCountedTracker::Get()->FreeSpace(cookie, size);
}

void TRefCountedTrackerFacade::ReallocateSpace(TRefCountedTypeCookie cookie, size_t freedSize, size_t allocatedSize)
{
    TRefCountedTracker::Get()->ReallocateSpace(cookie, freedSize, allocatedSize);
}

void TRefCountedTrackerFacade::Dump()
{
    fprintf(stderr, "%s", TRefCountedTracker::Get()->GetDebugInfo().data());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
