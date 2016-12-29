#include "ref_counted.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void RefCountedTrackerAllocate(
    TRefCountedTypeCookie cookie,
    size_t instanceSize)
{
    TRefCountedTracker::Get()->Allocate(cookie, instanceSize);
}

void RefCountedTrackerFree(
    TRefCountedTypeCookie cookie,
    size_t instanceSize)
{
    TRefCountedTracker::Get()->Free(cookie, instanceSize);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
