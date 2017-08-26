#include "new.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRefCountedTypeCookie GetRefCountedTypeCookie(
    TRefCountedTypeKey typeKey,
    size_t instanceSize,
    const TSourceLocation& location)
{
    return TRefCountedTracker::Get()->GetCookie(typeKey, instanceSize, location);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
