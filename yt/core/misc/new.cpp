#include "stdafx.h"
#include "common.h"
#include "new.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRefCountedTypeCookie GetRefCountedTypeCookie(
	TRefCountedTypeKey typeKey,
	const TSourceLocation& location)
{
    return TRefCountedTracker::Get()->GetCookie(typeKey, location);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
