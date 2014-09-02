#include "stdafx.h"
#include "common.h"
#include "new.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRefCountedTypeCookie GetRefCountedTypeCookie(TRefCountedTypeKey key)
{
    return TRefCountedTracker::Get()->GetCookie(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
