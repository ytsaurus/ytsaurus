#include "stdafx.h"
#include "common.h"
#include "ref_counted.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////


TExtrinsicRefCounted::~TExtrinsicRefCounted()
{
    // There are two common mistakes that may lead to triggering the checks below:
    // - Improper allocation/deallocation of ref-counted objects, e.g.
    //   creating an instance with raw "new" and deleting it afterwards with raw "delete"
    //   (possibly with the help of auto_ptr, unique_ptr, shared_ptr or similar helpers),
    //   or declaring an instance with static or automatic durations.
    // - Throwing an exception from ctor.
    YASSERT(RefCounter->GetRefCount() == 0);
#ifdef ENABLE_REF_COUNTED_TRACKING
    YASSERT(Cookie);
    TRefCountedTracker::Get()->Unregister(
        static_cast<TRefCountedTracker::TCookie>(Cookie));
#endif
}

TIntrinsicRefCounted::~TIntrinsicRefCounted()
{
    // For failed assertions, see the comments in TExtrinsicRefCounted::~TExtrinsicRefCounted.
    YASSERT(NDetail::AtomicallyFetch(&RefCounter) == 0);
#ifdef ENABLE_REF_COUNTED_TRACKING
    YASSERT(Cookie);
    TRefCountedTracker::Get()->Unregister(
        static_cast<TRefCountedTracker::TCookie>(Cookie));
#endif
}

#ifdef ENABLE_REF_COUNTED_TRACKING
void TExtrinsicRefCounted::BindToCookie(void* cookie)
{
    YASSERT(RefCounter->GetRefCount() > 0);
    YASSERT(!Cookie);
    Cookie = cookie;

    TRefCountedTracker::Get()->Register(
        static_cast<TRefCountedTracker::TCookie>(Cookie));
}

void TIntrinsicRefCounted::BindToCookie(void* cookie)
{
    YASSERT(NDetail::AtomicallyFetch(&RefCounter) > 0);
    YASSERT(!Cookie);
    Cookie = cookie;

    TRefCountedTracker::Get()->Register(
        static_cast<TRefCountedTracker::TCookie>(Cookie));
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT