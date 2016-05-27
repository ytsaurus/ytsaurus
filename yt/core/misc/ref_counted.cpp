#include "ref_counted.h"
#include "ref_counted_tracker.h"

#include <yt/core/misc/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRefCountedBase::~TRefCountedBase()
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    FinalizeTracking();
#endif
}

#ifdef YT_ENABLE_REF_COUNTED_TRACKING

void TRefCountedBase::InitializeTracking(TRefCountedTypeCookie typeCookie, size_t instanceSize)
{
    Y_ASSERT(TypeCookie_ == NullRefCountedTypeCookie);
    TypeCookie_ = typeCookie;

    Y_ASSERT(InstanceSize_ == 0);
    Y_ASSERT(instanceSize != 0);
    InstanceSize_ = instanceSize;

    TRefCountedTracker::Get()->Allocate(typeCookie, instanceSize);
}

void TRefCountedBase::FinalizeTracking()
{
    Y_ASSERT(TypeCookie_ != NullRefCountedTypeCookie);
    Y_ASSERT(InstanceSize_ != 0);
    TRefCountedTracker::Get()->Free(TypeCookie_, InstanceSize_);
}

#endif

////////////////////////////////////////////////////////////////////////////////

TExtrinsicRefCounted::TExtrinsicRefCounted()
    : RefCounter_(new NDetail::TRefCounter(this))
{ }

TExtrinsicRefCounted::~TExtrinsicRefCounted()
{
    // There are two common mistakes that may lead to triggering the checks below:
    // - Improper allocation/deallocation of ref-counted objects, e.g.
    //   creating an instance with raw "new" and deleting it afterwards with raw "delete"
    //   (possibly inside auto_ptr, unique_ptr, shared_ptr or similar helpers),
    //   or declaring an instance with static or automatic durations.
    // - Throwing an exception from ctor.
    Y_ASSERT(RefCounter_->GetRefCount() == 0);
}

////////////////////////////////////////////////////////////////////////////////

TIntrinsicRefCounted::TIntrinsicRefCounted()
    : RefCounter_(1)
{ }

TIntrinsicRefCounted::~TIntrinsicRefCounted()
{
    // For failed assertions, see the comments in TExtrinsicRefCounted::~TExtrinsicRefCounted.
    Y_ASSERT(RefCounter_.load() == 0);
}

////////////////////////////////////////////////////////////////////////////////

void NDetail::TRefCounter::Dispose()
{
    delete That_;
}

void NDetail::TRefCounter::Destroy()
{
    delete this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
