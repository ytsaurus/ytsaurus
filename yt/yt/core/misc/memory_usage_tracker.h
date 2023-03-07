#pragma once

#include "error.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IMemoryUsageTracker
    : public TIntrinsicRefCounted
{
    virtual TError TryAcquire(size_t size) = 0;
    virtual void Release(size_t size) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryUsageTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
