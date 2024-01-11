#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

struct IActionQueue
    : public virtual TRefCounted
{
    virtual void Schedule(TClosure callback, TDuration delay) = 0;

    virtual void Run() = 0;
    virtual void RunUntil(TInstant until) = 0;
    virtual void RunFor(TDuration duration) = 0;

    virtual void Stop() = 0;
    virtual bool IsStopped() const = 0;

    virtual TInstant GetNow() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IActionQueue)

////////////////////////////////////////////////////////////////////////////////

IActionQueuePtr CreateActionQueue(TInstant now);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
