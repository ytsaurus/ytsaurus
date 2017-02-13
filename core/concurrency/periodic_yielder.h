#pragma once

#include "public.h"

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicYielder
{
public:
    explicit TPeriodicYielder(TDuration period);

    //! Returns true, if we have released the thread and got back to execution.
    bool TryYield();

private:
    const NProfiling::TCpuDuration Period_;
    NProfiling::TCpuInstant LastYieldTime_ = NProfiling::GetCpuInstant();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
