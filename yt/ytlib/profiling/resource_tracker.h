#pragma once

#include "public.h"

#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NProfiling {

#if !defined(_win_) && !defined(_darwin_)

////////////////////////////////////////////////////////////////////////////////

class TResourceTracker
    : public TRefCounted
{
public:
    explicit TResourceTracker(IInvokerPtr invoker);

    void Start();

private:
    i64 TicksPerSecond;
    TInstant LastUpdateTime;

    yhash_map<Stroka, i64> PreviousUserJiffies;
    yhash_map<Stroka, i64> PreviousSystemJiffies;

    TPeriodicInvokerPtr PeriodicInvoker;
    static const TDuration UpdateInterval;

    void EnqueueUsage();

    void EnqueueCpuUsage();
    void EnqueueMemoryUsage();
    void EnqueueLfAllocCounters();

};

////////////////////////////////////////////////////////////////////////////////

#endif

} // namespace NProfiling
} // namespace NYT
