#pragma once

#include "public.h"

#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NProfiling {

#ifndef _win_

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

};

////////////////////////////////////////////////////////////////////////////////
            
#endif

} // namespace NProfiling
} // namespace NYT
