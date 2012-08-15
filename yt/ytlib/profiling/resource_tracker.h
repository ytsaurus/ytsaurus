#pragma once

#include "public.h"

#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TResourceTracker
    : public TRefCounted
{
public:
    explicit TResourceTracker(IInvokerPtr invoker);

    void Start();

private:
    i64 PreviousProcTicks;
    yhash_map<Stroka, i64> PreviousUserTicks;
    yhash_map<Stroka, i64> PreviousKernelTicks;

    TPeriodicInvokerPtr PeriodicInvoker;
    static const TDuration UpdateInterval;

    void EnqueueUsage();

    void EnqueueCpuUsage();
    void EnqueueMemoryUsage();

};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NProfiling
} // namespace NYT
