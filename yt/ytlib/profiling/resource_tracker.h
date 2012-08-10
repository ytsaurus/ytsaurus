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
    std::unordered_map<Stroka, i64> PreviousUserTicks;
    std::unordered_map<Stroka, i64> PreviousKernelTicks;

    TPeriodicInvokerPtr PeriodicInvoker;
    static const TDuration UpdateInterval;

    void EnqueueUsage();

    void EnqueueMemoryUsage();

};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NProfiling
} // namespace NYT
