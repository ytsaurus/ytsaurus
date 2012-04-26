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
    TResourceTracker(IInvoker::TPtr invoker);

    void EnqueueUsage();

    void Start();

private:
    i64 PreviousProcTicks;
    yhash_map<Stroka, i64> PreviousUserTicks;
    yhash_map<Stroka, i64> PreviousKernelTicks;

    TPeriodicInvoker::TPtr PeriodicInvoker;
    static const TDuration UpdateInterval;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NProfiling
} // namespace NYT
