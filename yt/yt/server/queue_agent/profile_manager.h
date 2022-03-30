#pragma once

#include "private.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct IQueueProfileManager
    : public TRefCounted
{
    virtual void Profile(
        const TQueueSnapshotPtr& previousQueueSnapshot,
        const TQueueSnapshotPtr& currentQueueSnapshot) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueProfileManager);

////////////////////////////////////////////////////////////////////////////////

IQueueProfileManagerPtr CreateQueueProfileManager(
    IInvokerPtr invoker,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
