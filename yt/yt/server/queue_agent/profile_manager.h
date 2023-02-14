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
    const NProfiling::TProfiler& profiler,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

struct IConsumerProfileManager
    : public TRefCounted
{
    virtual void Profile(
        const TConsumerSnapshotPtr& previousConsumerSnapshot,
        const TConsumerSnapshotPtr& currentConsumerSnapshot) = 0;
};

DEFINE_REFCOUNTED_TYPE(IConsumerProfileManager);

////////////////////////////////////////////////////////////////////////////////

IConsumerProfileManagerPtr CreateConsumerProfileManager(
    const NProfiling::TProfiler& profiler,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
