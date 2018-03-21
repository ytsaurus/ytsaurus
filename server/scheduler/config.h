#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ypath/public.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration LoopPeriod;
    TDuration FailedAllocationBackoffTime;
    int AllocationCommitConcurrency;

    TSchedulerConfig()
    {
        RegisterParameter("loop_period", LoopPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("failed_allocation_backoff_time", FailedAllocationBackoffTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("allocation_commit_concurrency", AllocationCommitConcurrency)
            .Default(256)
            .GreaterThanOrEqual(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
