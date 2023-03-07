#pragma once

#include <yt/core/profiling/profiler.h>

#include <yt/ytlib/scheduler/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! This class contains all controller-related profiling. It is effectively singletone.
//! TODO(max42): move this to controller agent bootstrap when it is finally
//! separate from scheduler.
class TControllerAgentCounterManager
{
public:
    TControllerAgentCounterManager();

    void IncrementAssertionsFailed(NScheduler::EOperationType operationType);

    static TControllerAgentCounterManager* Get();

private:
    TEnumIndexedVector<NScheduler::EOperationType, NProfiling::TMonotonicCounter> AssertionsFailed_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
