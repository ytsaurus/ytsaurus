#pragma once

#include <yt/core/profiling/profiler.h>

#include <yt/ytlib/scheduler/public.h>

namespace NYT {
namespace NControllerAgent {

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
    TEnumIndexedVector<NProfiling::TSimpleCounter, NScheduler::EOperationType> AssertionsFailed_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
