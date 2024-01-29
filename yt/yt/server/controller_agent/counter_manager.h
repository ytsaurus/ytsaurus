#pragma once

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

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
    TEnumIndexedArray<NScheduler::EOperationType, NProfiling::TCounter> AssertionsFailed_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
