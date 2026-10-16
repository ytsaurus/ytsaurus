#include "input_buffer.h"

#include "input_buffer_detail.h"

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

IInputBufferPtr CreateInputBuffer(
    TJobId jobId,
    NFlow::TStreamLimitUsageStateMap streamLimitUsageStates,
    NFlow::TEpochCycleTrackerPtr epochCycleTracker,
    THashMap<TStreamId, NFlow::TOfferedRateEstimatorPtr> offeredRateEstimators,
    TComputationSpecPtr computationSpec,
    TComputationId computationId,
    TDynamicComputationSpecPtr dynamicSpec,
    IInvokerPtr finalizerPoolInvoker,
    NProfiling::TProfiler profiler,
    std::function<TInstant()> timeProvider)
{
    return New<TInputBuffer>(
        jobId,
        std::move(streamLimitUsageStates),
        std::move(epochCycleTracker),
        std::move(offeredRateEstimators),
        std::move(computationSpec),
        std::move(computationId),
        std::move(dynamicSpec),
        std::move(finalizerPoolInvoker),
        std::move(profiler),
        std::move(timeProvider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
