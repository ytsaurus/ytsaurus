#include "input_buffer.h"

#include "input_buffer_detail.h"

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

IInputBufferPtr CreateInputBuffer(
    TJobId jobId,
    NFlow::TStreamLimitUsageStateMap streamLimitUsageStates,
    TComputationSpecPtr computationSpec,
    TComputationId computationId,
    TDynamicComputationSpecPtr dynamicSpec,
    IInvokerPtr finalizerPoolInvoker,
    NProfiling::TProfiler profiler)
{
    return New<TInputBuffer>(
        jobId,
        std::move(streamLimitUsageStates),
        std::move(computationSpec),
        std::move(computationId),
        std::move(dynamicSpec),
        std::move(finalizerPoolInvoker),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
