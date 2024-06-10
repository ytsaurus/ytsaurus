#pragma once

#include "fwd.h"

#include "concurrency_transforms.h"

#include <yt/cpp/roren/interface/roren.h>
#include "stateful_timer_impl/stateful_timer_par_do.h"
#include "vcpu_metrics.h"


#include <library/cpp/yt/memory/ref.h>
#include <yt/yt/core/actions/public.h>

#include <util/generic/ptr.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using TExecutionBlockConfig = std::variant<TSerializedBlockConfig, TConcurrentBlockConfig>;

////////////////////////////////////////////////////////////////////////////////

// Block of an execution graph that runs with particular concurrency mode.
class IExecutionBlock
    : public TThrRefBase
{
public:
    using TDoStartedFuture = NYT::TFuture<void>;

    virtual ~IExecutionBlock() = default;

    virtual void BindToInvoker(NYT::IInvokerPtr invoker) = 0;

    virtual IExecutionBlockPtr Clone() = 0;

    // This API is supposed to be used as follows.
    //
    // 1. Call Start(...), returned future will be set once all processing is done (after Finish() is called)
    //    if error occurs while processing (e.g. user code inside ParDo thorws error) future will be set prematurely with error.
    // 2. Multiple calls to AsyncDo(), returned future will be set once processing is started (before Do is called).
    // 3. Call Finish()
    // 4. Once future returned from Start() is set we are ready to next round.
    virtual NYT::TFuture<void> Start(IExecutionContextPtr executionContext) = 0;
    virtual TDoStartedFuture AsyncDo(NYT::TSharedRef data) = 0;
    virtual void AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers) = 0;
    virtual void AsyncFinish() = 0;

    virtual const std::vector<IExecutionBlockPtr>& GetOutputBlocks() const = 0;

    virtual TBlockPipeConfig GetInputPipeConfiguration() const = 0;
    virtual TRowVtable GetInputRowVtable() const = 0;

    TString GetDebugDescription() const;
    virtual void WriteDebugDescription(IOutputStream* out, const TString& indent = "") const = 0;

    virtual const THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr>& GetTimersCallbacks() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr CreateSerializedExecutionBlock(
        TBlockPipeConfig config,
        IRawParDoPtr parDo,
        THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr> timersCallbacks,
        std::vector<IExecutionBlockPtr> outputs = {},
        TVCpuMetricsPtr vcpuMetrics = {});

IExecutionBlockPtr CreateSerializedExecutionBlock(
    TBlockPipeConfig config,
    IRawParDoPtr parDo,
    THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr> timersCallbacks,
    std::vector<IRawOutputPtr> outputs,
    TVCpuMetricsPtr vcpuMetrics = {});

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr CreateConcurrentExecutionBlock(
        TBlockPipeConfig config,
        IRawParDoPtr parDo,
        std::vector<IExecutionBlockPtr> outputs,
        TVCpuMetricsPtr vcpuMetrics = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
