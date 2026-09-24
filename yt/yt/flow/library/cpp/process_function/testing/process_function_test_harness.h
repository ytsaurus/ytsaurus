#pragma once

#include "recording_output_collector.h"
#include "test_runtime_context.h"
#include "test_state_environment.h"

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <vector>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Drives a process function through whole epochs the way the worker does, so a test can exercise
//! any function granularity without rebuilding the epoch loop. Runs Init once; each RunEpoch
//! preloads state, processes the input, runs the end-of-epoch sync and commits. Captures the
//! latest epoch's emitted messages and timers for assertions.
class TProcessFunctionTestHarness
{
public:
    TProcessFunctionTestHarness(
        TTestStateEnvironment& env,
        IProcessFunctionBasePtr function,
        IRuntimeContextPtr context = TTestRuntimeContextBuilder().Build());

    //! Constructs with and freezes the environment's current process-function context.
    //! Configure all constructor dependencies before calling Create<T>().
    template <class TFunction>
    static TProcessFunctionTestHarness Create(
        TTestStateEnvironment& environment,
        IRuntimeContextPtr runtimeContext = TTestRuntimeContextBuilder().Build())
    {
        auto processFunctionContext = environment.CreateProcessFunctionContext();
        auto function = environment.CreateProcessFunction<TFunction>(processFunctionContext);
        return TProcessFunctionTestHarness(
            environment,
            std::move(function),
            std::move(runtimeContext),
            std::move(processFunctionContext));
    }

    template <class TFunction>
    TIntrusivePtr<TFunction> GetFunction() const
    {
        auto function = DynamicPointerCast<TFunction>(Function_);
        YT_VERIFY(function);
        return function;
    }

    //! Runs one epoch over |input|.
    void RunEpoch(const IInputContextPtr& input);

    //! Runs one epoch over the given entities.
    void RunEpoch(
        std::vector<TInputMessageConstPtr> messages,
        std::vector<TInputTimerConstPtr> timers = {},
        std::vector<TInputVisitConstPtr> visits = {});

    //! Messages emitted during the most recent epoch.
    const std::vector<TRecordingOutputCollector::TRecordedMessage>& GetMessages() const;
    //! Timers emitted during the most recent epoch.
    const std::vector<TRecordingOutputCollector::TRecordedTimer>& GetTimers() const;

    const IRuntimeContextPtr& GetContext() const;

private:
    TTestStateEnvironment& Env_;
    const IProcessFunctionBasePtr Function_;
    const IBatchProcessFunctionPtr Batch_;
    //! Set when the function has an end-of-epoch sync phase.
    ISyncProcessFunction* const SyncFunction_;
    const IRuntimeContextPtr Context_;
    TProcessFunctionContextPtr FrozenProcessFunctionContext_;

    TIntrusivePtr<TRecordingOutputCollector> Output_;
    bool Initialized_ = false;

    void EnsureInitialized();

    TProcessFunctionTestHarness(
        TTestStateEnvironment& env,
        IProcessFunctionBasePtr function,
        IRuntimeContextPtr context,
        TProcessFunctionContextPtr processFunctionContext);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
