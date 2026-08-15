#include "adapter_test_context.h"

#include <yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/process_function/host/computation.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/algorithm.h>
#include <util/system/type_name.h>

#include <type_traits>
#include <vector>

namespace NYT::NFlow::NTesting {
namespace {

class TSyncProbeProcessFunction
    : public IProcessFunction
    , public ISyncProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        ++ProcessCallCount_;
    }

    void Sync(
        const IRetryableTransactionPtr& /*transaction*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        ++SyncCallCount_;
    }

    int GetProcessCallCount() const
    {
        return ProcessCallCount_;
    }

    int GetSyncCallCount() const
    {
        return SyncCallCount_;
    }

private:
    int ProcessCallCount_ = 0;
    int SyncCallCount_ = 0;
};

static_assert(std::is_base_of_v<
    TTransformOrderedSourceComputation,
    TProcessFunctionTransformOrderedSourceComputation>);
static_assert(TProcessFunctionTransformOrderedSourceComputation::RequiresProcessingFunction);
static_assert(TProcessFunctionTransformOrderedSourceComputation::InvokesProcessFunctionSync);

TEST(TProcessFunctionTransformOrderedSourceComputationTest, InvokesSyncAtEpochBoundary)
{
    TTestStateEnvironment environment;
    auto function = New<TSyncProbeProcessFunction>();
    TProcessFunctionTestHarness harness(environment, function);

    harness.RunEpoch(std::vector<TInputMessageConstPtr>{});

    EXPECT_EQ(0, function->GetProcessCallCount());
    EXPECT_EQ(1, function->GetSyncCallCount());
}

////////////////////////////////////////////////////////////////////////////////

// Registered under its own class so DoSyncGoesThroughTheRegisteredAdapter below cannot be
// satisfied by TSyncProbeProcessFunction's instance-level counters above: this instance is
// created by the registry inside the adapter's constructor, not injected by the test.
class TRegisteredSyncCountingProcessFunction
    : public IProcessFunction
    , public ISyncProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }

    void Sync(
        const IRetryableTransactionPtr& /*transaction*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        ++SyncCallCount;
    }

    static int SyncCallCount;
};

int TRegisteredSyncCountingProcessFunction::SyncCallCount = 0;

YT_FLOW_DEFINE_PROCESS_FUNCTION(TRegisteredSyncCountingProcessFunction);

// DoSync refreshes the runtime context from the epoch's watermark state before entering user
// code, so the test must seed one the way a run iteration would; ApplyPendingStates is
// protected, hence this exposing subclass of the registered adapter.
class TSeededAdapterComputation
    : public TProcessFunctionTransformOrderedSourceComputation
{
public:
    using TProcessFunctionTransformOrderedSourceComputation::ApplyPendingStates;
    using TProcessFunctionTransformOrderedSourceComputation::TProcessFunctionTransformOrderedSourceComputation;
};

// Unlike InvokesSyncAtEpochBoundary above (which goes through TProcessFunctionTestHarness, a
// raw IProcessFunction plus a dynamic_cast the harness does itself), this test builds the
// context a real job would supply and constructs the REGISTERED adapter class via its own
// public constructor, then calls its DoSync override directly — the same override the worker
// invokes in production.
TEST(TProcessFunctionTransformOrderedSourceComputationAdapterTest, DoSyncGoesThroughTheRegisteredAdapter)
{
    TRegisteredSyncCountingProcessFunction::SyncCallCount = 0;

    auto queue = New<NConcurrency::TActionQueue>("AdapterTest");
    auto invoker = queue->GetInvoker();

    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = "NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation";
    spec->ProcessingFunction = std::string(TypeName<TRegisteredSyncCountingProcessFunction>());

    auto context = MakeAdapterTestComputationContext(invoker, std::move(spec));
    auto dynamicContext = MakeAdapterTestDynamicComputationContext();

    TIntrusivePtr<TSeededAdapterComputation> computation;
    NConcurrency::WaitFor(
        BIND([&] {
            computation = New<TSeededAdapterComputation>(context, dynamicContext);
            computation->UpdateWatermarkState(New<TWatermarkState>());
            computation->ApplyPendingStates();
            computation->DoSync(/*transaction*/ nullptr);
        }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();

    EXPECT_EQ(1, TRegisteredSyncCountingProcessFunction::SyncCallCount);
}

////////////////////////////////////////////////////////////////////////////////

// Registered under its own class, like the sync counter above: the instance is created by the
// registry inside the adapter's constructor, so the sensor is reachable only through a static.
class TRegisteredProfiledProcessFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        Probe = initContext->GetProfiler().Counter("/probe");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }

    static NProfiling::TCounter Probe;
};

NProfiling::TCounter TRegisteredProfiledProcessFunction::Probe;

YT_FLOW_DEFINE_PROCESS_FUNCTION(TRegisteredProfiledProcessFunction);

// The profiler tests in process_function_ut.cpp stop at TTestStateEnvironment. This one covers
// the production wiring: TComputationContext::Profiler reaching the function's Init through the
// registered adapter's DoInit, under the computation's own prefix.
TEST(TProcessFunctionTransformOrderedSourceComputationAdapterTest, DoInitHandsTheContextProfilerToTheFunction)
{
    TRegisteredProfiledProcessFunction::Probe = {};

    auto queue = New<NConcurrency::TActionQueue>("ProfilerAdapterTest");
    auto invoker = queue->GetInvoker();

    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = "NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation";
    spec->ProcessingFunction = std::string(TypeName<TRegisteredProfiledProcessFunction>());

    auto registry = New<NProfiling::TSolomonRegistry>();
    auto context = MakeAdapterTestComputationContext(invoker, std::move(spec));
    context->Profiler = NProfiling::TProfiler(registry, "/computation");
    auto dynamicContext = MakeAdapterTestDynamicComputationContext();

    TTestStateEnvironment environment;
    NConcurrency::WaitFor(
        BIND([&] {
            auto computation = New<TProcessFunctionTransformOrderedSourceComputation>(context, dynamicContext);
            computation->DoInit(environment.GetStateManager()->CreateContext());
        }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();

    EXPECT_TRUE(static_cast<bool>(TRegisteredProfiledProcessFunction::Probe));

    registry->SetWindowSize(12);
    registry->ProcessRegistrations();
    auto sensors = registry->ListSensors();
    EXPECT_TRUE(AnyOf(sensors, [] (const NProfiling::TSensorInfo& sensor) {
        return sensor.Name == "yt/computation/probe";
    }));
}

} // namespace
} // namespace NYT::NFlow::NTesting
