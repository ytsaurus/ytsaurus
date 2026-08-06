#include "adapter_test_context.h"

#include <yt/yt/flow/library/cpp/process_function/host/source_computation.h>
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

namespace NYT::NFlow::NTesting {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Registered under its own class: the instance is created by the registry inside the adapter's
// constructor, so the sensor is reachable only through a static.
class TRegisteredProfiledSourceProcessFunction
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

NProfiling::TCounter TRegisteredProfiledSourceProcessFunction::Probe;

YT_FLOW_DEFINE_PROCESS_FUNCTION(TRegisteredProfiledSourceProcessFunction);

// The source adapter used to carry its own copy of DoInit; it now shares the base's. This test
// keeps the profiler pinned on this adapter too, should the copy ever come back.
TEST(TProcessFunctionSourceComputationAdapterTest, DoInitHandsTheContextProfilerToTheFunction)
{
    TRegisteredProfiledSourceProcessFunction::Probe = {};

    auto queue = New<NConcurrency::TActionQueue>("ProfilerSourceAdapterTest");
    auto invoker = queue->GetInvoker();

    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = "NYT::NFlow::TProcessFunctionSourceComputation";
    spec->ProcessingFunction = std::string(TypeName<TRegisteredProfiledSourceProcessFunction>());

    auto registry = New<NProfiling::TSolomonRegistry>();
    auto context = MakeAdapterTestComputationContext(invoker, std::move(spec));
    context->Profiler = NProfiling::TProfiler(registry, "/computation");
    auto dynamicContext = MakeAdapterTestDynamicComputationContext();

    TTestStateEnvironment environment;
    NConcurrency::WaitFor(
        BIND([&] {
            auto computation = New<TProcessFunctionSourceComputation>(context, dynamicContext);
            computation->DoInit(environment.GetStateManager()->CreateContext());
        }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();

    EXPECT_TRUE(static_cast<bool>(TRegisteredProfiledSourceProcessFunction::Probe));

    registry->SetWindowSize(12);
    registry->ProcessRegistrations();
    auto sensors = registry->ListSensors();
    EXPECT_TRUE(AnyOf(sensors, [] (const NProfiling::TSensorInfo& sensor) {
        return sensor.Name == "yt/computation/probe";
    }));
}

////////////////////////////////////////////////////////////////////////////////

// Registered under its own class, like the profiled function above: the adapter creates the
// instance through the registry, so the observed id is reachable only through a static.
class TRegisteredPartitionAwareProcessFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        ObservedPartitionId = initContext->GetPartitionId();
    }

    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }

    static TPartitionId ObservedPartitionId;
};

TPartitionId TRegisteredPartitionAwareProcessFunction::ObservedPartitionId;

YT_FLOW_DEFINE_PROCESS_FUNCTION(TRegisteredPartitionAwareProcessFunction);

// The id the function sees is the hosting computation's, not the state manager's: the test
// environment generates one of its own, and the assertion pins which of the two arrives.
TEST(TProcessFunctionSourceComputationAdapterTest, DoInitHandsTheComputationPartitionIdToTheFunction)
{
    TRegisteredPartitionAwareProcessFunction::ObservedPartitionId = {};

    auto queue = New<NConcurrency::TActionQueue>("PartitionSourceAdapterTest");
    auto invoker = queue->GetInvoker();

    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = "NYT::NFlow::TProcessFunctionSourceComputation";
    spec->ProcessingFunction = std::string(TypeName<TRegisteredPartitionAwareProcessFunction>());

    auto partitionId = TPartitionId(TGuid::Create());
    auto context = MakeAdapterTestComputationContext(invoker, std::move(spec));
    context->Partition->PartitionId = partitionId;
    auto dynamicContext = MakeAdapterTestDynamicComputationContext();

    TTestStateEnvironment environment;
    ASSERT_NE(environment.GetPartitionId(), partitionId);

    NConcurrency::WaitFor(
        BIND([&] {
            auto computation = New<TProcessFunctionSourceComputation>(context, dynamicContext);
            computation->DoInit(environment.GetStateManager()->CreateContext());
        }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();

    EXPECT_EQ(TRegisteredPartitionAwareProcessFunction::ObservedPartitionId, partitionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NTesting
